"""
Main Web Scraper
Coordinates all components for ethical, robust web scraping
"""

import logging
import time
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml
import pandas as pd
from tqdm import tqdm
import json
from datetime import datetime
import chardet

from .robots_handler import RobotsHandler
from .url_manager import URLManager
from .storage import StorageManager
from .content_extractor import ContentExtractor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NGOScraper:
    """
    Main scraper class that coordinates all scraping activities.
    Designed for ethical, academic research on NGO networks.
    """

    def __init__(self, config_path: str = "config/scraping_rules.yaml"):
        """
        Initialize the scraper.

        Args:
            config_path: Path to configuration YAML file
        """
        # Load configuration
        self.config = self._load_config(config_path)

        # Initialize components (will be set per NGO)
        self.robots_handler: Optional[RobotsHandler] = None
        self.url_manager: Optional[URLManager] = None
        self.storage: Optional[StorageManager] = None
        self.content_extractor: Optional[ContentExtractor] = None

        # HTTP session with retry logic
        self.session = self._create_session()

        # Statistics
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_documents': 0,
            'total_links': 0,
            'start_time': None,
            'end_time': None
        }

        # Progress tracking
        self.progress_file = Path(self.config['session']['progress_file'])
        self.checkpoint_interval = self.config['session']['checkpoint_interval']
        self.requests_since_checkpoint = 0

        logger.info("NGO Scraper initialized")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic."""
        session = requests.Session()

        # Configure retries
        max_retries = self.config['rate_limiting']['max_retries']
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=self.config['performance']['connection_pool_size'],
            pool_maxsize=self.config['performance']['connection_pool_size']
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set user agent
        session.headers.update({
            'User-Agent': self.config['user_agent']
        })

        return session

    def _setup_logging(self, ngo_name: str):
        """Set up file logging for specific NGO."""
        if self.config['logging']['file_output']:
            log_file = Path(f"data/logs/{ngo_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            log_file.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )

            logging.getLogger().addHandler(file_handler)
            logger.info(f"Logging to {log_file}")

    def _initialize_for_ngo(self, ngo_name: str, base_url: str, max_depth: int, max_pages: int):
        """Initialize components for a specific NGO."""
        # Extract domain
        parsed = urlparse(base_url)
        domain = parsed.netloc

        # Initialize components
        self.robots_handler = RobotsHandler(self.config['user_agent'])
        self.url_manager = URLManager(domain, max_depth=max_depth, max_pages=max_pages)
        self.storage = StorageManager(ngo_name=ngo_name)
        self.content_extractor = ContentExtractor(base_url)

        # Set up logging
        self._setup_logging(ngo_name)

        logger.info(f"Initialized scraper for {ngo_name} ({base_url})")

    def _fetch_url(self, url: str) -> Optional[Tuple[bytes, str, str]]:
        """
        Fetch URL with proper error handling and rate limiting.

        Args:
            url: URL to fetch

        Returns:
            Tuple of (content, content_type, encoding) or None if failed
        """
        # Check robots.txt
        if self.config['crawl']['respect_robots_txt']:
            if not self.robots_handler.can_fetch(url):
                logger.warning(f"Blocked by robots.txt: {url}")
                return None

            # Check for crawl delay
            crawl_delay = self.robots_handler.get_crawl_delay(url)
            if crawl_delay:
                delay = max(crawl_delay, self.config['rate_limiting']['delay_between_requests'])
            else:
                delay = self.config['rate_limiting']['delay_between_requests']
        else:
            delay = self.config['rate_limiting']['delay_between_requests']

        # Rate limiting
        time.sleep(delay)

        try:
            # Make request
            logger.debug(f"Fetching: {url}")
            response = self.session.get(
                url,
                timeout=self.config['rate_limiting']['timeout'],
                allow_redirects=True
            )

            self.stats['total_requests'] += 1

            # Check status
            if response.status_code == 200:
                self.stats['successful_requests'] += 1

                content_type = response.headers.get('content-type', '').lower()
                encoding = response.encoding or 'utf-8'

                # Detect encoding if not provided
                if not response.encoding:
                    detected = chardet.detect(response.content)
                    if detected and detected['encoding']:
                        encoding = detected['encoding']

                logger.info(f"Successfully fetched: {url} ({len(response.content)} bytes)")

                return (response.content, content_type, encoding)

            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                self.stats['failed_requests'] += 1
                self.url_manager.mark_failed(url, f"HTTP {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {url}")
            self.stats['failed_requests'] += 1
            self.url_manager.mark_failed(url, "Timeout")
            time.sleep(self.config['rate_limiting']['delay_on_error'])
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            self.stats['failed_requests'] += 1
            self.url_manager.mark_failed(url, str(e))
            time.sleep(self.config['rate_limiting']['delay_on_error'])
            return None

        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            self.stats['failed_requests'] += 1
            self.url_manager.mark_failed(url, str(e))
            return None

    def _is_html_content(self, content_type: str) -> bool:
        """Check if content type is HTML."""
        return 'text/html' in content_type

    def _is_document(self, content_type: str, url: str) -> bool:
        """Check if content is a downloadable document."""
        # Check content type
        document_types = self.config['content_types'][1:]  # Exclude text/html
        if any(doc_type in content_type for doc_type in document_types):
            return True

        # Check file extension
        url_lower = url.lower()
        if any(url_lower.endswith(ext) for ext in self.config['download_extensions']):
            return True

        return False

    def _process_html_page(self, url: str, content: bytes, encoding: str, depth: int):
        """
        Process HTML page - extract links and content.

        Args:
            url: Page URL
            content: Page content
            encoding: Content encoding
            depth: Current crawl depth
        """
        try:
            # Decode content
            html = content.decode(encoding, errors='replace')

            # Check minimum content length
            if len(html) < self.config['quality']['min_content_length']:
                logger.debug(f"Page too short, skipping: {url}")
                return

            # Save HTML if configured
            if self.config['storage']['save_html']:
                check_duplicates = self.config['quality']['check_content_hash']
                self.storage.save_page(url, content, encoding, check_duplicates)

            # Extract links
            if self.config['extraction']['extract_links']:
                links = self.content_extractor.extract_links(html, url)

                # Store links for network analysis
                self.storage.add_links(url, links)
                self.stats['total_links'] += len(links)

                # Add internal links to queue
                if self.config['crawl']['follow_external_links'] is False:
                    internal_links = [link for link in links if link['type'] == 'internal']
                else:
                    internal_links = links

                for link in internal_links:
                    try:
                        link_url = link['url']

                        # Skip if matches exclusion pattern
                        if self.url_manager.should_exclude_url(
                            link_url,
                            self.config['url_exclusions']
                        ):
                            continue

                        # Determine priority
                        priority = self.url_manager.get_url_priority(
                            link_url,
                            self.config['priority_patterns']
                        )

                        # Add to queue
                        self.url_manager.add_url(
                            link_url,
                            depth=depth + 1,
                            parent_url=url,
                            priority=priority
                        )
                    except Exception as e:
                        logger.error(f"Error processing link {link.get('url', 'unknown')}: {e}", exc_info=True)
                        # Try to add with default priority if link_url was extracted
                        try:
                            if 'link_url' in locals():
                                self.url_manager.add_url(
                                    link_url,
                                    depth=depth + 1,
                                    parent_url=url,
                                    priority=3  # Default low priority
                                )
                        except:
                            pass  # Skip this link if it still fails

            # Extract and save document links
            documents = self.content_extractor.extract_document_links(
                html,
                url,
                self.config['download_extensions']
            )

            for doc in documents:
                try:
                    # Add document URL to queue with high priority for download
                    self.url_manager.add_url(
                        doc['url'],
                        depth=depth,
                        parent_url=url,
                        priority=0  # High priority for documents
                    )
                except Exception as e:
                    logger.error(f"Error queuing document {doc.get('url', 'unknown')}: {e}")

            # Extract metadata if configured
            if self.config['extraction']['extract_metadata']:
                metadata = self.content_extractor.extract_metadata(html, url)
                # TODO: Store metadata separately if needed

            logger.debug(f"Processed HTML page: {url}")

        except Exception as e:
            logger.error(f"Error processing HTML page {url}: {e}")

    def _process_document(self, url: str, content: bytes, content_type: str):
        """
        Process and save document.

        Args:
            url: Document URL
            content: Document content
            content_type: Content type
        """
        try:
            if self.config['storage']['save_documents']:
                filepath = self.storage.save_document(url, content, content_type)
                if filepath:
                    self.stats['total_documents'] += 1
                    logger.info(f"Saved document: {url}")

        except Exception as e:
            logger.error(f"Error processing document {url}: {e}")

    def _save_checkpoint(self):
        """Save current progress to file."""
        try:
            checkpoint_data = {
                'timestamp': datetime.now().isoformat(),
                'url_manager_state': self.url_manager.save_state(),
                'stats': self.stats
            }

            # Ensure directory exists
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2)

            logger.debug(f"Checkpoint saved to {self.progress_file}")

        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def _load_checkpoint(self) -> bool:
        """
        Load progress from checkpoint file.

        Returns:
            True if checkpoint loaded successfully
        """
        try:
            if not self.progress_file.exists():
                return False

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            # Restore URL manager state
            self.url_manager.load_state(checkpoint_data['url_manager_state'])

            # Restore stats
            self.stats.update(checkpoint_data['stats'])

            logger.info(f"Resumed from checkpoint: {checkpoint_data['timestamp']}")
            return True

        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return False

    def scrape_ngo(self, ngo_name: str, seed_urls: List[Dict],
                   max_depth: int = None, max_pages: int = None,
                   resume: bool = False) -> Dict:
        """
        Scrape a single NGO website.

        Args:
            ngo_name: Name of the NGO
            seed_urls: List of seed URL dictionaries
            max_depth: Maximum crawl depth (overrides config)
            max_pages: Maximum pages to scrape (overrides config)
            resume: Whether to resume from checkpoint

        Returns:
            Dictionary with scraping statistics
        """
        logger.info(f"=" * 80)
        logger.info(f"Starting scrape for: {ngo_name}")
        logger.info(f"=" * 80)

        self.stats['start_time'] = datetime.now().isoformat()

        # Use config defaults if not specified
        max_depth = max_depth or self.config['crawl']['max_depth']
        max_pages = max_pages or self.config['crawl']['max_pages_per_site']

        # Get base URL from first seed
        base_url = seed_urls[0]['url']

        # Initialize components
        self._initialize_for_ngo(ngo_name, base_url, max_depth, max_pages)

        # Try to resume from checkpoint
        if resume and self.config['session']['save_progress']:
            resumed = self._load_checkpoint()
            if not resumed:
                logger.info("No checkpoint found, starting fresh")

        # Add seed URLs to queue if not resuming or queue is empty
        if self.url_manager.queue_size() == 0:
            for seed in seed_urls:
                self.url_manager.add_url(
                    seed['url'],
                    depth=0,
                    priority=0  # High priority for seeds
                )

        # Main scraping loop
        with tqdm(total=max_pages, desc=f"Scraping {ngo_name}") as pbar:
            while True:
                # Get next URL
                next_url_data = self.url_manager.get_next_url()

                if not next_url_data:
                    logger.info("URL queue exhausted")
                    break

                depth, url, parent_url = next_url_data

                # Check if we've reached the limit (if set)
                if max_pages is not None and len(self.url_manager.visited_urls) >= max_pages:
                    logger.info(f"Reached max pages limit: {max_pages}")
                    break

                # Skip if already visited
                if self.url_manager.is_visited(url):
                    continue

                # Fetch URL
                result = self._fetch_url(url)

                if result:
                    content, content_type, encoding = result

                    # Mark as visited
                    self.url_manager.mark_visited(url)

                    # Process based on content type
                    if self._is_html_content(content_type):
                        self._process_html_page(url, content, encoding, depth)
                    elif self._is_document(content_type, url):
                        self._process_document(url, content, content_type)
                    else:
                        logger.debug(f"Skipping unsupported content type: {content_type} for {url}")

                    # Update progress bar
                    pbar.update(1)
                    pbar.set_postfix({
                        'Queue': self.url_manager.queue_size(),
                        'Links': self.stats['total_links'],
                        'Docs': self.stats['total_documents']
                    })

                    # Checkpoint
                    self.requests_since_checkpoint += 1
                    if (self.config['session']['save_progress'] and
                        self.requests_since_checkpoint >= self.checkpoint_interval):
                        self._save_checkpoint()
                        self.requests_since_checkpoint = 0

                else:
                    # Mark as visited even if failed to avoid retrying
                    self.url_manager.mark_visited(url)

        # Finalize
        self.stats['end_time'] = datetime.now().isoformat()

        # Combine all statistics
        final_stats = {
            **self.stats,
            'url_manager_stats': self.url_manager.get_stats(),
            'storage_stats': self.storage.get_stats()
        }

        # Save final data
        logger.info("Finalizing storage...")
        self.storage.finalize(additional_metadata=final_stats)

        # Log summary
        logger.info(f"=" * 80)
        logger.info(f"Scraping completed for: {ngo_name}")
        logger.info(f"Total requests: {self.stats['total_requests']}")
        logger.info(f"Successful: {self.stats['successful_requests']}")
        logger.info(f"Failed: {self.stats['failed_requests']}")
        logger.info(f"Pages visited: {len(self.url_manager.visited_urls)}")
        logger.info(f"Documents downloaded: {self.stats['total_documents']}")
        logger.info(f"Links extracted: {self.stats['total_links']}")
        logger.info(f"=" * 80)

        return final_stats

    def scrape_from_config(self, ngo_list_file: str = "config/ngo_list.csv",
                          url_seeds_file: str = "config/url_seeds.csv",
                          ngo_filter: Optional[List[str]] = None,
                          resume: bool = False):
        """
        Scrape multiple NGOs from configuration files.

        Args:
            ngo_list_file: Path to NGO list CSV
            url_seeds_file: Path to URL seeds CSV
            ngo_filter: Optional list of NGO names to scrape (scrape only these)
            resume: Whether to resume from checkpoints
        """
        # Load NGO list
        ngo_df = pd.read_csv(ngo_list_file)

        # Load URL seeds
        seeds_df = pd.read_csv(url_seeds_file)

        # Filter NGOs if specified
        if ngo_filter:
            ngo_df = ngo_df[ngo_df['canonical_name'].isin(ngo_filter)]

        # Sort by priority
        ngo_df = ngo_df.sort_values('scrape_priority')

        logger.info(f"Planning to scrape {len(ngo_df)} NGOs")

        # Scrape each NGO
        all_stats = {}

        for _, ngo_row in ngo_df.iterrows():
            ngo_name = ngo_row['canonical_name']

            # Get seed URLs for this NGO
            ngo_seeds = seeds_df[seeds_df['ngo_name'] == ngo_name]

            if len(ngo_seeds) == 0:
                logger.warning(f"No seed URLs found for {ngo_name}, skipping")
                continue

            # Prepare seed URLs
            seed_urls = []
            for _, seed_row in ngo_seeds.iterrows():
                seed_urls.append({
                    'url': seed_row['url'],
                    'type': seed_row['url_type'],
                    'depth_limit': seed_row['depth_limit']
                })

            # Scrape this NGO
            try:
                stats = self.scrape_ngo(
                    ngo_name,
                    seed_urls,
                    max_depth=int(ngo_seeds['depth_limit'].max()),
                    resume=resume
                )
                all_stats[ngo_name] = stats

            except Exception as e:
                logger.error(f"Error scraping {ngo_name}: {e}", exc_info=True)
                all_stats[ngo_name] = {'error': str(e)}

            # Pause between NGOs
            logger.info(f"Pausing before next NGO...")
            time.sleep(5)

        # Save overall statistics
        stats_file = Path("data/metadata/overall_scraping_stats.json")
        stats_file.parent.mkdir(parents=True, exist_ok=True)
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(all_stats, f, indent=2)

        logger.info(f"All scraping completed. Statistics saved to {stats_file}")

        return all_stats


def main():
    """Main entry point for running the scraper."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Academic Web Scraper for NGO Network Analysis"
    )
    parser.add_argument(
        '--config',
        default='config/scraping_rules.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--ngo-list',
        default='config/ngo_list.csv',
        help='Path to NGO list CSV'
    )
    parser.add_argument(
        '--url-seeds',
        default='config/url_seeds.csv',
        help='Path to URL seeds CSV'
    )
    parser.add_argument(
        '--filter',
        nargs='+',
        help='Filter to specific NGOs (space-separated names)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from previous checkpoint'
    )

    args = parser.parse_args()

    # Create and run scraper
    scraper = NGOScraper(config_path=args.config)
    scraper.scrape_from_config(
        ngo_list_file=args.ngo_list,
        url_seeds_file=args.url_seeds,
        ngo_filter=args.filter,
        resume=args.resume
    )


if __name__ == "__main__":
    main()
