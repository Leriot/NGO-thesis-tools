#!/usr/bin/env python3
"""
Test Dataset Scraper
Scrapes a limited dataset (30 HTML pages, 20 PDFs) from each NGO for testing purposes.
Saves to a separate test_dataset_{date} folder.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
from tqdm import tqdm
from multiprocessing import Process, Queue, current_process
from queue import Empty

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.robots_handler import RobotsHandler
from src.url_manager import URLManager
from src.content_extractor import ContentExtractor
from src.scraper import NGOScraper
from src.storage import StorageManager
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class TestStorageManager(StorageManager):
    """
    Modified storage manager for test datasets.
    Saves to test_dataset_{date} instead of raw.
    """

    def __init__(self, ngo_name: str = "default", test_date: str = None):
        """
        Initialize test storage manager.

        Args:
            ngo_name: Name of the NGO being scraped
            test_date: Date string for the test dataset folder
        """
        self.base_dir = Path("data")
        self.ngo_name = self._sanitize_filename(ngo_name)

        # Use provided date or generate new one
        if test_date is None:
            test_date = datetime.now().strftime("%Y%m%d")

        self.test_date = test_date
        self.session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create test dataset directory structure
        self.test_dataset_dir = self.base_dir / f"test_dataset_{self.test_date}"
        self.raw_dir = self.test_dataset_dir / self.ngo_name
        self.metadata_dir = self.test_dataset_dir / "metadata" / self.ngo_name
        self.logs_dir = self.test_dataset_dir / "logs"

        self._create_directories()

        # Paths for different content types
        self.pages_dir = self.raw_dir / "pages" / "html"
        self.documents_dir = self.raw_dir / "documents"
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        self.documents_dir.mkdir(parents=True, exist_ok=True)

        # Links and metadata storage
        self.links_file = self.raw_dir / "links.json"
        self.metadata_file = self.raw_dir / "metadata.json"

        # In-memory storage for links
        self.links: List[Dict] = []

        # Content hash tracking
        self.content_hashes: set = set()

        # Statistics
        self.stats = {
            'pages_saved': 0,
            'documents_saved': 0,
            'links_extracted': 0,
            'duplicate_content': 0,
            'errors': 0
        }

        logger.info(f"Test storage initialized for {self.ngo_name} at {self.raw_dir}")


class TestDatasetScraper(NGOScraper):
    """
    Test dataset scraper with limited page/document collection.
    """

    def __init__(self, config_path: str = "config/scraping_rules.yaml", test_date: str = None):
        """
        Initialize test dataset scraper.

        Args:
            config_path: Path to configuration YAML file
            test_date: Date string for the test dataset folder
        """
        super().__init__(config_path)
        self.test_date = test_date or datetime.now().strftime("%Y%m%d")

        # Limits for test dataset
        self.max_html_pages = 30
        self.max_pdfs = 20

        # Counters
        self.html_count = 0
        self.pdf_count = 0

    def _initialize_for_ngo(self, ngo_name: str, base_url: str, max_depth: int, max_pages: int):
        """Initialize components for a specific NGO with test storage."""
        from urllib.parse import urlparse

        # Extract domain
        parsed = urlparse(base_url)
        domain = parsed.netloc

        # Initialize components
        self.robots_handler = RobotsHandler(self.config['user_agent'])
        self.url_manager = URLManager(domain, max_depth=max_depth, max_pages=max_pages)

        # Use test storage manager instead of regular one
        self.storage = TestStorageManager(ngo_name=ngo_name, test_date=self.test_date)

        self.content_extractor = ContentExtractor(base_url)

        # Set up logging
        self._setup_logging(ngo_name)

        logger.info(f"Initialized test scraper for {ngo_name} ({base_url})")

    def _process_html_page(self, url: str, content: bytes, encoding: str, depth: int):
        """
        Process HTML page - limited to max_html_pages.

        Args:
            url: Page URL
            content: Page content
            encoding: Content encoding
            depth: Current crawl depth
        """
        # Check if we've reached HTML limit
        if self.html_count >= self.max_html_pages:
            logger.debug(f"Reached HTML page limit ({self.max_html_pages}), skipping: {url}")
            return

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
                saved = self.storage.save_page(url, content, encoding, check_duplicates)
                if saved:
                    self.html_count += 1
                    logger.info(f"Saved HTML page {self.html_count}/{self.max_html_pages}: {url}")

            # Extract metadata (including publication date)
            publication_date = None
            if self.config['extraction']['extract_metadata']:
                metadata = self.content_extractor.extract_metadata(html, url)
                publication_date = metadata.get('published_date')

            # Extract links
            if self.config['extraction']['extract_links']:
                links = self.content_extractor.extract_links(html, url)

                # Store links for network analysis
                self.storage.add_links(url, links, publication_date)
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
                        logger.error(f"Error processing link {link.get('url', 'unknown')}: {e}")

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

        except Exception as e:
            logger.error(f"Error processing HTML page {url}: {e}")

    def _process_document(self, url: str, content: bytes, content_type: str):
        """
        Process and save document - limited to max_pdfs.

        Args:
            url: Document URL
            content: Document content
            content_type: Content type
        """
        # Check if we've reached PDF/document limit
        if self.pdf_count >= self.max_pdfs:
            logger.debug(f"Reached PDF/document limit ({self.max_pdfs}), skipping: {url}")
            return

        try:
            if self.config['storage']['save_documents']:
                filepath = self.storage.save_document(url, content, content_type)
                if filepath:
                    self.pdf_count += 1
                    self.stats['total_documents'] += 1
                    logger.info(f"Saved document {self.pdf_count}/{self.max_pdfs}: {url}")

        except Exception as e:
            logger.error(f"Error processing document {url}: {e}")

    def scrape_test_ngo(self, ngo_name: str, seed_urls: List[Dict],
                        max_depth: int = None) -> Dict:
        """
        Scrape a single NGO for test dataset with limits.

        Args:
            ngo_name: Name of the NGO
            seed_urls: List of seed URL dictionaries
            max_depth: Maximum crawl depth (overrides config)

        Returns:
            Dictionary with scraping statistics
        """
        logger.info(f"=" * 80)
        logger.info(f"Starting TEST scrape for: {ngo_name}")
        logger.info(f"Limits: {self.max_html_pages} HTML pages, {self.max_pdfs} PDFs")
        logger.info(f"=" * 80)

        self.stats['start_time'] = datetime.now().isoformat()

        # Reset counters for this NGO
        self.html_count = 0
        self.pdf_count = 0

        # Use config defaults if not specified
        max_depth = max_depth or self.config['crawl']['max_depth']

        # For test dataset, limit total pages to avoid over-scraping
        max_pages = self.max_html_pages + self.max_pdfs + 50  # Add buffer for queue exploration

        # Get base URL from first seed
        base_url = seed_urls[0]['url']

        # Initialize components
        self._initialize_for_ngo(ngo_name, base_url, max_depth, max_pages)

        # Add seed URLs to queue
        for seed in seed_urls:
            self.url_manager.add_url(
                seed['url'],
                depth=0,
                priority=0  # High priority for seeds
            )

        # Main scraping loop
        with tqdm(total=self.max_html_pages + self.max_pdfs,
                  desc=f"Test scraping {ngo_name}") as pbar:
            while True:
                # Check if we've reached both limits
                if self.html_count >= self.max_html_pages and self.pdf_count >= self.max_pdfs:
                    logger.info("Reached both HTML and PDF limits")
                    break

                # Get next URL
                next_url_data = self.url_manager.get_next_url()

                if not next_url_data:
                    logger.info("URL queue exhausted")
                    break

                depth, url, parent_url = next_url_data

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
                    pbar.n = self.html_count + self.pdf_count
                    pbar.set_postfix({
                        'HTML': f'{self.html_count}/{self.max_html_pages}',
                        'PDFs': f'{self.pdf_count}/{self.max_pdfs}',
                        'Queue': self.url_manager.queue_size()
                    })
                    pbar.refresh()

                else:
                    # Mark as visited even if failed to avoid retrying
                    self.url_manager.mark_visited(url)

        # Finalize
        self.stats['end_time'] = datetime.now().isoformat()

        # Combine all statistics
        final_stats = {
            **self.stats,
            'html_pages_saved': self.html_count,
            'pdfs_saved': self.pdf_count,
            'url_manager_stats': self.url_manager.get_stats(),
            'storage_stats': self.storage.get_stats()
        }

        # Save final data
        logger.info("Finalizing storage...")
        self.storage.finalize(additional_metadata=final_stats)

        # Log summary
        logger.info(f"=" * 80)
        logger.info(f"Test scraping completed for: {ngo_name}")
        logger.info(f"HTML pages saved: {self.html_count}/{self.max_html_pages}")
        logger.info(f"PDFs saved: {self.pdf_count}/{self.max_pdfs}")
        logger.info(f"Total requests: {self.stats['total_requests']}")
        logger.info(f"Links extracted: {self.stats['total_links']}")
        logger.info(f"=" * 80)

        return final_stats

    def scrape_test_dataset(self,
                           ngo_list_file: str = "config/ngo_list.csv",
                           url_seeds_file: str = "config/url_seeds.csv",
                           ngo_filter: Optional[List[str]] = None):
        """
        Scrape test dataset from multiple NGOs.

        Args:
            ngo_list_file: Path to NGO list CSV
            url_seeds_file: Path to URL seeds CSV
            ngo_filter: Optional list of NGO names to scrape (scrape only these)
        """
        logger.info("=" * 80)
        logger.info(f"CREATING TEST DATASET - {self.test_date}")
        logger.info(f"Limits per NGO: {self.max_html_pages} HTML pages, {self.max_pdfs} PDFs")
        logger.info("=" * 80)

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
                stats = self.scrape_test_ngo(
                    ngo_name,
                    seed_urls,
                    max_depth=int(ngo_seeds['depth_limit'].max())
                )
                all_stats[ngo_name] = stats

            except Exception as e:
                logger.error(f"Error scraping {ngo_name}: {e}", exc_info=True)
                all_stats[ngo_name] = {'error': str(e)}

            # Pause between NGOs
            logger.info(f"Pausing before next NGO...")
            time.sleep(5)

        # Save overall statistics
        test_dataset_dir = Path(f"data/test_dataset_{self.test_date}")
        stats_file = test_dataset_dir / "metadata" / "overall_test_stats.json"
        stats_file.parent.mkdir(parents=True, exist_ok=True)

        import json
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(all_stats, f, indent=2)

        logger.info(f"=" * 80)
        logger.info(f"TEST DATASET SCRAPING COMPLETED")
        logger.info(f"Dataset saved to: {test_dataset_dir}")
        logger.info(f"Statistics saved to: {stats_file}")
        logger.info(f"=" * 80)

        return all_stats

    def scrape_test_dataset_parallel(self,
                                     ngo_list_file: str = "config/ngo_list.csv",
                                     url_seeds_file: str = "config/url_seeds.csv",
                                     ngo_filter: Optional[List[str]] = None,
                                     max_workers: int = 4):
        """
        Scrape test dataset from multiple NGOs in parallel.

        Args:
            ngo_list_file: Path to NGO list CSV
            url_seeds_file: Path to URL seeds CSV
            ngo_filter: Optional list of NGO names to scrape (scrape only these)
            max_workers: Maximum number of parallel workers (default: 4)
        """
        logger.info("=" * 80)
        logger.info(f"CREATING TEST DATASET - {self.test_date} (PARALLEL MODE)")
        logger.info(f"Limits per NGO: {self.max_html_pages} HTML pages, {self.max_pdfs} PDFs")
        logger.info(f"Using {max_workers} parallel workers")
        logger.info("=" * 80)

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

        # Prepare NGO scraping tasks
        scraping_tasks = []
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

            scraping_tasks.append({
                'ngo_name': ngo_name,
                'seed_urls': seed_urls,
                'max_depth': int(ngo_seeds['depth_limit'].max()),
                'test_date': self.test_date,
                'max_html': self.max_html_pages,
                'max_pdfs': self.max_pdfs
            })

        # Run scraping tasks in parallel
        all_stats = self._run_parallel_test_scraping(scraping_tasks, max_workers)

        # Save overall statistics
        test_dataset_dir = Path(f"data/test_dataset_{self.test_date}")
        stats_file = test_dataset_dir / "metadata" / "overall_test_stats.json"
        stats_file.parent.mkdir(parents=True, exist_ok=True)

        import json
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(all_stats, f, indent=2)

        logger.info(f"=" * 80)
        logger.info(f"TEST DATASET SCRAPING COMPLETED (PARALLEL)")
        logger.info(f"Dataset saved to: {test_dataset_dir}")
        logger.info(f"Statistics saved to: {stats_file}")
        logger.info(f"=" * 80)

        return all_stats

    def _run_parallel_test_scraping(self, tasks: List[Dict], max_workers: int) -> Dict:
        """
        Run test scraping tasks in parallel using multiprocessing.

        Args:
            tasks: List of scraping task dictionaries
            max_workers: Maximum number of parallel workers

        Returns:
            Dictionary of statistics per NGO
        """
        results_queue = Queue()
        processes = []
        all_stats = {}

        # Process tasks in batches
        for i in range(0, len(tasks), max_workers):
            batch = tasks[i:i + max_workers]
            batch_processes = []

            logger.info(f"Starting batch {i // max_workers + 1} with {len(batch)} NGOs")

            # Start processes for this batch
            for task in batch:
                p = Process(
                    target=_scrape_test_ngo_worker,
                    args=(task, results_queue)
                )
                p.start()
                batch_processes.append((p, task['ngo_name']))
                processes.append(p)

            # Wait for batch to complete
            for p, ngo_name in batch_processes:
                p.join()
                logger.info(f"Process for {ngo_name} completed")

            # Small pause between batches
            if i + max_workers < len(tasks):
                logger.info("Pausing before next batch...")
                time.sleep(2)

        # Collect results from queue
        while not results_queue.empty():
            try:
                ngo_name, stats = results_queue.get(timeout=1)
                all_stats[ngo_name] = stats
            except Empty:
                break

        return all_stats


def _scrape_test_ngo_worker(task: Dict, results_queue: Queue):
    """
    Worker function for parallel test scraping (must be at module level for pickling).

    Args:
        task: Dictionary with scraping task parameters
        results_queue: Queue to put results into
    """
    import sys
    from pathlib import Path

    # Add parent directory to path for imports
    sys.path.insert(0, str(Path(__file__).parent.parent))

    process_name = current_process().name
    ngo_name = task['ngo_name']

    # Set up logging for this worker
    worker_logger = logging.getLogger(f"Worker-{process_name}")
    worker_logger.setLevel(logging.INFO)

    # Create console handler if not exists
    if not worker_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(f'[{ngo_name}] %(asctime)s - %(levelname)s - %(message)s')
        )
        worker_logger.addHandler(handler)

    try:
        worker_logger.info(f"Starting test scrape for {ngo_name}")

        # Create a new test scraper instance for this process
        scraper = TestDatasetScraper(test_date=task['test_date'])

        # Set limits from task
        scraper.max_html_pages = task['max_html']
        scraper.max_pdfs = task['max_pdfs']

        # Run the scraping
        stats = scraper.scrape_test_ngo(
            ngo_name=task['ngo_name'],
            seed_urls=task['seed_urls'],
            max_depth=task['max_depth']
        )

        # Put results in queue
        results_queue.put((ngo_name, stats))

        worker_logger.info(f"Completed test scrape for {ngo_name}")

    except Exception as e:
        worker_logger.error(f"Error scraping {ngo_name}: {e}", exc_info=True)
        results_queue.put((ngo_name, {'error': str(e)}))


def main():
    """Main entry point for test dataset scraper."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test Dataset Scraper - Limited scraping for testing purposes"
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
        '--date',
        help='Date string for test dataset folder (default: today YYYYMMDD)'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Run scraping in parallel mode (multiple NGOs simultaneously)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of parallel workers (default: 4, only used with --parallel)'
    )

    args = parser.parse_args()

    # Create and run test scraper
    scraper = TestDatasetScraper(config_path=args.config, test_date=args.date)

    if args.parallel:
        # Run in parallel mode
        scraper.scrape_test_dataset_parallel(
            ngo_list_file=args.ngo_list,
            url_seeds_file=args.url_seeds,
            ngo_filter=args.filter,
            max_workers=args.max_workers
        )
    else:
        # Run in sequential mode
        scraper.scrape_test_dataset(
            ngo_list_file=args.ngo_list,
            url_seeds_file=args.url_seeds,
            ngo_filter=args.filter
        )


if __name__ == "__main__":
    main()
