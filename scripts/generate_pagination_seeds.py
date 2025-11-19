#!/usr/bin/env python3
"""
Pagination Seed URL Generator

Automatically generates seed URLs for paginated content sections.
This ensures all pagination pages are scraped without relying on link discovery.
"""

import argparse
import csv
import logging
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
import requests
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaginationDetector:
    """Detects pagination patterns and generates seed URLs"""

    def __init__(self, user_agent: str = "Mozilla/5.0 (Research Bot)"):
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def detect_max_pages(
        self,
        base_url: str,
        page_param: str = "page",
        timeout: int = 30
    ) -> Optional[int]:
        """
        Detect maximum number of pages by analyzing pagination HTML

        Args:
            base_url: Base URL to analyze
            page_param: Query parameter for pagination (default: "page")
            timeout: Request timeout in seconds

        Returns:
            Maximum page number or None if can't detect
        """
        try:
            logger.info(f"Detecting pagination for: {base_url}")

            # Fetch the first page
            response = self.session.get(base_url, timeout=timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Strategy 1: Look for pagination links with page numbers
            max_page = self._extract_max_from_pagination_links(soup, base_url, page_param)
            if max_page:
                logger.info(f"Found max page from pagination links: {max_page}")
                return max_page

            # Strategy 2: Look for "last page" or similar links
            max_page = self._extract_max_from_last_link(soup, base_url, page_param)
            if max_page:
                logger.info(f"Found max page from 'last' link: {max_page}")
                return max_page

            # Strategy 3: Look for text like "Page 1 of 47"
            max_page = self._extract_max_from_text(soup)
            if max_page:
                logger.info(f"Found max page from text: {max_page}")
                return max_page

            logger.warning("Could not detect maximum page number")
            return None

        except requests.RequestException as e:
            logger.error(f"Error fetching {base_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error detecting pagination: {e}")
            return None

    def _extract_max_from_pagination_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        page_param: str
    ) -> Optional[int]:
        """Extract max page from pagination links"""
        try:
            page_numbers = set()

            # Find all links
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')

                # Check if link contains page parameter
                if page_param in href or f'?{page_param}=' in href:
                    # Extract page number from URL
                    parsed = urlparse(href)
                    params = parse_qs(parsed.query)

                    if page_param in params:
                        try:
                            page_num = int(params[page_param][0])
                            page_numbers.add(page_num)
                        except (ValueError, IndexError):
                            pass

                # Also check link text for numbers
                text = link.get_text().strip()
                if text.isdigit():
                    page_numbers.add(int(text))

            if page_numbers:
                return max(page_numbers)

            return None

        except Exception as e:
            logger.debug(f"Error extracting from pagination links: {e}")
            return None

    def _extract_max_from_last_link(
        self,
        soup: BeautifulSoup,
        base_url: str,
        page_param: str
    ) -> Optional[int]:
        """Extract max page from 'last page' link"""
        try:
            # Look for links with text like "Last", ">>", "»", etc.
            last_indicators = ['last', 'poslední', '>>', '»', '→']

            for link in soup.find_all('a', href=True):
                text = link.get_text().strip().lower()

                if any(indicator in text for indicator in last_indicators):
                    href = link.get('href', '')
                    parsed = urlparse(href)
                    params = parse_qs(parsed.query)

                    if page_param in params:
                        try:
                            page_num = int(params[page_param][0])
                            return page_num
                        except (ValueError, IndexError):
                            pass

            return None

        except Exception as e:
            logger.debug(f"Error extracting from last link: {e}")
            return None

    def _extract_max_from_text(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract max page from text like 'Page 1 of 47' or 'Strana 1 z 47'"""
        try:
            text = soup.get_text()

            # Patterns to match:
            # - "Page 1 of 47"
            # - "Strana 1 z 47"
            # - "1 / 47"
            # - "1-20 of 940 results" (calculate pages)
            patterns = [
                r'(?:page|strana|stránka)\s+\d+\s+(?:of|z|ze)\s+(\d+)',
                r'\d+\s*/\s*(\d+)\s*(?:pages|stran)',
                r'(\d+)\s+(?:pages|stran)',
            ]

            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    return int(match.group(1))

            return None

        except Exception as e:
            logger.debug(f"Error extracting from text: {e}")
            return None

    def generate_pagination_urls(
        self,
        base_url: str,
        max_pages: int,
        page_param: str = "page",
        start_page: int = 1
    ) -> List[str]:
        """
        Generate list of pagination URLs

        Args:
            base_url: Base URL template
            max_pages: Maximum page number
            page_param: Query parameter for page number
            start_page: Starting page number (usually 1 or 0)

        Returns:
            List of generated URLs
        """
        urls = []

        parsed = urlparse(base_url)
        params = parse_qs(parsed.query)

        for page_num in range(start_page, max_pages + 1):
            # Update page parameter
            params[page_param] = [str(page_num)]

            # Rebuild URL
            new_query = urlencode(params, doseq=True)
            new_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                ''
            ))

            urls.append(new_url)

        return urls


def add_seeds_to_csv(
    csv_path: Path,
    ngo_name: str,
    url_type: str,
    urls: List[str],
    depth_limit: int = 5,
    backup: bool = True
):
    """
    Add pagination seed URLs to url_seeds.csv

    Args:
        csv_path: Path to url_seeds.csv
        ngo_name: Name of NGO
        url_type: Type of URL (e.g., 'publications')
        urls: List of URLs to add
        depth_limit: Depth limit for these URLs
        backup: Whether to create backup before modifying
    """
    if backup:
        backup_path = csv_path.with_suffix('.csv.backup')
        import shutil
        shutil.copy2(csv_path, backup_path)
        logger.info(f"Created backup at {backup_path}")

    # Read existing seeds
    existing_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        existing_rows = list(reader)

    # Remove existing entries for this NGO + URL type combination
    filtered_rows = [
        row for row in existing_rows
        if not (row['ngo_name'] == ngo_name and row['url_type'] == url_type)
    ]

    # Add new URLs
    new_rows = []
    for url in urls:
        new_rows.append({
            'ngo_name': ngo_name,
            'url_type': url_type,
            'url': url,
            'depth_limit': str(depth_limit)
        })

    # Write back to file
    all_rows = filtered_rows + new_rows

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['ngo_name', 'url_type', 'url', 'depth_limit'])
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(f"Added {len(new_rows)} seed URLs to {csv_path}")
    logger.info(f"Removed {len(existing_rows) - len(filtered_rows)} old entries for {ngo_name}/{url_type}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate pagination seed URLs for comprehensive scraping'
    )
    parser.add_argument(
        'base_url',
        help='Base URL with pagination (e.g., https://example.org/publikace)'
    )
    parser.add_argument(
        '--ngo-name',
        required=True,
        help='Name of NGO (must match url_seeds.csv)'
    )
    parser.add_argument(
        '--url-type',
        default='publications',
        help='Type of URL (default: publications)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        help='Maximum page number (auto-detect if not specified)'
    )
    parser.add_argument(
        '--page-param',
        default='page',
        help='Query parameter for pagination (default: page)'
    )
    parser.add_argument(
        '--start-page',
        type=int,
        default=1,
        help='Starting page number (default: 1)'
    )
    parser.add_argument(
        '--depth-limit',
        type=int,
        default=5,
        help='Depth limit for generated URLs (default: 5)'
    )
    parser.add_argument(
        '--csv-path',
        type=Path,
        default=Path('config/url_seeds.csv'),
        help='Path to url_seeds.csv (default: config/url_seeds.csv)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print URLs without modifying csv file'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create backup of csv file'
    )

    args = parser.parse_args()

    # Detect pagination
    detector = PaginationDetector()

    if args.max_pages:
        max_pages = args.max_pages
        logger.info(f"Using manually specified max pages: {max_pages}")
    else:
        max_pages = detector.detect_max_pages(args.base_url, args.page_param)
        if not max_pages:
            logger.error("Could not auto-detect max pages. Please specify --max-pages manually")
            sys.exit(1)

    # Generate URLs
    logger.info(f"Generating pagination URLs: pages {args.start_page} to {max_pages}")
    urls = detector.generate_pagination_urls(
        args.base_url,
        max_pages,
        args.page_param,
        args.start_page
    )

    logger.info(f"Generated {len(urls)} URLs")

    # Print sample
    print("\nGenerated URLs (showing first 5 and last 5):")
    for url in urls[:5]:
        print(f"  {url}")
    if len(urls) > 10:
        print("  ...")
        for url in urls[-5:]:
            print(f"  {url}")

    # Add to CSV if not dry run
    if args.dry_run:
        print("\n[DRY RUN] Would add these URLs to CSV")
    else:
        print(f"\nAdding URLs to {args.csv_path}...")
        add_seeds_to_csv(
            args.csv_path,
            args.ngo_name,
            args.url_type,
            urls,
            args.depth_limit,
            backup=not args.no_backup
        )
        print("Done!")


if __name__ == '__main__':
    main()
