"""
Test Scraping Utility
Allows testing scraping on a single URL without full configuration
"""

import argparse
import logging
import sys
from typing import Dict, Any
from urllib.parse import urlparse
import requests
from tabulate import tabulate

from .robots_handler import RobotsHandler
from .content_extractor import ContentExtractor
from .url_manager import URLManager


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestScraper:
    """
    Utility for testing scraping on individual URLs.
    """

    def __init__(self, user_agent: str = None):
        """
        Initialize test scraper.

        Args:
            user_agent: Custom user agent (default: academic research agent)
        """
        self.user_agent = user_agent or "AcademicResearch-NGONetworkAnalysis/1.0 (498079@mail.muni.cz)"

    def test_url(self, url: str, check_robots: bool = True,
                 verbose: bool = True) -> Dict[str, Any]:
        """
        Test scraping a single URL and return analysis.

        Args:
            url: URL to test
            check_robots: Whether to check robots.txt compliance
            verbose: Whether to print detailed output

        Returns:
            Dictionary with test results
        """
        results = {
            'url': url,
            'success': False,
            'robots_allowed': None,
            'status_code': None,
            'content_type': None,
            'content_length': None,
            'num_links': 0,
            'num_internal_links': 0,
            'num_external_links': 0,
            'num_documents': 0,
            'metadata': {},
            'error': None
        }

        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc

            if verbose:
                print(f"\n{'='*80}")
                print(f"Testing URL: {url}")
                print(f"{'='*80}\n")

            # Step 1: Check robots.txt
            if check_robots:
                if verbose:
                    print("Step 1: Checking robots.txt compliance...")

                robots_handler = RobotsHandler(self.user_agent)
                robots_allowed = robots_handler.can_fetch(url)
                crawl_delay = robots_handler.get_crawl_delay(url)

                results['robots_allowed'] = robots_allowed

                if verbose:
                    if robots_allowed:
                        print(f"  ✓ Scraping allowed by robots.txt")
                        if crawl_delay:
                            print(f"  ⏱  Recommended crawl delay: {crawl_delay} seconds")
                    else:
                        print(f"  ✗ Scraping BLOCKED by robots.txt")
                        print(f"  ⚠  You should not scrape this URL")

                if not robots_allowed and check_robots:
                    results['error'] = "Blocked by robots.txt"
                    if verbose:
                        print("\n⚠  Test stopped due to robots.txt restriction")
                    return results

            # Step 2: Fetch URL
            if verbose:
                print(f"\nStep 2: Fetching URL...")

            session = requests.Session()
            session.headers.update({'User-Agent': self.user_agent})

            response = session.get(url, timeout=30, allow_redirects=True)
            results['status_code'] = response.status_code
            results['content_type'] = response.headers.get('content-type', 'unknown')
            results['content_length'] = len(response.content)

            if verbose:
                print(f"  Status Code: {response.status_code}")
                print(f"  Content Type: {results['content_type']}")
                print(f"  Content Length: {results['content_length']:,} bytes")

                if response.history:
                    print(f"  Redirects: {len(response.history)}")
                    for i, resp in enumerate(response.history, 1):
                        print(f"    {i}. {resp.status_code} -> {resp.url}")

            if response.status_code != 200:
                results['error'] = f"HTTP {response.status_code}"
                if verbose:
                    print(f"\n⚠  Non-200 status code received")
                return results

            # Check if HTML
            if 'text/html' not in results['content_type']:
                results['error'] = "Not HTML content"
                if verbose:
                    print(f"\n⚠  URL does not return HTML content")
                return results

            # Step 3: Extract content
            if verbose:
                print(f"\nStep 3: Analyzing content...")

            html = response.text
            extractor = ContentExtractor(url)

            # Extract links
            links = extractor.extract_links(html, url)
            results['num_links'] = len(links)

            internal_links = [link for link in links if link['type'] == 'internal']
            external_links = [link for link in links if link['type'] == 'external']

            results['num_internal_links'] = len(internal_links)
            results['num_external_links'] = len(external_links)

            if verbose:
                print(f"  Total Links: {results['num_links']}")
                print(f"    - Internal: {results['num_internal_links']}")
                print(f"    - External: {results['num_external_links']}")

            # Extract documents
            documents = extractor.extract_document_links(
                html, url,
                extensions=['.pdf', '.doc', '.docx', '.xls', '.xlsx']
            )
            results['num_documents'] = len(documents)

            if verbose:
                print(f"  Documents Found: {results['num_documents']}")
                if documents:
                    doc_types = {}
                    for doc in documents:
                        doc_type = doc['type']
                        doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
                    print(f"    Types: {dict(doc_types)}")

            # Extract metadata
            metadata = extractor.extract_metadata(html, url)
            results['metadata'] = metadata

            if verbose:
                print(f"\nStep 4: Extracted metadata...")
                metadata_display = []
                for key, value in metadata.items():
                    if value and key in ['title', 'description', 'author',
                                        'published_date', 'language']:
                        # Truncate long values
                        val_str = str(value)
                        if len(val_str) > 80:
                            val_str = val_str[:77] + "..."
                        metadata_display.append([key, val_str])

                if metadata_display:
                    print(tabulate(metadata_display, headers=['Field', 'Value'],
                                 tablefmt='simple'))
                else:
                    print("  No metadata found")

            # Page type
            page_type = extractor.identify_page_type(html, url)
            results['page_type'] = page_type

            if verbose:
                print(f"\nPage Type: {page_type}")

            # Sample internal links
            if verbose and internal_links:
                print(f"\nSample Internal Links (first 10):")
                for i, link in enumerate(internal_links[:10], 1):
                    link_text = link['text'][:60] if link['text'] else '(no text)'
                    print(f"  {i}. {link_text}")
                    print(f"     → {link['url']}")

            # Sample external links
            if verbose and external_links:
                print(f"\nSample External Links (first 5):")
                for i, link in enumerate(external_links[:5], 1):
                    link_text = link['text'][:60] if link['text'] else '(no text)'
                    print(f"  {i}. {link_text}")
                    print(f"     → {link['url']}")

            # Sample documents
            if verbose and documents:
                print(f"\nDocuments Found:")
                for i, doc in enumerate(documents[:10], 1):
                    doc_text = doc['text'][:60] if doc['text'] else '(no text)'
                    print(f"  {i}. [{doc['type']}] {doc_text}")
                    print(f"     → {doc['url']}")

            results['success'] = True

            if verbose:
                print(f"\n{'='*80}")
                print(f"✓ Test completed successfully!")
                print(f"{'='*80}\n")

        except requests.exceptions.Timeout:
            results['error'] = "Request timeout"
            if verbose:
                print(f"\n✗ Error: Request timed out")

        except requests.exceptions.RequestException as e:
            results['error'] = f"Request error: {str(e)}"
            if verbose:
                print(f"\n✗ Error: {str(e)}")

        except Exception as e:
            results['error'] = f"Unexpected error: {str(e)}"
            if verbose:
                print(f"\n✗ Unexpected error: {str(e)}")
                logger.exception("Test scraping error")

        return results

    def test_multiple_urls(self, urls: list, check_robots: bool = True):
        """
        Test multiple URLs and show summary.

        Args:
            urls: List of URLs to test
            check_robots: Whether to check robots.txt
        """
        print(f"\nTesting {len(urls)} URLs...\n")

        results = []
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Testing: {url}")
            result = self.test_url(url, check_robots=check_robots, verbose=False)
            results.append(result)

            # Print summary for this URL
            if result['success']:
                print(f"  ✓ Success: {result['num_links']} links, "
                      f"{result['num_documents']} documents")
            else:
                print(f"  ✗ Failed: {result['error']}")

        # Overall summary
        print(f"\n{'='*80}")
        print("Summary:")
        print(f"{'='*80}\n")

        summary_table = []
        for result in results:
            summary_table.append([
                result['url'][:50],
                '✓' if result['success'] else '✗',
                result['status_code'] or 'N/A',
                result['num_links'],
                result['num_documents'],
                result['error'] or '-'
            ])

        print(tabulate(summary_table,
                      headers=['URL', 'Success', 'Status', 'Links', 'Docs', 'Error'],
                      tablefmt='grid'))


def main():
    """Command-line interface for test scraper."""
    parser = argparse.ArgumentParser(
        description="Test web scraping on individual URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test a single URL
  python -m src.test_scraper https://www.hnutiduha.cz

  # Test without checking robots.txt
  python -m src.test_scraper https://example.org --no-robots

  # Test multiple URLs
  python -m src.test_scraper https://site1.org https://site2.org

  # Test URLs from file
  python -m src.test_scraper --file urls.txt
        """
    )

    parser.add_argument(
        'urls',
        nargs='*',
        help='URL(s) to test'
    )

    parser.add_argument(
        '--file', '-f',
        help='File containing URLs (one per line)'
    )

    parser.add_argument(
        '--no-robots',
        action='store_true',
        help='Skip robots.txt check'
    )

    parser.add_argument(
        '--user-agent',
        help='Custom user agent string'
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Minimal output'
    )

    args = parser.parse_args()

    # Collect URLs
    urls = args.urls or []

    if args.file:
        try:
            with open(args.file, 'r') as f:
                file_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                urls.extend(file_urls)
        except FileNotFoundError:
            print(f"Error: File not found: {args.file}")
            sys.exit(1)

    if not urls:
        parser.print_help()
        print("\nError: No URLs provided")
        sys.exit(1)

    # Create test scraper
    test_scraper = TestScraper(user_agent=args.user_agent)

    # Test URL(s)
    check_robots = not args.no_robots

    if len(urls) == 1:
        # Single URL - detailed output
        result = test_scraper.test_url(
            urls[0],
            check_robots=check_robots,
            verbose=not args.quiet
        )

        # Exit with appropriate code
        sys.exit(0 if result['success'] else 1)
    else:
        # Multiple URLs - summary output
        test_scraper.test_multiple_urls(urls, check_robots=check_robots)


if __name__ == "__main__":
    main()
