"""
Sitemap XML Parser

Automatically discovers and parses sitemap.xml files to extract URLs,
priorities, and change frequencies for comprehensive site coverage.
"""

import logging
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import requests

logger = logging.getLogger(__name__)


class SitemapParser:
    """Parses sitemap.xml files and extracts URLs with metadata"""

    def __init__(self, user_agent: str = "Mozilla/5.0 (Research Bot)", timeout: int = 30):
        """
        Initialize sitemap parser

        Args:
            user_agent: User agent string for requests
            timeout: Request timeout in seconds
        """
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def discover_sitemap(self, base_url: str) -> Optional[str]:
        """
        Try to discover sitemap URL for a website

        Args:
            base_url: Base URL of the website

        Returns:
            Sitemap URL if found, None otherwise
        """
        parsed = urlparse(base_url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        # Common sitemap locations
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap1.xml',
            '/sitemap/sitemap.xml',
        ]

        # Try robots.txt first
        try:
            robots_url = f"{base}/robots.txt"
            response = self.session.get(robots_url, timeout=self.timeout)
            if response.status_code == 200:
                for line in response.text.splitlines():
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                        return sitemap_url
        except Exception as e:
            logger.debug(f"Could not check robots.txt: {e}")

        # Try common sitemap paths
        for path in sitemap_paths:
            sitemap_url = urljoin(base, path)
            try:
                response = self.session.head(sitemap_url, timeout=self.timeout)
                if response.status_code == 200:
                    logger.info(f"Found sitemap at: {sitemap_url}")
                    return sitemap_url
            except Exception:
                continue

        logger.warning(f"No sitemap found for {base_url}")
        return None

    def parse_sitemap(self, sitemap_url: str) -> List[Dict[str, str]]:
        """
        Parse sitemap XML and extract URLs with metadata

        Args:
            sitemap_url: URL of the sitemap

        Returns:
            List of URL dictionaries with 'loc', 'lastmod', 'changefreq', 'priority'
        """
        urls = []

        try:
            logger.info(f"Parsing sitemap: {sitemap_url}")
            response = self.session.get(sitemap_url, timeout=self.timeout)
            response.raise_for_status()

            # Parse XML
            root = ET.fromstring(response.content)

            # Handle namespaces
            namespaces = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'
            }

            # Check if it's a sitemap index
            sitemapindex = root.findall('ns:sitemap', namespaces)
            if sitemapindex:
                logger.info(f"Found sitemap index with {len(sitemapindex)} sitemaps")
                # Recursively parse each sitemap
                for sitemap in sitemapindex:
                    loc = sitemap.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        sub_urls = self.parse_sitemap(loc.text)
                        urls.extend(sub_urls)
                return urls

            # Parse regular sitemap
            urlset = root.findall('ns:url', namespaces)
            logger.info(f"Found {len(urlset)} URLs in sitemap")

            for url_elem in urlset:
                url_data = {}

                # Required: loc
                loc = url_elem.find('ns:loc', namespaces)
                if loc is not None and loc.text:
                    url_data['loc'] = loc.text.strip()
                else:
                    continue  # Skip if no URL

                # Optional: lastmod
                lastmod = url_elem.find('ns:lastmod', namespaces)
                if lastmod is not None and lastmod.text:
                    url_data['lastmod'] = lastmod.text.strip()
                else:
                    url_data['lastmod'] = None

                # Optional: changefreq
                changefreq = url_elem.find('ns:changefreq', namespaces)
                if changefreq is not None and changefreq.text:
                    url_data['changefreq'] = changefreq.text.strip()
                else:
                    url_data['changefreq'] = None

                # Optional: priority
                priority = url_elem.find('ns:priority', namespaces)
                if priority is not None and priority.text:
                    try:
                        url_data['priority'] = float(priority.text.strip())
                    except ValueError:
                        url_data['priority'] = None
                else:
                    url_data['priority'] = None

                urls.append(url_data)

        except ET.ParseError as e:
            logger.error(f"XML parse error for {sitemap_url}: {e}")
        except requests.RequestException as e:
            logger.error(f"Request error for {sitemap_url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error parsing sitemap {sitemap_url}: {e}")

        return urls

    def discover_and_parse(self, base_url: str) -> List[Dict[str, str]]:
        """
        Discover and parse sitemap in one step

        Args:
            base_url: Base URL of the website

        Returns:
            List of URL dictionaries
        """
        sitemap_url = self.discover_sitemap(base_url)
        if not sitemap_url:
            return []

        return self.parse_sitemap(sitemap_url)

    def urls_to_seeds(
        self,
        urls: List[Dict[str, str]],
        url_type: str = "sitemap",
        depth_limit: int = 5,
        min_priority: Optional[float] = None
    ) -> List[Dict[str, any]]:
        """
        Convert sitemap URLs to seed URL format

        Args:
            urls: List of URL dicts from sitemap
            url_type: Type to assign to seeds
            depth_limit: Depth limit for seeds
            min_priority: Minimum priority to include (0.0-1.0)

        Returns:
            List of seed URL dictionaries
        """
        seeds = []

        for url_data in urls:
            # Filter by priority if specified
            if min_priority is not None:
                priority = url_data.get('priority')
                if priority is None or priority < min_priority:
                    continue

            seeds.append({
                'url': url_data['loc'],
                'url_type': url_type,
                'depth_limit': depth_limit,
                'lastmod': url_data.get('lastmod'),
                'changefreq': url_data.get('changefreq'),
                'priority': url_data.get('priority')
            })

        return seeds
