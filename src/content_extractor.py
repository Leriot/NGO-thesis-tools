"""
Content Extractor
Extracts links, metadata, and structured content from HTML pages
"""

import logging
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re
from datetime import datetime


logger = logging.getLogger(__name__)


class ContentExtractor:
    """
    Extracts structured content from HTML pages.
    Focuses on links, metadata, and semantic content.
    """

    def __init__(self, base_url: str):
        """
        Initialize content extractor.

        Args:
            base_url: Base URL for resolving relative links
        """
        self.base_url = base_url
        self.base_domain = self._extract_domain(base_url)

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def extract_links(self, html: str, source_url: str) -> List[Dict]:
        """
        Extract all links from HTML with metadata.

        Args:
            html: HTML content
            source_url: URL of the page (for resolving relative links)

        Returns:
            List of link dictionaries with url, text, and type
        """
        links = []
        seen_urls = set()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            for anchor in soup.find_all('a', href=True):
                href = anchor.get('href', '').strip()

                if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue

                # Resolve relative URLs
                absolute_url = urljoin(source_url, href)

                # Skip duplicates in this page
                if absolute_url in seen_urls:
                    continue
                seen_urls.add(absolute_url)

                # Determine if internal or external
                link_domain = self._extract_domain(absolute_url)
                is_internal = (link_domain == self.base_domain or
                              link_domain.endswith(f'.{self.base_domain}'))

                # Extract anchor text
                anchor_text = anchor.get_text(strip=True)

                # Extract title attribute if available
                title = anchor.get('title', '')

                links.append({
                    'url': absolute_url,
                    'text': anchor_text,
                    'title': title,
                    'type': 'internal' if is_internal else 'external'
                })

            logger.debug(f"Extracted {len(links)} links from {source_url}")

        except Exception as e:
            logger.error(f"Error extracting links from {source_url}: {e}")

        return links

    def extract_metadata(self, html: str, url: str) -> Dict:
        """
        Extract metadata from HTML page.

        Args:
            html: HTML content
            url: URL of the page

        Returns:
            Dictionary with metadata
        """
        metadata = {
            'url': url,
            'title': None,
            'description': None,
            'keywords': None,
            'author': None,
            'published_date': None,
            'modified_date': None,
            'language': None,
            'og_type': None,
            'og_title': None,
            'og_description': None,
        }

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Title
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.get_text(strip=True)

            # Meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name', '').lower()
                property_attr = meta.get('property', '').lower()
                content = meta.get('content', '').strip()

                if not content:
                    continue

                # Standard meta tags
                if name == 'description':
                    metadata['description'] = content
                elif name == 'keywords':
                    metadata['keywords'] = content
                elif name == 'author':
                    metadata['author'] = content
                elif name in ('date', 'pubdate', 'publishdate', 'publication_date'):
                    metadata['published_date'] = self._parse_date(content)
                elif name in ('last-modified', 'modified', 'updated'):
                    metadata['modified_date'] = self._parse_date(content)
                elif name == 'language':
                    metadata['language'] = content

                # Open Graph tags
                elif property_attr == 'og:type':
                    metadata['og_type'] = content
                elif property_attr == 'og:title':
                    metadata['og_title'] = content
                elif property_attr == 'og:description':
                    metadata['og_description'] = content

            # Try to extract date from content (common patterns)
            if not metadata['published_date']:
                date = self._extract_date_from_content(soup)
                if date:
                    metadata['published_date'] = date

            # Language from html tag
            if not metadata['language']:
                html_tag = soup.find('html')
                if html_tag:
                    metadata['language'] = html_tag.get('lang', '')

            logger.debug(f"Extracted metadata from {url}")

        except Exception as e:
            logger.error(f"Error extracting metadata from {url}: {e}")

        return metadata

    def _parse_date(self, date_string: str) -> Optional[str]:
        """
        Parse date string to ISO format.

        Args:
            date_string: Date string to parse

        Returns:
            ISO formatted date string or None
        """
        # Common date formats
        formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%d.%m.%Y',
            '%d/%m/%Y',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_string.strip(), fmt)
                return dt.date().isoformat()
            except (ValueError, AttributeError):
                continue

        return None

    def _extract_date_from_content(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Try to extract publication date from common HTML patterns.

        Args:
            soup: BeautifulSoup object

        Returns:
            ISO date string or None
        """
        # Common date patterns in HTML
        date_patterns = [
            # Czech date patterns
            r'\b(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\b',
            # ISO dates
            r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b',
            # Other formats
            r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b',
        ]

        # Look for time elements
        time_elements = soup.find_all(['time', 'span', 'div'], class_=re.compile(r'date|time|publish', re.I))
        for elem in time_elements:
            datetime_attr = elem.get('datetime')
            if datetime_attr:
                date = self._parse_date(datetime_attr)
                if date:
                    return date

            text = elem.get_text(strip=True)
            for pattern in date_patterns:
                match = re.search(pattern, text)
                if match:
                    # Try to parse the matched date
                    date_str = match.group(0)
                    date = self._parse_date(date_str)
                    if date:
                        return date

        return None

    def extract_text_content(self, html: str) -> str:
        """
        Extract main text content from HTML, removing scripts, styles, etc.

        Args:
            html: HTML content

        Returns:
            Plain text content
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()

            # Get text
            text = soup.get_text(separator=' ', strip=True)

            # Clean up whitespace
            text = re.sub(r'\s+', ' ', text)

            return text

        except Exception as e:
            logger.error(f"Error extracting text content: {e}")
            return ""

    def identify_page_type(self, html: str, url: str) -> str:
        """
        Identify the type of page based on URL and content.

        Args:
            html: HTML content
            url: Page URL

        Returns:
            Page type string
        """
        url_lower = url.lower()

        # URL-based identification
        if any(pattern in url_lower for pattern in ['/publikace', '/publications', '/vyrocni-zpravy']):
            return 'publications'
        elif any(pattern in url_lower for pattern in ['/tiskove-zpravy', '/press-release', '/press']):
            return 'press_release'
        elif any(pattern in url_lower for pattern in ['/aktuality', '/news', '/clanky', '/articles']):
            return 'news'
        elif any(pattern in url_lower for pattern in ['/akce', '/events', '/udalosti']):
            return 'events'
        elif any(pattern in url_lower for pattern in ['/o-nas', '/about', '/team', '/lide', '/people']):
            return 'about'
        elif any(pattern in url_lower for pattern in ['/kontakt', '/contact']):
            return 'contact'
        elif any(pattern in url_lower for pattern in ['/kampane', '/campaigns']):
            return 'campaign'
        elif any(pattern in url_lower for pattern in ['/projekty', '/projects']):
            return 'projects'

        # Content-based identification
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.find('title')
            title_text = title.get_text().lower() if title else ''

            if any(word in title_text for word in ['publikace', 'publication', 'report', 'zpráva']):
                return 'publications'
            elif any(word in title_text for word in ['tisková zpráva', 'press release']):
                return 'press_release'
            elif any(word in title_text for word in ['aktuality', 'news', 'article']):
                return 'news'

        except Exception as e:
            logger.debug(f"Error in content-based page type identification: {e}")

        return 'general'

    def extract_document_links(self, html: str, source_url: str,
                               extensions: List[str] = None) -> List[Dict]:
        """
        Extract links to documents (PDFs, DOCs, etc.).

        Args:
            html: HTML content
            source_url: URL of the page
            extensions: List of file extensions to look for

        Returns:
            List of document link dictionaries
        """
        if extensions is None:
            extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']

        documents = []
        seen_urls = set()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            for anchor in soup.find_all('a', href=True):
                href = anchor.get('href', '').strip()

                if not href:
                    continue

                # Resolve relative URLs
                absolute_url = urljoin(source_url, href)

                # Check if it's a document
                url_lower = absolute_url.lower()
                is_document = any(url_lower.endswith(ext) for ext in extensions)

                if is_document and absolute_url not in seen_urls:
                    seen_urls.add(absolute_url)

                    # Extract anchor text
                    anchor_text = anchor.get_text(strip=True)

                    # Determine document type
                    doc_type = next((ext for ext in extensions if url_lower.endswith(ext)), 'unknown')

                    documents.append({
                        'url': absolute_url,
                        'text': anchor_text,
                        'type': doc_type,
                        'source_page': source_url
                    })

            if documents:
                logger.info(f"Found {len(documents)} document links on {source_url}")

        except Exception as e:
            logger.error(f"Error extracting document links from {source_url}: {e}")

        return documents

    def extract_personnel_info(self, html: str) -> List[Dict]:
        """
        Extract personnel information from about/team pages.

        Args:
            html: HTML content

        Returns:
            List of personnel dictionaries with name, role, etc.
        """
        personnel = []

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Look for common personnel section patterns
            # This is a simple heuristic-based approach
            potential_sections = soup.find_all(['div', 'section', 'article'],
                                              class_=re.compile(r'team|staff|people|person|member', re.I))

            for section in potential_sections:
                # Look for names (usually in headings or strong tags)
                names = section.find_all(['h2', 'h3', 'h4', 'strong', 'b'])

                for name_elem in names:
                    name = name_elem.get_text(strip=True)

                    # Skip if too short or too long
                    if len(name) < 3 or len(name) > 100:
                        continue

                    # Look for role/position (often in nearby elements)
                    role = ''
                    next_elem = name_elem.find_next(['p', 'div', 'span'])
                    if next_elem:
                        role = next_elem.get_text(strip=True)[:200]

                    personnel.append({
                        'name': name,
                        'role': role
                    })

            if personnel:
                logger.debug(f"Extracted {len(personnel)} personnel records")

        except Exception as e:
            logger.error(f"Error extracting personnel info: {e}")

        return personnel
