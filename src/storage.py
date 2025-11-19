"""
Storage System
Handles saving scraped data to disk in organized structure
"""

import logging
import os
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, quote
import re


logger = logging.getLogger(__name__)


class StorageManager:
    """
    Manages storage of scraped content with organized directory structure.
    """

    def __init__(self, base_dir: str = "data", ngo_name: str = "default"):
        """
        Initialize storage manager.

        Args:
            base_dir: Base directory for data storage
            ngo_name: Name of the NGO being scraped
        """
        self.base_dir = Path(base_dir)
        self.ngo_name = self._sanitize_filename(ngo_name)
        self.session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create directory structure
        self.raw_dir = self.base_dir / "raw" / self.ngo_name / self.session_timestamp
        self.metadata_dir = self.base_dir / "metadata" / self.ngo_name / self.session_timestamp
        self.logs_dir = self.base_dir / "logs"

        self._create_directories()

        # Paths for different content types
        self.pages_dir = self.raw_dir / "pages"
        self.documents_dir = self.raw_dir / "documents"
        self.pages_dir.mkdir(exist_ok=True)
        self.documents_dir.mkdir(exist_ok=True)

        # Links and metadata storage
        self.links_file = self.raw_dir / "links.json"
        self.metadata_file = self.raw_dir / "metadata.json"

        # In-memory storage for links
        self.links: List[Dict] = []

        # Content hash tracking (for duplicate content detection)
        self.content_hashes: set = set()

        # Statistics
        self.stats = {
            'pages_saved': 0,
            'documents_saved': 0,
            'links_extracted': 0,
            'duplicate_content': 0,
            'errors': 0
        }

        logger.info(f"Storage initialized for {self.ngo_name} at {self.raw_dir}")

    def _create_directories(self):
        """Create necessary directory structure."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename to be safe for filesystem.

        Args:
            filename: Original filename

        Returns:
            Sanitized filename
        """
        # Remove or replace invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove leading/trailing spaces and dots
        filename = filename.strip('. ')
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        return filename or 'unnamed'

    def _url_to_filename(self, url: str, extension: str = '.html') -> str:
        """
        Convert URL to a safe filename.

        Args:
            url: URL to convert
            extension: File extension to use

        Returns:
            Safe filename
        """
        parsed = urlparse(url)
        path = parsed.path or 'index'

        # Remove leading/trailing slashes
        path = path.strip('/')

        # Replace slashes with underscores
        path = path.replace('/', '_')

        # Sanitize
        path = self._sanitize_filename(path)

        # Add hash of full URL to ensure uniqueness
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]

        # Ensure it has the right extension
        if not path.endswith(extension):
            path = f"{path}_{url_hash}{extension}"
        else:
            path = f"{path[:-len(extension)]}_{url_hash}{extension}"

        return path

    def _content_hash(self, content: bytes) -> str:
        """
        Create hash of content for duplicate detection.

        Args:
            content: Content bytes

        Returns:
            Hash string
        """
        return hashlib.sha256(content).hexdigest()

    def is_duplicate_content(self, content: bytes) -> bool:
        """
        Check if content has been seen before.

        Args:
            content: Content to check

        Returns:
            True if duplicate
        """
        content_hash = self._content_hash(content)
        if content_hash in self.content_hashes:
            self.stats['duplicate_content'] += 1
            return True
        self.content_hashes.add(content_hash)
        return False

    def save_page(self, url: str, content: bytes, encoding: str = 'utf-8',
                  check_duplicates: bool = True) -> Optional[str]:
        """
        Save HTML page content.

        Args:
            url: URL of the page
            content: Page content as bytes
            encoding: Content encoding
            check_duplicates: Whether to check for duplicate content

        Returns:
            Path to saved file or None if not saved
        """
        try:
            # Check for duplicates if requested
            if check_duplicates and self.is_duplicate_content(content):
                logger.debug(f"Duplicate content not saved: {url}")
                return None

            # Generate filename
            filename = self._url_to_filename(url, '.html')
            filepath = self.pages_dir / filename

            # Save content
            with open(filepath, 'wb') as f:
                f.write(content)

            self.stats['pages_saved'] += 1
            logger.debug(f"Saved page: {filepath}")

            # Also save metadata about this page
            self._save_page_metadata(url, filepath, len(content), encoding)

            return str(filepath)

        except Exception as e:
            logger.error(f"Error saving page {url}: {e}")
            self.stats['errors'] += 1
            return None

    def save_document(self, url: str, content: bytes, content_type: str = None) -> Optional[str]:
        """
        Save document (PDF, DOC, etc.).

        Args:
            url: URL of the document
            content: Document content as bytes
            content_type: MIME type of the content

        Returns:
            Path to saved file or None if not saved
        """
        try:
            # Determine extension from URL or content type
            parsed = urlparse(url)
            path = parsed.path
            if '.' in path:
                extension = '.' + path.split('.')[-1].lower()
            elif content_type:
                # Map content type to extension
                type_map = {
                    'application/pdf': '.pdf',
                    'application/msword': '.doc',
                    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
                    'application/vnd.ms-excel': '.xls',
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx'
                }
                extension = type_map.get(content_type, '.bin')
            else:
                extension = '.bin'

            # Check for duplicates
            if self.is_duplicate_content(content):
                logger.debug(f"Duplicate document not saved: {url}")
                return None

            # Generate filename
            filename = self._url_to_filename(url, extension)
            filepath = self.documents_dir / filename

            # Save content
            with open(filepath, 'wb') as f:
                f.write(content)

            self.stats['documents_saved'] += 1
            logger.info(f"Saved document: {filepath}")

            # Save metadata
            self._save_document_metadata(url, filepath, len(content), content_type)

            return str(filepath)

        except Exception as e:
            logger.error(f"Error saving document {url}: {e}")
            self.stats['errors'] += 1
            return None

    def add_links(self, source_url: str, links: List[Dict], publication_date: Optional[str] = None):
        """
        Add extracted links to storage.

        Args:
            source_url: URL where links were found
            links: List of link dicts with 'url', 'text', 'type' keys
            publication_date: Publication date of the source page (ISO format)
        """
        for link in links:
            self.links.append({
                'source_url': source_url,
                'target_url': link.get('url'),
                'anchor_text': link.get('text', ''),
                'link_type': link.get('type', 'unknown'),  # internal/external
                'publication_date': publication_date or 'N/A',
                'timestamp': datetime.now().isoformat()
            })
            self.stats['links_extracted'] += 1

    def _save_page_metadata(self, url: str, filepath: Path, size: int, encoding: str):
        """Save metadata about a scraped page."""
        metadata_file = self.metadata_dir / 'pages_metadata.jsonl'
        with open(metadata_file, 'a', encoding='utf-8') as f:
            json.dump({
                'url': url,
                'filepath': str(filepath),
                'size_bytes': size,
                'encoding': encoding,
                'timestamp': datetime.now().isoformat()
            }, f)
            f.write('\n')

    def _save_document_metadata(self, url: str, filepath: Path, size: int, content_type: Optional[str]):
        """Save metadata about a scraped document."""
        metadata_file = self.metadata_dir / 'documents_metadata.jsonl'
        with open(metadata_file, 'a', encoding='utf-8') as f:
            json.dump({
                'url': url,
                'filepath': str(filepath),
                'size_bytes': size,
                'content_type': content_type,
                'timestamp': datetime.now().isoformat()
            }, f)
            f.write('\n')

    def save_links(self):
        """Save all collected links to JSON file."""
        try:
            with open(self.links_file, 'w', encoding='utf-8') as f:
                json.dump(self.links, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(self.links)} links to {self.links_file}")
        except Exception as e:
            logger.error(f"Error saving links: {e}")
            self.stats['errors'] += 1

    def save_session_metadata(self, additional_data: Optional[Dict] = None):
        """
        Save metadata about the scraping session.

        Args:
            additional_data: Additional metadata to include
        """
        try:
            metadata = {
                'ngo_name': self.ngo_name,
                'session_timestamp': self.session_timestamp,
                'start_time': self.session_timestamp,
                'end_time': datetime.now().strftime("%Y%m%d_%H%M%S"),
                'statistics': self.stats,
                'storage_paths': {
                    'raw_dir': str(self.raw_dir),
                    'pages_dir': str(self.pages_dir),
                    'documents_dir': str(self.documents_dir),
                    'links_file': str(self.links_file)
                }
            }

            if additional_data:
                metadata.update(additional_data)

            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved session metadata to {self.metadata_file}")

        except Exception as e:
            logger.error(f"Error saving session metadata: {e}")
            self.stats['errors'] += 1

    def get_stats(self) -> Dict:
        """Get storage statistics."""
        return self.stats.copy()

    def finalize(self, additional_metadata: Optional[Dict] = None):
        """
        Finalize storage - save links and metadata.

        Args:
            additional_metadata: Additional metadata to save
        """
        logger.info("Finalizing storage...")
        self.save_links()
        self.save_session_metadata(additional_metadata)
        logger.info(f"Storage finalized. Stats: {self.stats}")
