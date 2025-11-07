"""
Basic tests for the NGO scraper components
"""

import pytest
from src.url_manager import URLManager
from src.content_extractor import ContentExtractor
from src.robots_handler import RobotsHandler


class TestURLManager:
    """Tests for URL management and normalization."""

    def test_url_normalization(self):
        """Test URL normalization."""
        manager = URLManager("example.org")

        # Test basic normalization
        url1 = manager.normalize_url("https://example.org/page/")
        url2 = manager.normalize_url("https://example.org/page")
        assert url1 == url2

        # Test query parameter sorting
        url3 = manager.normalize_url("https://example.org/page?b=2&a=1")
        url4 = manager.normalize_url("https://example.org/page?a=1&b=2")
        assert url3 == url4

    def test_internal_url_detection(self):
        """Test internal vs external URL detection."""
        manager = URLManager("example.org")

        assert manager.is_internal_url("https://example.org/page") is True
        assert manager.is_internal_url("https://www.example.org/page") is True
        assert manager.is_internal_url("https://other.com/page") is False

    def test_url_deduplication(self):
        """Test that duplicate URLs are not added."""
        manager = URLManager("example.org", max_depth=3, max_pages=100)

        # Add same URL twice
        result1 = manager.add_url("https://example.org/page", depth=0)
        result2 = manager.add_url("https://example.org/page", depth=0)

        assert result1 is True
        assert result2 is False  # Should be rejected as duplicate

    def test_depth_limiting(self):
        """Test that depth limits are enforced."""
        manager = URLManager("example.org", max_depth=2, max_pages=100)

        # Should succeed within depth limit
        result1 = manager.add_url("https://example.org/page1", depth=1)
        assert result1 is True

        # Should fail exceeding depth limit
        result2 = manager.add_url("https://example.org/page2", depth=3)
        assert result2 is False

    def test_url_queue_ordering(self):
        """Test that URLs are prioritized correctly."""
        manager = URLManager("example.org", max_depth=3, max_pages=100)

        # Add URLs with different priorities
        manager.add_url("https://example.org/low", depth=0, priority=3)
        manager.add_url("https://example.org/high", depth=0, priority=0)
        manager.add_url("https://example.org/medium", depth=0, priority=1)

        # Should get high priority first
        depth, url, _ = manager.get_next_url()
        assert "high" in url


class TestContentExtractor:
    """Tests for content extraction."""

    def test_link_extraction(self):
        """Test extracting links from HTML."""
        extractor = ContentExtractor("https://example.org")

        html = """
        <html>
        <body>
            <a href="/page1">Internal Link</a>
            <a href="https://other.com/page2">External Link</a>
            <a href="javascript:void(0)">JS Link</a>
        </body>
        </html>
        """

        links = extractor.extract_links(html, "https://example.org")

        # Should extract 2 valid links (not the javascript one)
        assert len(links) >= 2

        # Check that internal/external classification works
        link_types = [link['type'] for link in links]
        assert 'internal' in link_types
        assert 'external' in link_types

    def test_metadata_extraction(self):
        """Test extracting metadata from HTML."""
        extractor = ContentExtractor("https://example.org")

        html = """
        <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="Test description">
            <meta name="author" content="Test Author">
        </head>
        <body>
            <p>Content</p>
        </body>
        </html>
        """

        metadata = extractor.extract_metadata(html, "https://example.org/page")

        assert metadata['title'] == "Test Page"
        assert metadata['description'] == "Test description"
        assert metadata['author'] == "Test Author"

    def test_document_link_extraction(self):
        """Test extracting document links."""
        extractor = ContentExtractor("https://example.org")

        html = """
        <html>
        <body>
            <a href="/doc1.pdf">PDF Document</a>
            <a href="/doc2.doc">Word Document</a>
            <a href="/page.html">HTML Page</a>
        </body>
        </html>
        """

        documents = extractor.extract_document_links(
            html,
            "https://example.org",
            extensions=['.pdf', '.doc', '.docx']
        )

        # Should find 2 documents (PDF and DOC)
        assert len(documents) == 2

        # Check that both types are found
        doc_types = [doc['type'] for doc in documents]
        assert '.pdf' in doc_types
        assert '.doc' in doc_types

    def test_page_type_identification(self):
        """Test identifying page types."""
        extractor = ContentExtractor("https://example.org")

        # Test URL-based identification
        assert extractor.identify_page_type("", "https://example.org/publikace") == "publications"
        assert extractor.identify_page_type("", "https://example.org/news") == "news"
        assert extractor.identify_page_type("", "https://example.org/o-nas") == "about"
        assert extractor.identify_page_type("", "https://example.org/contact") == "contact"


class TestRobotsHandler:
    """Tests for robots.txt handling."""

    def test_robots_handler_initialization(self):
        """Test that robots handler can be initialized."""
        handler = RobotsHandler("TestBot/1.0")
        assert handler.user_agent == "TestBot/1.0"

    def test_robots_url_generation(self):
        """Test robots.txt URL generation."""
        handler = RobotsHandler("TestBot/1.0")

        robots_url = handler._get_robots_url("https://example.org/page/subpage")
        assert robots_url == "https://example.org/robots.txt"

    def test_domain_extraction(self):
        """Test domain extraction from URL."""
        handler = RobotsHandler("TestBot/1.0")

        domain = handler._get_domain("https://example.org/page/subpage?query=1")
        assert domain == "https://example.org"


class TestURLExclusion:
    """Tests for URL exclusion patterns."""

    def test_exclusion_patterns(self):
        """Test that exclusion patterns work correctly."""
        manager = URLManager("example.org")

        exclusions = ['/admin/', '/login/', '/wp-admin/']

        assert manager.should_exclude_url("https://example.org/admin/page", exclusions) is True
        assert manager.should_exclude_url("https://example.org/login/", exclusions) is True
        assert manager.should_exclude_url("https://example.org/wp-admin/", exclusions) is True
        assert manager.should_exclude_url("https://example.org/public/page", exclusions) is False


class TestURLPriority:
    """Tests for URL prioritization."""

    def test_priority_patterns(self):
        """Test that priority patterns work correctly."""
        manager = URLManager("example.org")

        priority_patterns = {
            'high': ['/publikace/', '/publications/'],
            'medium': ['/news/', '/events/'],
            'low': ['/gallery/']
        }

        assert manager.get_url_priority("https://example.org/publikace/doc", priority_patterns) == 0
        assert manager.get_url_priority("https://example.org/news/article", priority_patterns) == 1
        assert manager.get_url_priority("https://example.org/gallery/photo", priority_patterns) == 2
        assert manager.get_url_priority("https://example.org/random", priority_patterns) == 3


# Run tests with: pytest tests/test_scraper.py -v
