#!/usr/bin/env python3
"""
Content Cleaner & Relevance Filter - Data Cleanup Tool #2

Purpose:
    Cleans HTML content, removes boilerplate, deduplicates documents, and filters
    for relevance to NGO network analysis using Czech social science keywords.

What it does:
    1. Cleans HTML by removing:
       - Boilerplate (nav, footer, ads, menus)
       - High link-density blocks (>60% links = navigation)
       - Scripts, styles, forms
    2. Deduplicates using shingling + Jaccard similarity (0.85 threshold)
    3. Scores relevance using Czech keywords:
       - Cooperation terms (spolupráce, partner) - weight 3
       - Funding terms (grant, dotace) - weight 2-3
       - Policy actors (ministerstvo, parlament) - weight 1-3
       - Actions (podpora, projekt) - weight 1
    4. Filters using DUAL criteria:
       - Raw score: ≥5 points total
       - Density score: ≥0.5 points per 100 words (prevents long irrelevant docs)
    5. Outputs to:
       - processed_data/relevant/ - passes both filters
       - processed_data/irrelevant/ - fails one or both filters
       - processed_data/duplicates/{master}/ - grouped by first occurrence

Usage:
    # Process all HTML files for an organization
    python scripts/filter_content.py --org "Hnutí DUHA"

    # Process specific scrape session
    python scripts/filter_content.py --org "Hnutí DUHA" --session "20240115_103000"

    # Process all organizations
    python scripts/filter_content.py --all

    # Adjust filtering thresholds
    python scripts/filter_content.py --org "Arnika" --min-score 10 --min-density 1.0

    # More lenient filtering
    python scripts/filter_content.py --org "Arnika" --min-score 3 --min-density 0.3

Author: Thesis Research - NGO Network Analysis
"""

import os
import sys
import re
import string
import argparse
import yaml
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from bs4 import BeautifulSoup

# --- DEFAULT PATHS ---
DEFAULT_CONFIG_PATH = "config/content_filter_keywords.yaml"

# --- FILTERING CONFIGURATION (The "Tuning Knobs") ---

# 1. Minimum Absolute Score: Document must have at least this many points total
DEFAULT_MIN_RAW_SCORE = 5

# 2. Minimum Density Score: Points per 100 words (creates "Length Penalty")
DEFAULT_MIN_DENSITY = 0.5

# 3. Deduplication threshold (Jaccard similarity)
DEFAULT_SIMILARITY_THRESHOLD = 0.85


def load_keyword_config(config_path):
    """
    Load keyword configuration from YAML file.

    Returns:
        dict: Configuration containing keywords, boilerplate terms, and thresholds
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # Build keyword dictionary with enhanced variations
        keywords_dict = {}

        for category, keywords_list in config['keywords'].items():
            for keyword_entry in keywords_list:
                root = keyword_entry['root']
                weight = keyword_entry['weight']
                variations = keyword_entry.get('variations', [])

                # Store both root and all variations
                # Using a tuple (root, variations_set) as value
                keywords_dict[root] = {
                    'weight': weight,
                    'variations': set(variations) if variations else set(),
                    'category': category,
                    'description': keyword_entry.get('description', '')
                }

        return {
            'keywords': keywords_dict,
            'boilerplate_terms': config['boilerplate']['junk_terms'],
            'safe_terms': config['boilerplate']['safe_terms'],
            'link_density_threshold': config['boilerplate']['link_density_threshold'],
            'filtering': config['filtering']
        }

    except FileNotFoundError:
        print(f"⚠ Warning: Keyword config file not found: {config_path}")
        print(f"  Using fallback minimal keywords...")
        # Fallback to minimal keywords
        return {
            'keywords': {
                'spoluprác': {'weight': 3, 'variations': set(), 'category': 'relations'},
                'partner': {'weight': 3, 'variations': set(), 'category': 'relations'},
            },
            'boilerplate_terms': ['nav', 'footer', 'header', 'sidebar', 'menu'],
            'safe_terms': ['main', 'article', 'content'],
            'link_density_threshold': 0.6,
            'filtering': {
                'min_raw_score': DEFAULT_MIN_RAW_SCORE,
                'min_density_score': DEFAULT_MIN_DENSITY,
                'similarity_threshold': DEFAULT_SIMILARITY_THRESHOLD
            }
        }
    except Exception as e:
        print(f"⚠ Error loading keyword config: {e}")
        raise


class ContentFilter:
    """Cleans, deduplicates, and filters scraped content for relevance."""

    def __init__(self, data_root="data", min_raw_score=None,
                 min_density=None, similarity_threshold=None, config_path=None):
        self.data_root = Path(data_root)

        # Load configuration from file
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH

        self.config = load_keyword_config(config_path)

        # Use provided values or fall back to config file or defaults
        self.min_raw_score = min_raw_score if min_raw_score is not None else \
                             self.config['filtering']['min_raw_score']
        self.min_density = min_density if min_density is not None else \
                           self.config['filtering']['min_density_score']
        self.similarity_threshold = similarity_threshold if similarity_threshold is not None else \
                                   self.config['filtering']['similarity_threshold']

        # Store keywords and boilerplate config
        self.keywords = self.config['keywords']
        self.boilerplate_terms = self.config['boilerplate_terms']
        self.safe_terms = self.config['safe_terms']
        self.link_density_threshold = self.config['link_density_threshold']

        self.stats = {
            'processed': 0,
            'relevant': 0,
            'irrelevant': 0,
            'duplicates': 0,
            'errors': 0
        }

        # Track unique documents by session (for deduplication)
        self.unique_docs = {}

    def find_organizations(self):
        """Find all organizations with scraped data."""
        raw_path = self.data_root / "raw"
        if not raw_path.exists():
            return []
        return [d.name for d in raw_path.iterdir() if d.is_dir()]

    def find_sessions(self, org_name):
        """Find all scrape sessions for an organization."""
        org_path = self.data_root / "raw" / org_name
        if not org_path.exists():
            return []
        return [d.name for d in org_path.iterdir() if d.is_dir()]

    def clean_html_content(self, html_content):
        """
        Parse HTML, strip boilerplate, and return clean text.

        Returns:
            str: Cleaned text content
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove standard junk tags
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'form', 'button', 'input']):
            tag.decompose()

        # Remove elements by class/id/role heuristics
        for element in soup.find_all(True):
            if not hasattr(element, 'attrs') or element.attrs is None:
                continue

            # Safe Mode Check - protect content elements
            id_str = str(element.get('id', '')).lower()
            class_str = " ".join(element.get('class', [])).lower()

            if any(safe in class_str for safe in self.safe_terms) or any(safe in id_str for safe in self.safe_terms):
                continue

            # Role Check - remove navigation/banner roles
            if element.get('role'):
                role = str(element.get('role')).lower()
                if role in ['banner', 'navigation', 'contentinfo', 'search']:
                    element.decompose()
                    continue

            # ID/Class Junk Check
            if element.get('id'):
                if any(term in id_str for term in self.boilerplate_terms):
                    element.decompose()
                    continue
            if element.get('class'):
                if any(term in class_str for term in self.boilerplate_terms):
                    element.decompose()
                    continue

        # Link Density Check - remove navigation-heavy blocks
        for div in soup.find_all(['div', 'ul', 'section']):
            text_len = len(div.get_text(strip=True))
            if text_len < 10:
                continue

            link_len = sum(len(a.get_text(strip=True)) for a in div.find_all('a'))

            if text_len > 0:
                density = link_len / text_len
                if density > self.link_density_threshold:
                    div.decompose()

        # Extract cleaned text
        text = soup.get_text(separator=' ')
        clean_text = re.sub(r'\s+', ' ', text).strip()
        return clean_text

    def get_shingles(self, text, k=3):
        """
        Create k-word shingles for deduplication.

        Args:
            text: Input text
            k: Shingle size (default: 3-word sequences)

        Returns:
            set: Set of k-word tuples
        """
        translator = str.maketrans('', '', string.punctuation)
        clean_tokens = text.translate(translator).lower().split()
        if len(clean_tokens) < k:
            return set(clean_tokens)
        return set(tuple(clean_tokens[i:i+k]) for i in range(len(clean_tokens) - k + 1))

    def calculate_jaccard_similarity(self, set_a, set_b):
        """Calculate Jaccard similarity between two sets."""
        if not set_a and not set_b:
            return 0.0
        intersection = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        return intersection / union if union > 0 else 0.0

    def calculate_relevance_metrics(self, text):
        """
        Calculate both Raw Score (total weight) and Density Score (weight per 100 words).

        Uses both root matching and exact variations for better accuracy.
        For each keyword, counts occurrences of:
        1. The root (catches most variations)
        2. Exact variations (for precision)

        Takes the maximum count to avoid double-counting.

        Returns:
            tuple: (raw_score, density_score, found_stats)
        """
        text_lower = text.lower()
        words = text_lower.split()
        word_count = len(words)

        if word_count == 0:
            return 0, 0, []

        raw_score = 0
        found_stats = []

        for root, keyword_data in self.keywords.items():
            weight = keyword_data['weight']
            variations = keyword_data['variations']

            # Count root occurrences
            root_count = text_lower.count(root)

            # Count exact variation matches (using word boundaries)
            # Build regex pattern for whole word matching
            if variations:
                # Create pattern like: \b(spolupráce|spolupracovat|...)\b
                variation_pattern = r'\b(' + '|'.join(re.escape(v.lower()) for v in variations) + r')\b'
                variation_matches = re.findall(variation_pattern, text_lower)
                variation_count = len(variation_matches)

                # Use the maximum to avoid double-counting
                # (variations already contain the root in most cases)
                count = max(root_count, variation_count)
            else:
                count = root_count

            if count > 0:
                term_score = count * weight
                raw_score += term_score
                category = keyword_data.get('category', '')
                found_stats.append(f"{root}({count}×w{weight})")

        # Density Score: Points per 100 words
        density_score = (raw_score / word_count) * 100

        return raw_score, density_score, found_stats

    def process_file(self, html_path, output_dirs):
        """
        Process a single HTML file.

        Returns:
            str: Status - 'relevant', 'irrelevant', 'duplicate', or 'error'
        """
        try:
            # Read HTML
            html_content = html_path.read_text(encoding='utf-8', errors='ignore')

            # 1. CLEAN
            cleaned_text = self.clean_html_content(html_content)
            if len(cleaned_text) < 50:
                return 'error'  # Too short after cleaning

            # 2. DEDUPLICATE
            current_shingles = self.get_shingles(cleaned_text)
            if not current_shingles:
                return 'error'

            is_duplicate = False
            matched_master = None

            for master_name, master_shingles in self.unique_docs.items():
                score = self.calculate_jaccard_similarity(current_shingles, master_shingles)
                if score >= self.similarity_threshold:
                    is_duplicate = True
                    matched_master = master_name
                    break

            output_filename = html_path.stem + ".txt"

            if is_duplicate:
                # Save as duplicate
                master_folder = Path(matched_master).stem
                dupe_path = output_dirs['duplicates'] / master_folder
                dupe_path.mkdir(parents=True, exist_ok=True)
                (dupe_path / output_filename).write_text(cleaned_text, encoding='utf-8')
                print(f"  ≈ Duplicate: {html_path.name} → matches {matched_master}")
                return 'duplicate'

            else:
                # 3. FILTER RELEVANCE (using Density)
                self.unique_docs[html_path.name] = current_shingles

                raw_score, density_score, found_stats = self.calculate_relevance_metrics(cleaned_text)

                # Decision logic: BOTH criteria must pass
                is_relevant = (raw_score >= self.min_raw_score) and (density_score >= self.min_density)

                if is_relevant:
                    target_path = output_dirs['relevant'] / output_filename
                    print(f"  ✓ Relevant: {html_path.name} (Raw: {raw_score}, Density: {density_score:.2f})")
                    status = 'relevant'
                else:
                    target_path = output_dirs['irrelevant'] / output_filename
                    print(f"  ✗ Filtered: {html_path.name} (Raw: {raw_score}, Density: {density_score:.2f})")
                    status = 'irrelevant'

                # Add metadata header
                metadata = f"SOURCE_FILE: {html_path.name}\n"
                metadata += f"PROCESSED: {datetime.now().isoformat()}\n"
                metadata += f"RELEVANCE_RAW_SCORE: {raw_score}\n"
                metadata += f"RELEVANCE_DENSITY: {density_score:.2f}\n"
                metadata += f"WORD_COUNT: {len(cleaned_text.split())}\n"
                if found_stats:
                    metadata += f"KEYWORDS_FOUND: {', '.join(found_stats)}\n"
                metadata += "\n" + "="*80 + "\n\n"

                target_path.write_text(metadata + cleaned_text, encoding='utf-8')
                return status

        except Exception as e:
            print(f"  ✗ Error processing {html_path.name}: {e}")
            return 'error'

    def process_session(self, org_name, session_name):
        """Process all HTML files in a specific scrape session."""
        # Input path
        session_path = self.data_root / "raw" / org_name / session_name
        pages_path = session_path / "pages"

        if not pages_path.exists():
            print(f"  No pages folder found: {pages_path}")
            return

        # Output paths
        output_base = self.data_root / "processed" / org_name / session_name
        output_dirs = {
            'relevant': output_base / "relevant",
            'irrelevant': output_base / "irrelevant",
            'duplicates': output_base / "duplicates"
        }

        for dir_path in output_dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)

        # Find all HTML files
        html_files = list(pages_path.glob("*.html")) + list(pages_path.glob("*.htm"))

        if not html_files:
            print(f"  No HTML files found in {pages_path}")
            return

        print(f"\n📄 Processing {len(html_files)} HTML files from {session_name}")
        print(f"   Output: {output_base}")

        # Reset deduplication for this session
        self.unique_docs = {}

        # Process each file
        for html_path in html_files:
            result = self.process_file(html_path, output_dirs)
            self.stats['processed'] += 1

            if result == 'relevant':
                self.stats['relevant'] += 1
            elif result == 'irrelevant':
                self.stats['irrelevant'] += 1
            elif result == 'duplicate':
                self.stats['duplicates'] += 1
            else:
                self.stats['errors'] += 1

    def process_organization(self, org_name, session_filter=None):
        """Process all sessions for an organization."""
        sessions = self.find_sessions(org_name)

        if not sessions:
            print(f"⚠ No scrape sessions found for '{org_name}'")
            return

        if session_filter:
            sessions = [s for s in sessions if s == session_filter]
            if not sessions:
                print(f"⚠ Session '{session_filter}' not found for '{org_name}'")
                return

        print(f"\n{'='*80}")
        print(f"Processing Organization: {org_name}")
        print(f"{'='*80}")
        print(f"Sessions: {len(sessions)}")
        print(f"Filters: Raw Score ≥{self.min_raw_score}, Density ≥{self.min_density}/100 words")

        for session in sessions:
            self.process_session(org_name, session)

    def process_all(self):
        """Process all organizations and sessions."""
        orgs = self.find_organizations()

        if not orgs:
            print("⚠ No organizations found in data/raw/")
            return

        print(f"\n🔍 Found {len(orgs)} organizations: {', '.join(orgs)}")

        for org in orgs:
            self.process_organization(org)

    def print_summary(self):
        """Print processing statistics."""
        print(f"\n{'='*80}")
        print("📊 PROCESSING SUMMARY")
        print(f"{'='*80}")
        print(f"Total HTML files processed: {self.stats['processed']}")
        print(f"  ✓ Relevant (kept):        {self.stats['relevant']}")
        print(f"  ✗ Irrelevant (filtered):  {self.stats['irrelevant']}")
        print(f"  ≈ Duplicates (removed):   {self.stats['duplicates']}")
        print(f"  ⚠ Errors:                 {self.stats['errors']}")

        if self.stats['relevant'] > 0:
            keep_rate = (self.stats['relevant'] / self.stats['processed']) * 100
            print(f"\n📈 Keep Rate: {keep_rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Clean, deduplicate, and filter HTML content for relevance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all HTML files for one organization
  python scripts/filter_content.py --org "Hnutí DUHA"

  # Process specific scrape session
  python scripts/filter_content.py --org "Hnutí DUHA" --session "20240115_103000"

  # Process all organizations
  python scripts/filter_content.py --all

  # Stricter filtering
  python scripts/filter_content.py --org "Arnika" --min-score 10 --min-density 1.0

  # More lenient filtering
  python scripts/filter_content.py --org "Greenpeace ČR" --min-score 3 --min-density 0.3
        """
    )

    parser.add_argument('--org', type=str, help='Organization name to process')
    parser.add_argument('--session', type=str, help='Specific session timestamp to process')
    parser.add_argument('--all', action='store_true', help='Process all organizations')
    parser.add_argument('--data-root', type=str, default='data',
                       help='Root data directory (default: data)')
    parser.add_argument('--config', type=str, default=None,
                       help=f'Path to keyword config file (default: {DEFAULT_CONFIG_PATH})')
    parser.add_argument('--min-score', type=int, default=None,
                       help=f'Minimum raw relevance score (overrides config, default from config: 5)')
    parser.add_argument('--min-density', type=float, default=None,
                       help=f'Minimum density score per 100 words (overrides config, default from config: 0.5)')
    parser.add_argument('--similarity', type=float, default=None,
                       help=f'Jaccard similarity threshold for dupes (overrides config, default from config: 0.85)')
    parser.add_argument('--list', action='store_true',
                       help='List available organizations and sessions')

    args = parser.parse_args()

    # Initialize filter
    filter_obj = ContentFilter(
        data_root=args.data_root,
        min_raw_score=args.min_score,
        min_density=args.min_density,
        similarity_threshold=args.similarity,
        config_path=args.config
    )

    # Handle --list command
    if args.list:
        orgs = filter_obj.find_organizations()
        if not orgs:
            print("No organizations found in data/raw/")
            return

        print("\n📁 Available Organizations and Sessions:")
        for org in orgs:
            sessions = filter_obj.find_sessions(org)
            print(f"\n  {org}:")
            for session in sessions:
                print(f"    - {session}")
        return

    # Validate arguments
    if not args.all and not args.org:
        parser.error("Must specify either --org or --all")

    # Process data
    if args.all:
        filter_obj.process_all()
    else:
        filter_obj.process_organization(args.org, session_filter=args.session)

    # Print summary
    filter_obj.print_summary()


if __name__ == "__main__":
    main()
