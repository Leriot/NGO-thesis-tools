#!/usr/bin/env python3
"""
Sitemap Discovery Tool

Automatically discovers sitemap.xml for organizations and adds URLs as seeds.
Run this before first scrape to ensure comprehensive coverage.
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sitemap_parser import SitemapParser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def add_sitemap_seeds(
    ngo_name: str,
    base_url: str,
    csv_path: Path = Path('config/url_seeds.csv'),
    min_priority: float = None,
    depth_limit: int = 5,
    dry_run: bool = False
):
    """
    Discover sitemap and add URLs to seed CSV

    Args:
        ngo_name: Name of organization
        base_url: Base URL of website
        csv_path: Path to url_seeds.csv
        min_priority: Minimum sitemap priority to include (0.0-1.0)
        depth_limit: Depth limit for discovered URLs
        dry_run: If True, show URLs without adding to CSV
    """
    parser = SitemapParser()

    # Discover and parse sitemap
    logger.info(f"Discovering sitemap for {ngo_name} at {base_url}")
    urls = parser.discover_and_parse(base_url)

    if not urls:
        logger.warning(f"No sitemap found or no URLs in sitemap for {base_url}")
        return

    logger.info(f"Found {len(urls)} URLs in sitemap")

    # Convert to seeds
    seeds = parser.urls_to_seeds(
        urls,
        url_type='sitemap',
        depth_limit=depth_limit,
        min_priority=min_priority
    )

    logger.info(f"Generated {len(seeds)} seed URLs (min_priority={min_priority})")

    # Show sample
    print("\nSample URLs (first 10):")
    for seed in seeds[:10]:
        priority_str = f"priority={seed.get('priority', 'N/A')}"
        lastmod_str = f"lastmod={seed.get('lastmod', 'N/A')}"
        print(f"  {seed['url']} ({priority_str}, {lastmod_str})")

    if len(seeds) > 10:
        print(f"  ... and {len(seeds) - 10} more")

    if dry_run:
        print("\n[DRY RUN] Would add these URLs to CSV")
        return

    # Add to CSV
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return

    # Backup CSV
    backup_path = csv_path.with_suffix('.csv.backup')
    import shutil
    shutil.copy2(csv_path, backup_path)
    logger.info(f"Created backup at {backup_path}")

    # Read existing seeds
    existing_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        existing_rows = list(reader)

    # Remove existing sitemap entries for this org
    filtered_rows = [
        row for row in existing_rows
        if not (row['ngo_name'] == ngo_name and row['url_type'] == 'sitemap')
    ]

    # Add new sitemap URLs
    new_rows = []
    for seed in seeds:
        new_rows.append({
            'ngo_name': ngo_name,
            'url_type': 'sitemap',
            'url': seed['url'],
            'depth_limit': str(depth_limit)
        })

    # Write back to file
    all_rows = filtered_rows + new_rows

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['ngo_name', 'url_type', 'url', 'depth_limit'])
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(f"✓ Added {len(new_rows)} sitemap URLs to {csv_path}")
    logger.info(f"  Removed {len(existing_rows) - len(filtered_rows)} old sitemap entries")

    print(f"\n✓ Sitemap discovery complete!")
    print(f"  Organization: {ngo_name}")
    print(f"  URLs added: {len(new_rows)}")
    print(f"  Depth limit: {depth_limit}")


def main():
    parser = argparse.ArgumentParser(
        description='Discover sitemap.xml and add URLs as seeds',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover sitemap for organization
  python scripts/discover_sitemap.py "Hnutí DUHA" https://www.hnutiduha.cz

  # Dry run to preview
  python scripts/discover_sitemap.py "Hnutí DUHA" https://www.hnutiduha.cz --dry-run

  # Only include high-priority URLs
  python scripts/discover_sitemap.py "Arnika" https://www.arnika.org --min-priority 0.8

  # Custom depth limit
  python scripts/discover_sitemap.py "Greenpeace ČR" https://www.greenpeace.org/czech --depth 3
"""
    )

    parser.add_argument(
        'ngo_name',
        help='Name of organization (must match ngo_list.csv)'
    )

    parser.add_argument(
        'base_url',
        help='Base URL of website (e.g., https://www.hnutiduha.cz)'
    )

    parser.add_argument(
        '--min-priority',
        type=float,
        help='Minimum sitemap priority to include (0.0-1.0)'
    )

    parser.add_argument(
        '--depth',
        type=int,
        default=5,
        help='Depth limit for discovered URLs (default: 5)'
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
        help='Show URLs without modifying CSV file'
    )

    args = parser.parse_args()

    add_sitemap_seeds(
        ngo_name=args.ngo_name,
        base_url=args.base_url,
        csv_path=args.csv_path,
        min_priority=args.min_priority,
        depth_limit=args.depth,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
