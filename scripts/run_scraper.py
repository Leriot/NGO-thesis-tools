#!/usr/bin/env python3
"""
Main Scraper Runner Script

Coordinates scraping sessions with session management, checkpointing,
and per-organization filtering.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scraper import NGOScraper
from src.session_manager import SessionManager, SessionStatus


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_scraping_session(
    organization: Optional[str] = None,
    session_id: Optional[str] = None,
    resume: bool = False,
    config_path: str = "config/scraping_rules.yaml",
    ngo_list_file: str = "config/ngo_list.csv",
    url_seeds_file: str = "config/url_seeds.csv"
):
    """
    Run a scraping session with session management

    Args:
        organization: Organization name to filter (None = all)
        session_id: Existing session ID to continue
        resume: Whether to resume from checkpoint
        config_path: Path to scraping config
        ngo_list_file: Path to NGO list CSV
        url_seeds_file: Path to URL seeds CSV
    """
    session_manager = SessionManager()

    # Handle session creation or resumption
    if session_id:
        # Use existing session
        session = session_manager.get_session(session_id)
        if not session:
            logger.error(f"Session '{session_id}' not found")
            sys.exit(1)

        logger.info(f"Using session: {session_id}")

        # Mark as in progress if resuming
        if resume:
            session_manager.update_session_status(
                session_id,
                SessionStatus.IN_PROGRESS
            )

        output_dir = Path(session['output_dir'])

    else:
        # Create new session
        logger.info("Creating new scraping session...")

        session_id = session_manager.create_session(
            organization=organization,
            notes=f"Scraping {organization or 'all organizations'}"
        )

        session = session_manager.get_session(session_id)
        output_dir = Path(session['output_dir'])

        logger.info(f"Session created: {session_id}")
        logger.info(f"Output directory: {output_dir}")

    # Initialize scraper
    try:
        logger.info("Initializing scraper...")
        scraper = NGOScraper(config_path=config_path)

        # Update progress file to use session-specific directory
        scraper.progress_file = output_dir / "checkpoint.json"

        # Prepare organization filter
        ngo_filter = [organization] if organization else None

        # Run scraping
        logger.info("=" * 80)
        if organization:
            logger.info(f"Starting scrape for: {organization}")
        else:
            logger.info("Starting scrape for: ALL ORGANIZATIONS")
        logger.info("=" * 80)

        stats = scraper.scrape_from_config(
            ngo_list_file=ngo_list_file,
            url_seeds_file=url_seeds_file,
            ngo_filter=ngo_filter,
            resume=resume
        )

        # Update session with final stats
        session_manager.update_session_status(
            session_id,
            SessionStatus.COMPLETED,
            stats={
                'total_pages_scraped': stats.get('successful_requests', 0),
                'total_errors': stats.get('failed_requests', 0)
            }
        )

        logger.info("=" * 80)
        logger.info("✓ Scraping completed successfully!")
        logger.info(f"Session: {session_id}")
        logger.info(f"Output: {output_dir}")
        logger.info("=" * 80)

        return 0

    except KeyboardInterrupt:
        logger.warning("\n\n⚠ Scraping interrupted by user")
        session_manager.update_session_status(
            session_id,
            SessionStatus.INTERRUPTED
        )
        return 1

    except Exception as e:
        logger.error(f"✗ Scraping failed: {e}", exc_info=True)
        session_manager.update_session_status(
            session_id,
            SessionStatus.FAILED
        )
        return 1


def main():
    parser = argparse.ArgumentParser(
        description='Run NGO web scraper with session management',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all organizations
  python scripts/run_scraper.py

  # Scrape specific organization
  python scripts/run_scraper.py --organization "Hnutí DUHA"

  # Resume previous session
  python scripts/run_scraper.py --resume --session-id 20231119_143022_Hnuti_DUHA

  # Start new session for specific organization
  python scripts/run_scraper.py --organization "Arnika"
"""
    )

    parser.add_argument(
        '--organization',
        '-o',
        help='Scrape only this organization (e.g., "Hnutí DUHA")'
    )

    parser.add_argument(
        '--session-id',
        '-s',
        help='Session ID to continue (for resume or tracking)'
    )

    parser.add_argument(
        '--resume',
        '-r',
        action='store_true',
        help='Resume from previous checkpoint'
    )

    parser.add_argument(
        '--config',
        '-c',
        default='config/scraping_rules.yaml',
        help='Path to scraping configuration (default: config/scraping_rules.yaml)'
    )

    parser.add_argument(
        '--ngo-list',
        default='config/ngo_list.csv',
        help='Path to NGO list CSV (default: config/ngo_list.csv)'
    )

    parser.add_argument(
        '--url-seeds',
        default='config/url_seeds.csv',
        help='Path to URL seeds CSV (default: config/url_seeds.csv)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.resume and not args.session_id:
        # Try to auto-detect last session for this organization
        session_manager = SessionManager()
        resumable = session_manager.get_resumable_sessions()

        if args.organization:
            resumable = [s for s in resumable if s['organization'] == args.organization]

        if not resumable:
            logger.error("No resumable sessions found. Please specify --session-id")
            sys.exit(1)

        # Use most recent
        args.session_id = resumable[0]['session_id']
        logger.info(f"Auto-detected session: {args.session_id}")

    # Run scraping
    exit_code = run_scraping_session(
        organization=args.organization,
        session_id=args.session_id,
        resume=args.resume,
        config_path=args.config,
        ngo_list_file=args.ngo_list,
        url_seeds_file=args.url_seeds
    )

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
