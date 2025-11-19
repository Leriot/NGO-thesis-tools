#!/usr/bin/env python3
"""
Interactive Menu for NGO Web Scraper

Provides a text-based UI for managing scraping sessions:
- Start new scraping sessions
- Resume interrupted sessions
- Check session status
- Delete old sessions
- Generate pagination seeds
- Run diagnostics
"""

import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime
import subprocess
import shutil

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.session_manager import SessionManager, SessionStatus


class ScraperMenu:
    """Interactive menu for scraper management"""

    def __init__(self):
        self.session_manager = SessionManager()
        self.running = True

    def clear_screen(self):
        """Clear terminal screen"""
        print("\033[2J\033[H", end="")

    def print_header(self):
        """Print menu header"""
        print("=" * 70)
        print("  NGO WEB SCRAPER - Management Console")
        print("=" * 70)
        print()

    def print_menu(self):
        """Print main menu options"""
        print("\nMAIN MENU:")
        print("  [1] Start New Scraping Session")
        print("  [2] Resume Previous Session")
        print("  [3] View Session Status")
        print("  [4] List All Sessions")
        print("  [5] Delete Session")
        print("  [6] Generate Pagination Seeds")
        print("  [7] Run Configuration Diagnostics")
        print("  [8] View Statistics")
        print("  [0] Exit")
        print()

    def get_input(self, prompt: str, default: Optional[str] = None) -> str:
        """Get user input with optional default"""
        if default:
            prompt = f"{prompt} [{default}]: "
        else:
            prompt = f"{prompt}: "

        value = input(prompt).strip()
        return value if value else (default or "")

    def get_choice(self, prompt: str, options: List[str]) -> str:
        """Get user choice from a list of options"""
        print(f"\n{prompt}")
        for i, option in enumerate(options, 1):
            print(f"  [{i}] {option}")
        print("  [0] Cancel")

        while True:
            try:
                choice = int(input("\nSelect option: "))
                if choice == 0:
                    return ""
                if 1 <= choice <= len(options):
                    return options[choice - 1]
                print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a number.")

    def confirm(self, prompt: str) -> bool:
        """Get yes/no confirmation"""
        while True:
            answer = input(f"{prompt} (y/n): ").lower().strip()
            if answer in ['y', 'yes']:
                return True
            if answer in ['n', 'no']:
                return False
            print("Please enter 'y' or 'n'")

    def start_new_session(self):
        """Start a new scraping session"""
        self.clear_screen()
        self.print_header()
        print("START NEW SCRAPING SESSION\n")

        # Get organization filter
        print("Which organization would you like to scrape?")
        print("  - Enter organization name (e.g., 'Hnutí DUHA')")
        print("  - Press ENTER to scrape all organizations")
        print()

        org = self.get_input("Organization name", "all")
        if org.lower() == "all":
            org = None

        # Get notes
        notes = self.get_input("Session notes (optional)", "")

        # Confirm
        print("\n" + "=" * 70)
        print("SESSION CONFIGURATION:")
        print(f"  Organization: {org or 'All'}")
        print(f"  Notes: {notes or 'None'}")
        print("=" * 70)

        if not self.confirm("\nStart scraping?"):
            print("Cancelled.")
            return

        # Create session
        print("\nCreating session...")
        session_id = self.session_manager.create_session(
            organization=org,
            notes=notes
        )

        session = self.session_manager.get_session(session_id)
        print(f"\nSession created: {session_id}")
        print(f"Output directory: {session['output_dir']}")

        # Build command
        cmd = ["python", "-m", "scripts.run_scraper"]
        if org:
            cmd.extend(["--organization", org])
        cmd.append("--session-id")
        cmd.append(session_id)

        print(f"\nStarting scraper...")
        print(f"Command: {' '.join(cmd)}\n")

        # Run scraper
        try:
            result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
            if result.returncode == 0:
                self.session_manager.update_session_status(
                    session_id,
                    SessionStatus.COMPLETED
                )
                print("\n✓ Scraping completed successfully!")
            else:
                self.session_manager.update_session_status(
                    session_id,
                    SessionStatus.FAILED
                )
                print("\n✗ Scraping failed!")
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user")
            self.session_manager.update_session_status(
                session_id,
                SessionStatus.INTERRUPTED
            )
        except Exception as e:
            print(f"\n✗ Error running scraper: {e}")
            self.session_manager.update_session_status(
                session_id,
                SessionStatus.FAILED
            )

        input("\nPress ENTER to continue...")

    def resume_session(self):
        """Resume a previous session"""
        self.clear_screen()
        self.print_header()
        print("RESUME PREVIOUS SESSION\n")

        # Get resumable sessions
        resumable = self.session_manager.get_resumable_sessions()

        if not resumable:
            print("No resumable sessions found.")
            input("\nPress ENTER to continue...")
            return

        # Display sessions
        print(f"Found {len(resumable)} resumable session(s):\n")
        for i, session in enumerate(resumable, 1):
            start_time = datetime.fromisoformat(session['start_time'])
            print(f"[{i}] {session['session_id']}")
            print(f"    Status: {session['status']}")
            print(f"    Organization: {session['organization'] or 'All'}")
            print(f"    Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"    Pages scraped: {session['total_pages_scraped']}")
            print()

        # Select session
        print("[0] Cancel")
        while True:
            try:
                choice = int(input("\nSelect session to resume: "))
                if choice == 0:
                    return
                if 1 <= choice <= len(resumable):
                    session = resumable[choice - 1]
                    break
                print("Invalid choice.")
            except ValueError:
                print("Please enter a number.")

        # Confirm
        print(f"\nResuming session: {session['session_id']}")
        if not self.confirm("Continue?"):
            print("Cancelled.")
            return

        # Build command
        cmd = [
            "python", "-m", "scripts.run_scraper",
            "--resume",
            "--session-id", session['session_id']
        ]

        print(f"\nResuming scraper...")
        print(f"Command: {' '.join(cmd)}\n")

        # Run scraper
        try:
            result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
            if result.returncode == 0:
                self.session_manager.update_session_status(
                    session['session_id'],
                    SessionStatus.COMPLETED
                )
                print("\n✓ Scraping completed successfully!")
            else:
                self.session_manager.update_session_status(
                    session['session_id'],
                    SessionStatus.FAILED
                )
                print("\n✗ Scraping failed!")
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user")
            self.session_manager.update_session_status(
                session['session_id'],
                SessionStatus.INTERRUPTED
            )
        except Exception as e:
            print(f"\n✗ Error running scraper: {e}")

        input("\nPress ENTER to continue...")

    def view_session_status(self):
        """View detailed status of a session"""
        self.clear_screen()
        self.print_header()
        print("VIEW SESSION STATUS\n")

        session_id = self.get_input("Enter session ID")
        if not session_id:
            return

        session = self.session_manager.get_session(session_id)
        if not session:
            print(f"\n✗ Session '{session_id}' not found")
            input("\nPress ENTER to continue...")
            return

        # Display session summary
        print("\n" + "=" * 70)
        print(self.session_manager.get_session_summary(session_id))
        print("=" * 70)

        # Get latest checkpoint
        checkpoint = self.session_manager.get_latest_checkpoint(session_id)
        if checkpoint:
            checkpoint_time = datetime.fromisoformat(checkpoint['timestamp'])
            print(f"\nLatest Checkpoint:")
            print(f"  Time: {checkpoint_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Pages scraped: {checkpoint['pages_scraped']}")
            print(f"  Queue size: {checkpoint['queue_size']}")

        input("\nPress ENTER to continue...")

    def list_all_sessions(self):
        """List all sessions"""
        self.clear_screen()
        self.print_header()
        print("ALL SESSIONS\n")

        # Get filter
        print("Filter by status:")
        print("  [1] All")
        print("  [2] In Progress")
        print("  [3] Completed")
        print("  [4] Failed")
        print("  [5] Interrupted")

        filter_choice = self.get_input("Select filter", "1")

        status_map = {
            "2": SessionStatus.IN_PROGRESS,
            "3": SessionStatus.COMPLETED,
            "4": SessionStatus.FAILED,
            "5": SessionStatus.INTERRUPTED
        }

        status_filter = status_map.get(filter_choice)

        # Get sessions
        sessions = self.session_manager.list_sessions(status=status_filter, limit=50)

        if not sessions:
            print("\nNo sessions found.")
            input("\nPress ENTER to continue...")
            return

        print(f"\nFound {len(sessions)} session(s):\n")
        for session in sessions:
            start_time = datetime.fromisoformat(session['start_time'])
            status_icon = {
                'completed': '✓',
                'failed': '✗',
                'interrupted': '⚠',
                'in_progress': '▶'
            }.get(session['status'], '?')

            print(f"{status_icon} {session['session_id']}")
            print(f"   Status: {session['status']}")
            print(f"   Organization: {session['organization'] or 'All'}")
            print(f"   Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Pages: {session['total_pages_scraped']} scraped, {session['total_pages_skipped']} skipped")
            print()

        input("\nPress ENTER to continue...")

    def delete_session(self):
        """Delete a session"""
        self.clear_screen()
        self.print_header()
        print("DELETE SESSION\n")

        session_id = self.get_input("Enter session ID to delete")
        if not session_id:
            return

        session = self.session_manager.get_session(session_id)
        if not session:
            print(f"\n✗ Session '{session_id}' not found")
            input("\nPress ENTER to continue...")
            return

        # Show session info
        print("\n" + self.session_manager.get_session_summary(session_id))

        # Confirm deletion
        print("\n⚠ WARNING: This action cannot be undone!")
        delete_files = self.confirm("Also delete output files?")

        if not self.confirm(f"Delete session '{session_id}'?"):
            print("Cancelled.")
            input("\nPress ENTER to continue...")
            return

        # Delete
        self.session_manager.delete_session(session_id, delete_files=delete_files)
        print(f"\n✓ Session '{session_id}' deleted")

        input("\nPress ENTER to continue...")

    def generate_pagination_seeds(self):
        """Generate pagination seed URLs"""
        self.clear_screen()
        self.print_header()
        print("GENERATE PAGINATION SEEDS\n")

        print("This tool helps generate seed URLs for paginated content.")
        print("Example: https://example.org/publikace?page=1 through page=47\n")

        base_url = self.get_input("Base URL (e.g., https://hnutiduha.cz/publikace)")
        if not base_url:
            return

        ngo_name = self.get_input("NGO name (must match url_seeds.csv)")
        if not ngo_name:
            return

        url_type = self.get_input("URL type", "publications")
        page_param = self.get_input("Page parameter name", "page")

        # Ask for max pages
        print("\nDetection options:")
        print("  [1] Auto-detect max pages")
        print("  [2] Specify manually")

        choice = self.get_input("Select option", "1")

        cmd = [
            "python", "scripts/generate_pagination_seeds.py",
            base_url,
            "--ngo-name", ngo_name,
            "--url-type", url_type,
            "--page-param", page_param
        ]

        if choice == "2":
            max_pages = self.get_input("Maximum page number")
            if max_pages:
                cmd.extend(["--max-pages", max_pages])

        # Dry run option
        if self.confirm("\nDry run (preview without saving)?"):
            cmd.append("--dry-run")

        print("\nRunning pagination generator...\n")

        try:
            subprocess.run(cmd, cwd=Path(__file__).parent.parent)
            print("\n✓ Done!")
        except Exception as e:
            print(f"\n✗ Error: {e}")

        input("\nPress ENTER to continue...")

    def run_diagnostics(self):
        """Run configuration diagnostics"""
        self.clear_screen()
        self.print_header()
        print("RUN CONFIGURATION DIAGNOSTICS\n")

        print("Running diagnostics tool...\n")

        try:
            subprocess.run(
                ["python", "scripts/check_config.py"],
                cwd=Path(__file__).parent.parent
            )
        except Exception as e:
            print(f"\n✗ Error running diagnostics: {e}")

        input("\nPress ENTER to continue...")

    def view_statistics(self):
        """View overall scraping statistics"""
        self.clear_screen()
        self.print_header()
        print("SCRAPING STATISTICS\n")

        all_sessions = self.session_manager.list_sessions(limit=1000)

        if not all_sessions:
            print("No sessions found.")
            input("\nPress ENTER to continue...")
            return

        # Calculate statistics
        total_sessions = len(all_sessions)
        completed = sum(1 for s in all_sessions if s['status'] == 'completed')
        failed = sum(1 for s in all_sessions if s['status'] == 'failed')
        in_progress = sum(1 for s in all_sessions if s['status'] == 'in_progress')
        interrupted = sum(1 for s in all_sessions if s['status'] == 'interrupted')

        total_pages = sum(s['total_pages_scraped'] for s in all_sessions)
        total_errors = sum(s['total_errors'] for s in all_sessions)

        print("=" * 70)
        print("OVERALL STATISTICS")
        print("=" * 70)
        print(f"\nSessions:")
        print(f"  Total: {total_sessions}")
        print(f"  Completed: {completed}")
        print(f"  Failed: {failed}")
        print(f"  In Progress: {in_progress}")
        print(f"  Interrupted: {interrupted}")
        print(f"\nPages:")
        print(f"  Total scraped: {total_pages}")
        print(f"  Total errors: {total_errors}")

        # Organization breakdown
        orgs = {}
        for session in all_sessions:
            org = session['organization'] or 'All'
            if org not in orgs:
                orgs[org] = {'count': 0, 'pages': 0}
            orgs[org]['count'] += 1
            orgs[org]['pages'] += session['total_pages_scraped']

        print(f"\nBy Organization:")
        for org, stats in sorted(orgs.items()):
            print(f"  {org}: {stats['count']} sessions, {stats['pages']} pages")

        print("=" * 70)

        input("\nPress ENTER to continue...")

    def run(self):
        """Main menu loop"""
        while self.running:
            self.clear_screen()
            self.print_header()
            self.print_menu()

            choice = self.get_input("Select option")

            if choice == "1":
                self.start_new_session()
            elif choice == "2":
                self.resume_session()
            elif choice == "3":
                self.view_session_status()
            elif choice == "4":
                self.list_all_sessions()
            elif choice == "5":
                self.delete_session()
            elif choice == "6":
                self.generate_pagination_seeds()
            elif choice == "7":
                self.run_diagnostics()
            elif choice == "8":
                self.view_statistics()
            elif choice == "0":
                if self.confirm("\nExit scraper menu?"):
                    self.running = False
            else:
                print("\n✗ Invalid option. Please try again.")
                input("\nPress ENTER to continue...")

        print("\nGoodbye!\n")


def main():
    menu = ScraperMenu()
    try:
        menu.run()
    except KeyboardInterrupt:
        print("\n\nInterrupted. Goodbye!\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
