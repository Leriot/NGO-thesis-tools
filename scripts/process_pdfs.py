#!/usr/bin/env python3
"""
PDF Text Extractor - Data Cleanup Tool #1

Purpose:
    Extracts text from scraped PDFs, cleans formatting artifacts, and quarantines
    image-based PDFs that require OCR processing.

What it does:
    1. Extracts text from PDFs using pdfplumber (preserves table layout)
    2. Removes repeated headers/footers (lines appearing on >50% of pages)
    3. Collapses excessive whitespace (3+ newlines → 2)
    4. Quality check: PDFs with <100 chars are likely scanned images
    5. Successful extractions → .txt files in 'extracted_text' folder
    6. Failed extractions → PDFs moved to 'needs_ocr' quarantine folder

Usage:
    # Process all PDFs for an organization
    python scripts/process_pdfs.py --org "Hnutí DUHA"

    # Process specific scrape session
    python scripts/process_pdfs.py --org "Hnutí DUHA" --session "20240115_103000"

    # Process all organizations
    python scripts/process_pdfs.py --all

    # Custom threshold for OCR detection
    python scripts/process_pdfs.py --org "Arnika" --min-chars 200

Author: Thesis Research - NGO Network Analysis
"""

import os
import sys
import shutil
import argparse
import pdfplumber
import re
from pathlib import Path
from collections import Counter
from datetime import datetime

# Default Configuration
DEFAULT_MIN_TEXT_LENGTH = 100  # Characters threshold for OCR detection
DEFAULT_HEADER_THRESHOLD = 0.5  # Lines appearing on >50% of pages are headers


class PDFProcessor:
    """Processes PDFs from scraped data, extracting text and handling image-based PDFs."""

    def __init__(self, data_root="data", min_text_length=DEFAULT_MIN_TEXT_LENGTH):
        self.data_root = Path(data_root)
        self.min_text_length = min_text_length
        self.stats = {
            'processed': 0,
            'success': 0,
            'quarantined': 0,
            'errors': 0
        }

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

    def extract_text_from_pdf(self, pdf_path):
        """
        Extract text from a PDF with cleaning.

        Returns:
            str or None: Extracted and cleaned text, or None if extraction failed
        """
        full_text = []
        page_headers = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extract text from all pages
                for page in pdf.pages:
                    # layout=True preserves table structures but adds whitespace
                    text = page.extract_text(layout=True)
                    if text:
                        full_text.append(text)
                        # Capture first line for header detection
                        lines = text.strip().split('\n')
                        if lines:
                            page_headers.append(lines[0].strip())

                if not full_text:
                    return None

                # --- STEP 1: Header/Footer Removal ---
                # If a line appears on >50% of pages, it's likely a running header
                header_counts = Counter(page_headers)
                threshold = len(pdf.pages) * DEFAULT_HEADER_THRESHOLD
                junk_lines = {line for line, count in header_counts.items()
                             if count > threshold and line}

                cleaned_pages = []
                for page_text in full_text:
                    lines = page_text.split('\n')
                    # Filter out junk lines
                    clean_lines = [l for l in lines if l.strip() not in junk_lines]
                    cleaned_pages.append("\n".join(clean_lines))

                final_text = "\n".join(cleaned_pages)

                # --- STEP 2: Whitespace Collapse ---
                # Fix "Huge Gaps" issue - replace 3+ newlines with 2 (paragraph break)
                final_text = re.sub(r'\n{3,}', '\n\n', final_text)

                return final_text

        except Exception as e:
            print(f"  ✗ Error reading PDF: {e}")
            return None

    def process_pdf(self, pdf_path, output_dir, quarantine_dir):
        """
        Process a single PDF file.

        Returns:
            str: Status - 'success', 'quarantine', or 'error'
        """
        filename = pdf_path.name

        # Extract text
        text = self.extract_text_from_pdf(pdf_path)

        # Check quality
        if text and len(text.strip()) > self.min_text_length:
            # SUCCESS: Save as text file
            txt_filename = pdf_path.stem + ".txt"
            output_path = output_dir / txt_filename

            # Add source reference at top
            final_output = f"SOURCE_FILE: {filename}\n"
            final_output += f"EXTRACTED: {datetime.now().isoformat()}\n"
            final_output += f"TEXT_LENGTH: {len(text)} characters\n\n"
            final_output += text

            try:
                output_path.write_text(final_output, encoding='utf-8')
                print(f"  ✓ Extracted: {filename} ({len(text)} chars)")
                return 'success'
            except Exception as e:
                print(f"  ✗ Failed to save {filename}: {e}")
                return 'error'
        else:
            # QUARANTINE: Likely image-based, needs OCR
            text_len = len(text.strip()) if text else 0
            print(f"  ⚠ Quarantine: {filename} ({text_len} chars < {self.min_text_length})")

            try:
                dest_path = quarantine_dir / filename
                shutil.copy2(pdf_path, dest_path)
                return 'quarantine'
            except Exception as e:
                print(f"  ✗ Failed to quarantine {filename}: {e}")
                return 'error'

    def process_session(self, org_name, session_name):
        """Process all PDFs in a specific scrape session."""
        # Input paths
        session_path = self.data_root / "raw" / org_name / session_name
        documents_path = session_path / "documents"

        if not documents_path.exists():
            print(f"  No documents folder found: {documents_path}")
            return

        # Output paths
        output_base = self.data_root / "processed" / org_name / session_name
        extracted_dir = output_base / "extracted_text"
        quarantine_dir = output_base / "needs_ocr"

        extracted_dir.mkdir(parents=True, exist_ok=True)
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        # Find all PDFs
        pdf_files = list(documents_path.glob("*.pdf"))

        if not pdf_files:
            print(f"  No PDF files found in {documents_path}")
            return

        print(f"\n📄 Processing {len(pdf_files)} PDFs from {session_name}")
        print(f"   Output: {extracted_dir}")

        # Process each PDF
        for pdf_path in pdf_files:
            result = self.process_pdf(pdf_path, extracted_dir, quarantine_dir)
            self.stats['processed'] += 1

            if result == 'success':
                self.stats['success'] += 1
            elif result == 'quarantine':
                self.stats['quarantined'] += 1
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
        print(f"Total PDFs processed:     {self.stats['processed']}")
        print(f"  ✓ Successfully extracted: {self.stats['success']}")
        print(f"  ⚠ Quarantined (needs OCR): {self.stats['quarantined']}")
        print(f"  ✗ Errors:                 {self.stats['errors']}")

        if self.stats['quarantined'] > 0:
            print(f"\n💡 Tip: Files in 'needs_ocr' folders are likely scanned images.")
            print(f"   Use an OCR tool (e.g., Tesseract, AWS Textract, LLM vision) to extract text.")


def main():
    parser = argparse.ArgumentParser(
        description="Extract text from scraped PDFs and quarantine image-based PDFs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all PDFs for one organization
  python scripts/process_pdfs.py --org "Hnutí DUHA"

  # Process specific scrape session
  python scripts/process_pdfs.py --org "Hnutí DUHA" --session "20240115_103000"

  # Process all organizations
  python scripts/process_pdfs.py --all

  # Stricter OCR detection (require 200 chars minimum)
  python scripts/process_pdfs.py --org "Arnika" --min-chars 200
        """
    )

    parser.add_argument('--org', type=str, help='Organization name to process')
    parser.add_argument('--session', type=str, help='Specific session timestamp to process')
    parser.add_argument('--all', action='store_true', help='Process all organizations')
    parser.add_argument('--data-root', type=str, default='data',
                       help='Root data directory (default: data)')
    parser.add_argument('--min-chars', type=int, default=DEFAULT_MIN_TEXT_LENGTH,
                       help=f'Minimum characters to avoid quarantine (default: {DEFAULT_MIN_TEXT_LENGTH})')
    parser.add_argument('--list', action='store_true',
                       help='List available organizations and sessions')

    args = parser.parse_args()

    # Initialize processor
    processor = PDFProcessor(data_root=args.data_root, min_text_length=args.min_chars)

    # Handle --list command
    if args.list:
        orgs = processor.find_organizations()
        if not orgs:
            print("No organizations found in data/raw/")
            return

        print("\n📁 Available Organizations and Sessions:")
        for org in orgs:
            sessions = processor.find_sessions(org)
            print(f"\n  {org}:")
            for session in sessions:
                print(f"    - {session}")
        return

    # Validate arguments
    if not args.all and not args.org:
        parser.error("Must specify either --org or --all")

    # Process data
    if args.all:
        processor.process_all()
    else:
        processor.process_organization(args.org, session_filter=args.session)

    # Print summary
    processor.print_summary()


if __name__ == "__main__":
    main()
