# Data Cleanup Tools

This repository includes two Python scripts for processing and filtering scraped data. These tools help prepare raw scraped content for LLM-based network analysis.

## Overview

```
Raw Data → Script 1: PDF Extraction → Script 2: Content Filtering → Clean Data for Analysis
```

1. **`process_pdfs.py`** - Extracts text from PDFs, quarantines image-based scans
2. **`filter_content.py`** - Cleans HTML, deduplicates, filters for relevance

---

## Script 1: `process_pdfs.py` - PDF Text Extraction

### Purpose
Extracts readable text from scraped PDFs and identifies scanned documents that require OCR processing.

### What It Does

#### 1. Text Extraction
- Uses `pdfplumber` library with layout preservation (maintains table structures)
- Extracts text from all pages in each PDF

#### 2. Cleaning Operations
- **Header/Footer Removal**: Detects lines appearing on >50% of pages (e.g., "Page 5 of 20") and removes them
- **Whitespace Collapse**: Reduces 3+ consecutive newlines to 2 (preserves paragraph breaks)

#### 3. Quality Check
- Counts total characters in extracted text
- If text length < 100 characters → likely a scanned image/photo-based PDF
- These are moved to a "quarantine" folder for later OCR processing

#### 4. Output
- **Successful extractions** → saved as `.txt` files in `data/processed/{org}/{session}/extracted_text/`
- **Failed extractions** → original PDFs copied to `data/processed/{org}/{session}/needs_ocr/`

### Usage

```bash
# List available organizations and sessions
python scripts/process_pdfs.py --list

# Process all PDFs for one organization (all sessions)
python scripts/process_pdfs.py --org "Hnutí DUHA"

# Process specific scrape session
python scripts/process_pdfs.py --org "Hnutí DUHA" --session "20240115_103000"

# Process all organizations
python scripts/process_pdfs.py --all

# Custom threshold (require 200 chars minimum before quarantine)
python scripts/process_pdfs.py --org "Arnika" --min-chars 200
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--org` | Organization name to process | Required (unless --all) |
| `--session` | Specific session timestamp | All sessions |
| `--all` | Process all organizations | False |
| `--min-chars` | Minimum characters to avoid quarantine | 100 |
| `--data-root` | Root data directory | `data` |
| `--list` | List available orgs/sessions | - |

### Output Structure

```
data/processed/{org}/{session}/
├── extracted_text/          # Successfully extracted PDFs
│   ├── document1.txt
│   ├── document2.txt
│   └── ...
└── needs_ocr/               # Scanned PDFs requiring OCR
    ├── scan1.pdf
    └── scan2.pdf
```

Each `.txt` file includes metadata header:
```
SOURCE_FILE: report_2023.pdf
EXTRACTED: 2024-01-15T10:30:00
TEXT_LENGTH: 4521 characters

[actual text content here...]
```

### Known Limitations

1. **Header detection**: Only checks first line of each page (won't catch multi-line headers)
2. **Footer detection**: Not specifically handled (only headers are detected)
3. **OCR not included**: Scanned PDFs are identified but not automatically OCR'd

### Dependencies

```bash
pip install pdfplumber
```

---

## Script 2: `filter_content.py` - Content Cleaning & Filtering

### Purpose
Cleans HTML content, removes boilerplate and duplicates, and filters documents for relevance to NGO network analysis using Czech social science keywords.

### What It Does

#### 1. HTML Cleaning (Boilerplate Removal)

Removes non-content elements:
- **Tag removal**: `<script>`, `<style>`, `<noscript>`, `<iframe>`, `<svg>`, `<form>`, `<button>`, `<input>`
- **Class/ID heuristics**: Elements with classes/IDs like `nav`, `footer`, `sidebar`, `menu`, `cookie`, `ads`
- **Role attributes**: Elements with `role="navigation"`, `role="banner"`, etc.
- **Link density**: Blocks where >60% of text is links (indicates navigation menus)

Protects content elements with safe terms: `article`, `content`, `main`, `body`

#### 2. Deduplication

Uses **shingling** + **Jaccard similarity**:
- Creates 3-word sequences (shingles) from each document
- Compares documents using Jaccard index: `intersection / union`
- If similarity ≥ 0.85 (85%) → marked as duplicate
- Duplicates are grouped by the first (master) occurrence

**Example:**
```
Doc A: "the cat sat on the mat" → shingles: {(the,cat,sat), (cat,sat,on), (sat,on,the), ...}
Doc B: "the cat sat on the rug" → shingles: {(the,cat,sat), (cat,sat,on), (sat,on,the), ...}
Jaccard = 0.8 → Not duplicate (< 0.85)
```

#### 3. Relevance Scoring

Uses **Czech keywords** with weights based on social science theory:

| Category | Keywords (roots) | Weight | Rationale |
|----------|------------------|--------|-----------|
| **Relations** | spoluprác, partner, alianc, koalic | 3 | Core network ties |
| | člen, platform, síť | 2 | Membership/structure |
| **Funding** | grant, financ, dar, nadac, fond, dotac | 2-3 | Resource dependency |
| **Policy Actors** | ministerstv | 3 | High-level influence |
| | parlament, senát | 2 | Legislative ties |
| | vlád, výbor, zákon | 1 | Common terms (low weight) |
| **Actions** | podpor, organiz, projekt | 1 | Context indicators |
| **Topics** | klimat, uhlí, strategi | 1 | Domain context |

**How scoring works:**
1. Search for keyword roots (e.g., "spoluprác" matches "spolupráce", "spolupracovat", "spolupracující")
2. Count occurrences: `spolupráce(5×)` = 5 occurrences
3. Calculate score: `5 × weight(3) = 15 points`
4. Sum all keyword scores → **Raw Score**

#### 4. Density Filtering (Length Penalty)

To prevent long documents with few keywords from passing:

- **Density Score** = `(Raw Score / Word Count) × 100`
- Example: 10 points in 1000 words = 1.0 density

**Why this matters:**
- Document A: 500 words, 10 points → Density = 2.0 ✓
- Document B: 5000 words, 10 points → Density = 0.2 ✗

Document B is probably a general news article with incidental keyword mentions.

#### 5. Decision Logic

A document is **RELEVANT** if it passes BOTH filters:
```
KEEP if: (Raw Score ≥ 5) AND (Density ≥ 0.5)
```

Default thresholds:
- **Minimum Raw Score**: 5 points
- **Minimum Density**: 0.5 points per 100 words

#### 6. Output

Documents are sorted into three folders:

```
data/processed/{org}/{session}/
├── relevant/           # Passes both filters
│   ├── page1.txt
│   └── page2.txt
├── irrelevant/         # Fails one or both filters
│   ├── page3.txt
│   └── page4.txt
└── duplicates/         # Grouped by master document
    ├── page1/          # Master: page1.html
    │   ├── page5.txt   # Duplicate of page1
    │   └── page6.txt
    └── page2/
        └── page7.txt
```

Each file includes metadata header:
```
SOURCE_FILE: page1.html
PROCESSED: 2024-01-15T10:30:00
RELEVANCE_RAW_SCORE: 15
RELEVANCE_DENSITY: 2.34
WORD_COUNT: 641
KEYWORDS_FOUND: spoluprác(3×w3), partner(2×w3), ministerstv(1×w3)

================================================================================

[actual cleaned text here...]
```

### Usage

```bash
# List available organizations and sessions
python scripts/filter_content.py --list

# Process all HTML files for one organization (all sessions)
python scripts/filter_content.py --org "Hnutí DUHA"

# Process specific scrape session
python scripts/filter_content.py --org "Hnutí DUHA" --session "20240115_103000"

# Process all organizations
python scripts/filter_content.py --all

# Stricter filtering (more selective)
python scripts/filter_content.py --org "Arnika" --min-score 10 --min-density 1.0

# More lenient filtering (keep more documents)
python scripts/filter_content.py --org "Greenpeace ČR" --min-score 3 --min-density 0.3

# Adjust deduplication sensitivity
python scripts/filter_content.py --org "Arnika" --similarity 0.90  # More strict (fewer dupes)
python scripts/filter_content.py --org "Arnika" --similarity 0.75  # More lenient (more dupes)
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--org` | Organization name to process | Required (unless --all) |
| `--session` | Specific session timestamp | All sessions |
| `--all` | Process all organizations | False |
| `--min-score` | Minimum raw relevance score | 5 |
| `--min-density` | Minimum density per 100 words | 0.5 |
| `--similarity` | Jaccard threshold for duplicates | 0.85 |
| `--data-root` | Root data directory | `data` |
| `--list` | List available orgs/sessions | - |

### Tuning Recommendations

#### If keeping too many irrelevant documents:
```bash
# Increase thresholds
python scripts/filter_content.py --org "..." --min-score 8 --min-density 1.0
```

#### If losing too many relevant documents:
```bash
# Decrease thresholds
python scripts/filter_content.py --org "..." --min-score 3 --min-density 0.3
```

#### If too many duplicates are kept:
```bash
# Lower similarity threshold (more aggressive deduplication)
python scripts/filter_content.py --org "..." --similarity 0.75
```

#### If too many unique documents marked as duplicates:
```bash
# Raise similarity threshold (less aggressive)
python scripts/filter_content.py --org "..." --similarity 0.90
```

### Customizing Keywords

**Keywords are now configurable!** Edit `config/content_filter_keywords.yaml` to customize filtering:

```yaml
keywords:
  relations:
    - root: "spoluprác"
      weight: 3
      variations: ["spolupráce", "spolupracovat", "spolupracující", "spolupracoval"]
      description: "Cooperation, collaboration"

    - root: "partner"
      weight: 3
      variations: ["partner", "partneři", "partnerství", "partnerský", "partnera"]
      description: "Partners, partnerships"

  funding:
    - root: "grant"
      weight: 3
      variations: ["grant", "grantu", "grantů", "grantový"]
      description: "Grant"
```

**How keyword matching works (improved in v2):**

1. **Root matching**: Searches for the keyword root (e.g., "spoluprác")
2. **Variation matching**: Uses regex with word boundaries to match exact variations (e.g., `\bspolupráce\b`)
3. **Takes maximum**: Uses max(root_count, variation_count) to avoid double-counting
4. **Scores**: `count × weight`

**Example:**
```
Text: "Naše spolupráce s partnery zahrnuje 5 partnerů."

spoluprác: root_count=1, variations=['spolupráce']=1 → count=1, score=1×3=3
partner:   root_count=2, variations=['partnery','partnerů']=2 → count=2, score=2×3=6

Total Raw Score: 9
Word Count: 7
Density: (9/7)×100 = 128.57 points per 100 words → PASS ✓
```

**Adding new keywords:**

```yaml
- root: "kooperac"
  weight: 3
  variations: ["kooperace", "kooperační", "kooperovat"]
  description: "Cooperation (alternative term)"
```

**Adjusting weights:**
- Weight 1: Common terms (context indicators)
- Weight 2: Medium importance (structural terms)
- Weight 3: High importance (core network concepts)

**Adjusting default thresholds:**

```yaml
filtering:
  min_raw_score: 5       # Change to 3 for more lenient, 10 for stricter
  min_density_score: 0.5 # Change to 0.3 for more lenient, 1.0 for stricter
  similarity_threshold: 0.85
```

**Using custom config:**

```bash
# Create custom config
cp config/content_filter_keywords.yaml config/my_keywords.yaml

# Edit your keywords
nano config/my_keywords.yaml

# Use custom config
python scripts/filter_content.py --org "..." --config config/my_keywords.yaml
```

### Dependencies

```bash
pip install beautifulsoup4 pyyaml
```

---

## Installation

Both scripts require additional dependencies:

```bash
pip install pdfplumber beautifulsoup4 pyyaml
```

Note: `beautifulsoup4` and `pyyaml` are already in requirements.txt. Only `pdfplumber` needs to be added:

```bash
# Or install from requirements.txt
pip install -r requirements.txt
```

---

## Workflow Example

Complete data cleanup pipeline:

```bash
# 1. List available data
python scripts/process_pdfs.py --list
python scripts/filter_content.py --list

# 2. Process PDFs for specific organization
python scripts/process_pdfs.py --org "Hnutí DUHA"

# 3. Filter HTML content for same organization
python scripts/filter_content.py --org "Hnutí DUHA"

# 4. Review results
ls -lh data/processed/Hnutí\ DUHA/*/extracted_text/
ls -lh data/processed/Hnutí\ DUHA/*/relevant/

# 5. Process everything
python scripts/process_pdfs.py --all
python scripts/filter_content.py --all
```

---

## Output for Analysis

After running both scripts, your analysis-ready data will be in:

```
data/processed/{org}/{session}/
├── extracted_text/     # Cleaned PDF text (from script 1)
├── relevant/           # Filtered HTML content (from script 2)
├── irrelevant/         # Filtered out content
├── duplicates/         # Duplicate pages
└── needs_ocr/          # Scanned PDFs requiring OCR
```

**For LLM network analysis**, focus on:
- `extracted_text/` - Reports, publications, policy documents
- `relevant/` - Web pages discussing partnerships, funding, policy actors

---

## Troubleshooting

### Script 1: PDF Processing

**Problem**: All PDFs going to quarantine
```bash
# Solution: Lower the character threshold
python scripts/process_pdfs.py --org "..." --min-chars 50
```

**Problem**: Error "pdfplumber not found"
```bash
pip install pdfplumber
```

**Problem**: Encoding errors in output
- Script uses UTF-8 with `errors='ignore'` - should handle Czech characters
- Check system locale: `locale` (should include UTF-8)

### Script 2: Content Filtering

**Problem**: No relevant documents found
```bash
# Solution: Lower filtering thresholds
python scripts/filter_content.py --org "..." --min-score 3 --min-density 0.25
```

**Problem**: Everything marked as irrelevant
- Check if Czech keywords are appropriate for your data
- Try `--min-score 1 --min-density 0.1` to see what passes
- Review keywords in script (lines 69-102)

**Problem**: Error "beautifulsoup4 not found"
```bash
pip install beautifulsoup4
```

**Problem**: Too many duplicates
- Websites often have template pages with similar content
- This is expected behavior - review `duplicates/` folder to verify
- Adjust with `--similarity 0.90` if needed

---

## Code Quality Notes

### Script 1 (`process_pdfs.py`)
✅ **Strengths**:
- Clean separation of concerns (extraction, cleaning, quality check)
- Good error handling with try-except blocks
- Preserves original PDFs in quarantine (doesn't delete)
- Metadata tracking in output

⚠️ **Limitations**:
- Header detection only checks first line (misses multi-line headers)
- No footer detection
- Doesn't attempt OCR (by design - requires external tool)

### Script 2 (`filter_content.py`)
✅ **Strengths**:
- Sophisticated deduplication using shingling
- Dual-criteria filtering (raw + density) prevents false positives
- Good balance of safe vs. junk element detection
- Link density check removes navigation blocks
- Extensive keyword dictionary with theory-based weights

⚠️ **Limitations**:
- No stemming (relies on root matching like "spoluprác" to catch variations)
- Keywords hardcoded (not in config file)
- Reprocesses all files each run (no incremental processing)

---

## Citation

If you use these scripts in your research, please cite:

```
Leriot. (2024). Data Cleanup Tools for NGO Network Analysis.
Web Scraper for Thesis Research, Masaryk University.
https://github.com/Leriot/leriot-web-scraper-for-thesis
```

---

## Contact

- Repository: https://github.com/Leriot/leriot-web-scraper-for-thesis
- Issues: Use GitHub Issues for bug reports or feature requests
