# Academic Web Scraper for NGO Network Analysis

A robust, ethically-compliant web scraper designed for academic research on Czech climate policy NGOs. This tool supports network analysis by systematically collecting web content, hyperlinks, and documents from NGO websites.

## Project Overview

This scraper was developed for thesis research comparing LLM-extracted organizational networks with survey-based networks (COMPON project). It focuses on 20 Czech climate NGOs and emphasizes:

- **Academic integrity** - Respects robots.txt and rate limits
- **Robustness** - Handles failures gracefully with automatic retries
- **Reproducibility** - Comprehensive logging and checkpoint system
- **Flexibility** - Easy configuration for different NGOs and scraping strategies

## Features

### Core Capabilities
- ✅ Respects `robots.txt` and crawl delays
- ✅ Configurable rate limiting (default: 2 seconds between requests)
- ✅ URL deduplication and normalization
- ✅ Automatic content-type detection
- ✅ Session resumption for interrupted scrapes
- ✅ Comprehensive error handling and logging
- ✅ Progress tracking with statistics
- ✅ Link extraction and classification (internal/external)
- ✅ Document download (PDFs, DOCs, etc.)
- ✅ Metadata extraction from HTML pages

### Data Collection
1. **Website Structure**
   - All internal hyperlinks
   - External links for shared resource analysis
   - Site structure mapping

2. **Content Pages**
   - Publications and reports (especially PDFs)
   - Press releases
   - News articles
   - Event announcements
   - Policy statements

3. **Metadata**
   - Team/About pages for personnel overlap
   - Publication dates
   - Document metadata

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd ngo-network-scraper
```

2. **Create a virtual environment (recommended)**
```bash
python -m venv venv

# On Linux/Mac:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

## Configuration

The scraper uses three configuration files in the `config/` directory:

### 1. NGO List (`config/ngo_list.csv`)

Defines the NGOs to scrape with their basic information:

```csv
canonical_name,aliases,website_domain,scrape_priority
Hnutí DUHA,"Friends of the Earth Czech Republic;DUHA",hnutiduha.cz,1
Arnika,Arnika - Citizens Support Association,arnika.org,1
```

**Fields:**
- `canonical_name`: Official name used for storage
- `aliases`: Alternative names (semicolon-separated)
- `website_domain`: Domain to scrape
- `scrape_priority`: Priority level (1=high, 2=medium, 3=low)

### 2. URL Seeds (`config/url_seeds.csv`)

Specifies starting URLs for each NGO:

```csv
ngo_name,url_type,url,depth_limit
Hnutí DUHA,homepage,https://www.hnutiduha.cz,3
Hnutí DUHA,publications,https://www.hnutiduha.cz/publikace,2
```

**Fields:**
- `ngo_name`: Must match `canonical_name` from NGO list
- `url_type`: Description (homepage, publications, news, etc.)
- `url`: Full URL to start crawling from
- `depth_limit`: Maximum crawl depth from this URL

### 3. Scraping Rules (`config/scraping_rules.yaml`)

Main configuration for scraping behavior:

```yaml
rate_limiting:
  requests_per_minute: 30
  delay_between_requests: 2.0  # seconds

crawl:
  max_depth: 3
  max_pages_per_site: 500
  respect_robots_txt: true

content_types:
  - text/html
  - application/pdf
  - application/msword
```

See the full file for all available options.

## Usage

### Interactive Menu (Recommended for Beginners)

The easiest way to use this scraper is through the interactive text-based UI:

```bash
python scripts/scraper_menu.py
```

This provides a full-featured menu interface with the following options:

**Main Menu:**
1. **Run Scraper** - Start a new scraping session with preview
2. **View Recent Scraping Sessions** - See scraping history
3. **Resume Interrupted Session** - Continue a stopped scrape
4. **Test URL** - Preview what will be scraped from a URL
5. **Configuration** - View/edit current settings

**Organization Management:**
6. **View Organizations & History** - See all NGOs with:
   - Number of seed URLs
   - Total sessions and pages scraped
   - Last scrape date
   - Completion status
7. **Manage Seed URLs** - View and add seed URLs for organizations
8. **Add New Organization** - Wizard to add a new NGO to the system

**Utilities:**
9. **Generate Pagination Seeds** - Auto-generate URLs for paginated content
10. **Run Configuration Diagnostics** - Check for config issues
11. **View Statistics** - See aggregate scraping statistics
12. **Exit**

The menu provides:
- ✅ **Pre-scrape preview** - Shows robots.txt delay, seed URLs, and configuration
- ✅ **Organization tracking** - Monitor which NGOs have been scraped
- ✅ **Visual feedback** - Progress bars and real-time statistics
- ✅ **Guided workflows** - Step-by-step wizards for complex tasks

### Command Line Usage

Scrape all configured NGOs:

```bash
python -m src.scraper
```

### Advanced Usage

**Scrape specific NGOs:**
```bash
python -m src.scraper --filter "Hnutí DUHA" "Arnika" "Greenpeace ČR"
```

**Resume interrupted scraping:**
```bash
python -m src.scraper --resume
```

**Use custom configuration:**
```bash
python -m src.scraper --config my_config.yaml --ngo-list my_ngos.csv
```

**Full options:**
```bash
python -m src.scraper --help
```

### Test Dataset Scraper (For Local Testing)

Create a smaller test dataset for testing methods and fine-tuning before running full scrapes:

```bash
# Scrape test dataset from all NGOs (30 HTML pages, 20 PDFs each)
python scripts/test_dataset_scraper.py

# Scrape test dataset from specific NGOs
python scripts/test_dataset_scraper.py --filter "Hnuti DUHA" "Arnika"

# Specify custom date for dataset folder
python scripts/test_dataset_scraper.py --date 20240115
```

**What it does:**
- ✅ Scrapes limited data (30 HTML pages, 20 PDFs per NGO)
- ✅ Saves to `data/test_dataset_{date}/` instead of `data/raw/`
- ✅ Uses same folder structure: `{org_name}/pages/html/` and `{org_name}/documents/`
- ✅ Stops after scraping (no automatic cleanup)
- ✅ Perfect for testing data cleanup methods locally

**Output structure:**
```
data/test_dataset_20240115/
├── Hnuti DUHA/
│   ├── pages/html/          # Up to 30 HTML files
│   ├── documents/           # Up to 20 PDFs
│   ├── links.json
│   └── metadata.json
├── Arnika/
│   ├── pages/html/
│   ├── documents/
│   ├── links.json
│   └── metadata.json
└── metadata/
    └── overall_test_stats.json
```

**Workflow:**
1. Create test dataset: `python scripts/test_dataset_scraper.py --filter "Hnuti DUHA"`
2. Test your cleanup methods on the small dataset
3. Fine-tune parameters and verify results
4. Once satisfied, run full pipeline on complete data

### Programmatic Usage

```python
from src.scraper import NGOScraper

# Initialize scraper
scraper = NGOScraper(config_path='config/scraping_rules.yaml')

# Scrape a single NGO
stats = scraper.scrape_ngo(
    ngo_name="Hnuti DUHA",
    seed_urls=[
        {'url': 'https://www.hnutiduha.cz', 'type': 'homepage', 'depth_limit': 3}
    ],
    max_depth=3,
    max_pages=500
)

# Or scrape from configuration files
scraper.scrape_from_config(
    ngo_list_file='config/ngo_list.csv',
    url_seeds_file='config/url_seeds.csv',
    ngo_filter=['Hnuti DUHA', 'Arnika']  # Optional filter
)
```

### Testing URLs

Before running full scrapes, you can test individual URLs to see what would be scraped:

**Test a single URL:**
```bash
python -m src.test_scraper https://www.hnutiduha.cz
```

This will show you:
- ✓ Robots.txt compliance check
- ✓ HTTP status and content type
- ✓ Number of links found (internal/external)
- ✓ Documents found (PDFs, DOCs, etc.)
- ✓ Extracted metadata
- ✓ Sample links and documents

**Test multiple URLs:**
```bash
python -m src.test_scraper https://site1.org https://site2.org https://site3.org
```

**Test URLs from a file:**
```bash
python -m src.test_scraper --file test_urls.txt
```

**Skip robots.txt check (for testing only):**
```bash
python -m src.test_scraper https://example.org --no-robots
```

**Example output:**
```
================================================================================
Testing URL: https://www.hnutiduha.cz
================================================================================

Step 1: Checking robots.txt compliance...
  ✓ Scraping allowed by robots.txt

Step 2: Fetching URL...
  Status Code: 200
  Content Type: text/html; charset=UTF-8
  Content Length: 45,231 bytes

Step 3: Analyzing content...
  Total Links: 127
    - Internal: 89
    - External: 38
  Documents Found: 5
    Types: {'.pdf': 5}

Step 4: Extracted metadata...
Field            Value
---------------  ----------------------------------------
title            Hnutí DUHA - Friends of the Earth CZ
description      Ekologická organizace...
language         cs

Page Type: homepage
```

This is **extremely useful** for:
- Testing new NGO URLs before adding to config
- Debugging scraping issues
- Verifying robots.txt compliance
- Previewing what will be scraped

## Output Structure

Scraped data is organized as follows:

```
data/
├── raw/
│   └── {ngo_name}/
│       └── {timestamp}/
│           ├── pages/              # HTML files
│           ├── documents/          # PDFs, DOCs, etc.
│           ├── links.json          # All extracted links
│           └── metadata.json       # Session metadata
├── metadata/
│   └── {ngo_name}/
│       └── {timestamp}/
│           ├── pages_metadata.jsonl
│           └── documents_metadata.jsonl
└── logs/
    └── {ngo_name}_{timestamp}.log
```

### Output Files

**`links.json`**: Network analysis data
```json
[
  {
    "source_url": "https://example.org/page1",
    "target_url": "https://example.org/page2",
    "anchor_text": "Read more",
    "link_type": "internal",
    "timestamp": "2024-01-15T10:30:00"
  }
]
```

**`metadata.json`**: Session information
```json
{
  "ngo_name": "Hnutí DUHA",
  "session_timestamp": "20240115_103000",
  "statistics": {
    "total_requests": 245,
    "successful_requests": 240,
    "pages_saved": 220,
    "documents_saved": 15,
    "links_extracted": 1850
  }
}
```

## Data Cleanup & Processing

After scraping, use the included data cleanup tools to prepare content for analysis.

### Tool 1: PDF Text Extraction (`process_pdfs.py`)

Extracts text from PDFs and identifies scanned documents needing OCR:

```bash
# Process all PDFs for an organization
python scripts/process_pdfs.py --org "Hnutí DUHA"

# Process all organizations
python scripts/process_pdfs.py --all

# List available data
python scripts/process_pdfs.py --list
```

**What it does:**
- ✅ Extracts text from PDFs using pdfplumber (preserves tables)
- ✅ Removes repeated headers/footers (>50% page occurrence)
- ✅ Collapses excessive whitespace
- ✅ Quarantines image-based PDFs (<100 chars) for OCR

**Output:**
- `data/processed/{org}/{session}/extracted_text/` - Successfully extracted text files
- `data/processed/{org}/{session}/needs_ocr/` - PDFs requiring OCR processing

### Tool 2: Content Filtering (`filter_content.py`)

Cleans HTML, removes duplicates, and filters for relevance to NGO network analysis:

```bash
# Process all HTML for an organization
python scripts/filter_content.py --org "Hnutí DUHA"

# Adjust filtering thresholds (stricter)
python scripts/filter_content.py --org "Arnika" --min-score 10 --min-density 1.0

# More lenient filtering
python scripts/filter_content.py --org "Arnika" --min-score 3 --min-density 0.3
```

**What it does:**
- ✅ Removes boilerplate (nav, footer, ads, menus)
- ✅ Deduplicates using shingling + Jaccard similarity (85% threshold)
- ✅ Scores relevance using Czech NGO keywords (cooperation, funding, policy actors)
- ✅ Filters using dual criteria: raw score + density per 100 words

**Output:**
- `data/processed/{org}/{session}/relevant/` - Content passing filters (for analysis)
- `data/processed/{org}/{session}/irrelevant/` - Filtered out content
- `data/processed/{org}/{session}/duplicates/` - Duplicate pages (grouped by original)

### Configuring Keywords

Keywords for content filtering are defined in `config/content_filter_keywords.yaml`:

```yaml
keywords:
  relations:
    - root: "spoluprác"
      weight: 3
      variations: ["spolupráce", "spolupracovat", "spolupracující"]
      description: "Cooperation, collaboration"

    - root: "partner"
      weight: 3
      variations: ["partner", "partneři", "partnerství"]
      description: "Partners, partnerships"
```

You can:
- ✏️ Add new keywords and categories
- ⚖️ Adjust weights (1-3) based on importance
- 📝 Add variations to catch different word forms
- 🎯 Customize thresholds in the `filtering` section

**See `DATA_CLEANUP.md` for comprehensive documentation.**

### Tool 3: Actor Extraction with GLiNER (`extract_actors.py`)

Extracts actors (Organizations and Persons) from Czech text using GLiNER for network analysis:

```bash
# Extract actors from one organization (recommended)
python scripts/extract_actors.py --org "Hnutí DUHA"

# Extract from all organizations
python scripts/extract_actors.py --all

# Adjust confidence threshold (lower = more entities, higher = more precise)
python scripts/extract_actors.py --org "Arnika" --threshold 0.3

# List available data
python scripts/extract_actors.py --list
```

**What it does:**
- ✅ Uses GLiNER multilingual model for zero-shot entity recognition
- ✅ Extracts "Organization" and "Person" entities from Czech texts
- ✅ No training required - works out of the box on Czech language
- ✅ Creates SQLite database per organization with:
  - All extracted entities with context
  - Co-occurrence networks (actors mentioned together)
  - Source document tracking
- ✅ Exports JSON files for network analysis

**Output:**
- `data/actors/{org}/{session}/actors.db` - SQLite database
- `data/actors/{org}/{session}/entities.json` - All entities with context
- `data/actors/{org}/{session}/unique_entities.json` - Deduplicated entities
- `data/actors/{org}/{session}/co_occurrence_network.json` - Network edges

**Requirements:**
- Requires GPU for optimal performance (CPU works but slower)
- Downloads ~500MB model on first run
- Processes only "relevant" content by default (from filter_content.py)

### Complete Workflow

```bash
# 1. Scrape data using interactive menu
python scripts/scraper_menu.py

# 2. Extract text from PDFs
python scripts/process_pdfs.py --all

# 3. Filter and clean HTML content
python scripts/filter_content.py --all

# 4. Extract actors (organizations and persons) using GLiNER
python scripts/extract_actors.py --all

# 5. Analyze network data
# (actor databases are in data/actors/*/*, JSON files ready for analysis)
```

## Ethical Compliance

This scraper follows best practices for ethical web scraping:

### 1. User Agent Identification
- Clear identification as academic research
- Contact email included: --

### 2. Robots.txt Compliance
- Automatically checks and respects `robots.txt`
- Honors crawl delays specified by websites
- Can be disabled if needed (but not recommended)

### 3. Rate Limiting
- Default: 2 seconds between requests
- Configurable delays per website
- Automatic backoff on errors

### 4. Resource Consideration
- Limits on pages per site (default: 500)
- Limits on crawl depth (default: 3)
- Efficient duplicate detection

### 5. Transparency
- Comprehensive logging of all activities
- Detailed audit trail
- No attempt to circumvent restrictions

## Troubleshooting

### Common Issues

**Issue: "Blocked by robots.txt"**
```
Solution: The website's robots.txt disallows scraping. This is respected by default.
You can check the robots.txt at: https://example.org/robots.txt
```

**Issue: "Connection timeout"**
```
Solution: Increase the timeout in config:
rate_limiting:
  timeout: 60  # Increase from 30 to 60 seconds
```

**Issue: "Too many failed requests"**
```
Solution:
1. Check your internet connection
2. Verify the URLs are correct
3. Increase delay_between_requests
4. Check website status
```

**Issue: "Memory error during large scrapes"**
```
Solution:
1. Reduce max_pages_per_site
2. Process NGOs individually with --filter
3. Enable checkpoint system (on by default)
```

### Logs and Debugging

- **Console output**: Real-time progress and errors
- **Log files**: Detailed logs in `data/logs/`
- **Set debug level** in `config/scraping_rules.yaml`:
  ```yaml
  logging:
    level: DEBUG
  ```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Structure

- `src/scraper.py` - Main scraper orchestration
- `src/robots_handler.py` - robots.txt compliance
- `src/url_manager.py` - URL queue and deduplication
- `src/content_extractor.py` - HTML parsing and content extraction
- `src/storage.py` - Data storage management

### Adding New Features

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Target Organizations

This scraper is configured for 19 Czech climate NGOs:

1. Aliance pro energetickou soběstačnost
2. Arnika
3. Autoklub ČR
4. Beleco
5. Calla
6. Centrum pro dopravu a energetiku
7. CI2
8. Český svaz ochránců přírody
9. Ekologický institut Veronica
10. Extinction Rebellion
11. Fakta o klimatu
12. Frank Bold
13. Fridays for Future
14. Greenpeace ČR
15. Hnutí DUHA
16. Klimatická koalice
17. Limity jsme my
18. Nesehnutí
19. Zelený kruh

## Research Context

This tool supports thesis research comparing:
- LLM-extracted organizational networks from web content
- Survey-based networks from the COMPON project
- Analysis of Czech climate policy NGO ecosystem

## License

This project is intended for academic research purposes.

## Contact

For questions or issues:
- Email: 498079@mail.muni.cz
- Repository: https://github.com/Leriot/leriot-web-scraper-for-thesis

## Acknowledgments

- Built for thesis research at Masaryk University
- Based on COMPON project methodology
- Follows ethical web scraping guidelines
- Builds upon excellent open-source libraries (see [REFERENCES.md](REFERENCES.md))

Special thanks to the developers and maintainers of:
- Python Software Foundation
- Project Jupyter
- NetworkX team
- Beautiful Soup developers
- All contributors to the scientific Python ecosystem

## Citation

### Citing This Project

If you use this scraper in your research, please cite:

```
[Your Name]. (2024). Academic Web Scraper for NGO Network Analysis.
Thesis Research, Masaryk University.
https://github.com/Leriot/leriot-web-scraper-for-thesis
```

### Citing Dependencies

This project uses many third-party libraries. For academic publications, please also cite the relevant libraries you use. Complete citation information for all dependencies is available in:

- **[REFERENCES.md](REFERENCES.md)** - Comprehensive reference list with APA citations, licenses, and DOIs
- **[REFERENCES.bib](REFERENCES.bib)** - BibTeX file for LaTeX users

Key libraries to cite:
- **NetworkX** (Hagberg et al., 2008) - Network analysis
- **Beautiful Soup** (Richardson, 2023) - Web scraping
- **pandas** (McKinney & pandas team, 2023) - Data processing
- **Jupyter** (Kluyver et al., 2016) - Interactive analysis
- **Matplotlib** (Hunter, 2007) - Visualization

See `REFERENCES.md` for complete citations and DOIs.

