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

### Basic Usage

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

### Programmatic Usage

```python
from src.scraper import NGOScraper

# Initialize scraper
scraper = NGOScraper(config_path='config/scraping_rules.yaml')

# Scrape a single NGO
stats = scraper.scrape_ngo(
    ngo_name="Hnutí DUHA",
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
    ngo_filter=['Hnutí DUHA', 'Arnika']  # Optional filter
)
```

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
- Repository: [GitHub URL]

## Acknowledgments

- Built for thesis research at Masaryk University
- Based on COMPON project methodology
- Follows ethical web scraping guidelines

## Citation

If you use this scraper in your research, please cite:

```
[Your Name]. (2024). Academic Web Scraper for NGO Network Analysis.
Thesis Research, Masaryk University.
```

## Changelog

### Version 1.0.0 (2024)
- Initial release
- Support for 20 Czech NGOs
- Robots.txt compliance
- Link extraction and classification
- Document download
- Session resumption
- Comprehensive logging
