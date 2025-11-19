# NGO Web Scraper - User Guide

## Overview

This scraper has been enhanced with comprehensive session management, pagination handling, and robust checkpointing features to ensure complete and reliable data collection.

## Key Features

### ✨ New in This Update

1. **Session Management** - Track each scraping run with unique IDs and status tracking
2. **Resume Functionality** - Continue interrupted scrapes from where they left off
3. **Per-Organization Scraping** - Scrape specific organizations instead of all at once
4. **Pagination Seed Generator** - Automatically generate URLs for all pagination pages
5. **Interactive Menu** - Text-based UI for managing scraping sessions
6. **Enhanced Logging** - Detailed tracking of why URLs are skipped
7. **Increased Depth Limits** - Better handling of deep pagination structures (depth: 5, pages: 1000)
8. **Checkpointing** - Automatic save points every 50 pages

## Quick Start

### Option 1: Interactive Menu (Recommended)

```bash
python scripts/scraper_menu.py
```

This launches an interactive menu with options to:
- Start new scraping sessions
- Resume previous sessions
- View session status
- Delete old sessions
- Generate pagination seeds
- Run diagnostics

### Option 2: Command Line

```bash
# Scrape specific organization
python scripts/run_scraper.py --organization "Hnutí DUHA"

# Scrape all organizations
python scripts/run_scraper.py

# Resume previous session
python scripts/run_scraper.py --resume --session-id 20231119_143022_Hnuti_DUHA
```

## Fixing Missing Pagination Data

### Problem

The scraper was missing publications because:
1. **Depth limits too low** - Pagination links were discovered at depth > 3
2. **No explicit pagination URLs** - Relied on discovering links in HTML
3. **Page limits reached** - Stopped at 500 pages before finding all content

### Solution: Generate Pagination Seeds

For organizations with many pages of publications (like Hnutí DUHA with 47 pages):

```bash
# Auto-detect max pages
python scripts/generate_pagination_seeds.py \
  "https://hnutiduha.cz/publikace" \
  --ngo-name "Hnutí DUHA" \
  --url-type "publications"

# Or specify manually
python scripts/generate_pagination_seeds.py \
  "https://hnutiduha.cz/publikace" \
  --ngo-name "Hnutí DUHA" \
  --url-type "publications" \
  --max-pages 47
```

This will:
1. Detect or use specified max page number
2. Generate URLs: `publikace?page=1`, `publikace?page=2`, ..., `publikace?page=47`
3. Add them to `config/url_seeds.csv` as individual seed URLs
4. Each page starts at depth 0, ensuring all articles are within depth limit

### Dry Run (Preview Only)

```bash
python scripts/generate_pagination_seeds.py \
  "https://hnutiduha.cz/publikace" \
  --ngo-name "Hnutí DUHA" \
  --url-type "publications" \
  --dry-run
```

## Session Management

### Session States

- **in_progress** - Currently running or can be resumed
- **completed** - Finished successfully
- **failed** - Ended with errors
- **interrupted** - Stopped by user (Ctrl+C)

### Session Output Structure

Each session creates its own output directory:

```
data/
  runs/
    20231119_143022_Hnuti_DUHA/          # Session ID
      checkpoint.json                     # Resume point
      raw/                                # Raw HTML files
      metadata/                           # Extracted metadata
      logs/                               # Session logs
```

### Viewing Session Status

```bash
# Via interactive menu
python scripts/scraper_menu.py
# Choose option [3] View Session Status

# Via Python
python -c "
from src.session_manager import SessionManager
sm = SessionManager()
print(sm.get_session_summary('20231119_143022_Hnuti_DUHA'))
"
```

## Advanced Usage

### Scraping Strategy for Comprehensive Data

1. **Generate pagination seeds first**
   ```bash
   # For each organization with many pages
   python scripts/generate_pagination_seeds.py <url> --ngo-name <name>
   ```

2. **Scrape per organization**
   ```bash
   # Scrape one organization at a time
   python scripts/run_scraper.py --organization "Hnutí DUHA"
   ```

3. **Monitor progress**
   ```bash
   # Check logs in real-time
   tail -f data/runs/<session-id>/logs/scraper.log
   ```

4. **Resume if interrupted**
   ```bash
   # Resume from checkpoint
   python scripts/run_scraper.py --resume --session-id <session-id>
   ```

### Understanding Depth Limits

**Before (depth=3, problem):**
```
depth 0: /publikace
depth 1: /publikace?page=2          ← discovered from page 1
depth 2: /publikace/article-123     ← discovered from page 2
depth 3: /publikace/related-456     ← discovered from article
depth 4: /publikace?page=3          ← SKIPPED! Exceeds depth limit
```

**After (depth=5, with pagination seeds):**
```
depth 0: /publikace                 ← seed
depth 0: /publikace?page=2          ← seed (not discovered!)
depth 0: /publikace?page=3          ← seed
depth 1: /publikace/article-123     ← from page 2
depth 2: /publikace/related-456     ← from article
depth 3: ...                        ← more links
```

### Configuration Updates

**scraping_rules.yaml:**
- `max_depth`: 3 → 5 (handle deeper pagination)
- `max_pages_per_site`: 500 → 1000 (larger sites)

**url_seeds.csv:**
- All depth limits increased from 2-3 to 5

## Troubleshooting

### Missing Pages

**Symptom:** Publications from certain years are missing

**Diagnosis:**
1. Check depth exceeded count:
   ```python
   from src.session_manager import SessionManager
   sm = SessionManager()
   session = sm.get_session('<session-id>')
   # Look at skipped_depth stat
   ```

2. Check if pagination links exist:
   ```bash
   curl https://example.org/publikace | grep "page="
   ```

**Solutions:**
- Generate pagination seeds for that section
- Increase depth limit further if needed
- Check URL exclusion patterns

### Interrupted Scraping

**Symptom:** Scraping stopped mid-way

**Solution:**
```bash
# List resumable sessions
python scripts/scraper_menu.py
# Choose [2] Resume Previous Session

# Or via command line
python scripts/run_scraper.py --resume --session-id <session-id>
```

### Rate Limiting / 403 Errors

**Symptom:** Getting blocked by website

**Solution:**
1. Increase delay in `config/scraping_rules.yaml`:
   ```yaml
   rate_limiting:
     delay_between_requests: 3.0  # increase from 2.0
   ```

2. Check robots.txt:
   ```bash
   python scripts/check_config.py
   ```

## Statistics and Monitoring

### Enhanced URL Skip Tracking

The scraper now tracks exactly why URLs are skipped:

- `skipped_depth` - Exceeded max depth
- `skipped_max_pages` - Hit page limit
- `skipped_excluded` - Matched exclusion pattern
- `skipped_invalid` - Invalid/malformed URL
- `duplicate_count` - Already visited

View stats:
```python
from src.url_manager import URLManager
manager = URLManager("example.org", max_depth=5)
# ... after scraping ...
print(manager.get_stats())
```

### Session Statistics

```bash
python scripts/scraper_menu.py
# Choose [8] View Statistics
```

Shows:
- Total sessions by status
- Pages scraped across all sessions
- Breakdown by organization

## Best Practices

### 1. Scrape Per Organization

Instead of scraping all at once:
```bash
# Good
for org in "Hnutí DUHA" "Arnika" "Greenpeace ČR"; do
  python scripts/run_scraper.py --organization "$org"
done

# Bad (harder to debug if errors occur)
python scripts/run_scraper.py  # all organizations
```

### 2. Generate Pagination Seeds First

Before first scrape:
```bash
# Inspect the website manually
# Count pages or look for "Page X of Y"
# Then generate seeds

python scripts/generate_pagination_seeds.py <url> \
  --ngo-name <name> \
  --max-pages <count>
```

### 3. Use Dry Run for Testing

Test pagination detection:
```bash
python scripts/generate_pagination_seeds.py <url> \
  --ngo-name <name> \
  --dry-run
```

### 4. Monitor Logs

```bash
# Terminal 1: Run scraper
python scripts/run_scraper.py --organization "Hnutí DUHA"

# Terminal 2: Monitor logs
tail -f data/runs/*/logs/scraper.log
```

### 5. Regular Checkpoints

The scraper saves every 50 pages. For very large sites, you can reduce this:

```yaml
# config/scraping_rules.yaml
session:
  checkpoint_interval: 25  # save more frequently
```

## Files Reference

### New Files

- `src/session_manager.py` - Session tracking and management
- `scripts/scraper_menu.py` - Interactive menu interface
- `scripts/run_scraper.py` - Main scraper runner with session support
- `scripts/generate_pagination_seeds.py` - Pagination URL generator
- `data/scraping_sessions.db` - SQLite database for sessions

### Modified Files

- `src/url_manager.py` - Enhanced skip reason tracking
- `config/scraping_rules.yaml` - Increased depth and page limits
- `config/url_seeds.csv` - Increased all depth limits to 5

## Database Schema

### Sessions Table

```sql
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    organization TEXT,
    start_time TEXT NOT NULL,
    end_time TEXT,
    status TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    total_pages_scraped INTEGER DEFAULT 0,
    total_pages_skipped INTEGER DEFAULT 0,
    total_errors INTEGER DEFAULT 0,
    config_snapshot TEXT,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

### Checkpoints Table

```sql
CREATE TABLE checkpoints (
    checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    pages_scraped INTEGER,
    queue_size INTEGER,
    checkpoint_data TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
```

## API Examples

### Python API

```python
from src.session_manager import SessionManager, SessionStatus
from src.scraper import NGOScraper

# Create session
sm = SessionManager()
session_id = sm.create_session(organization="Hnutí DUHA")

# Run scraper
scraper = NGOScraper()
scraper.scrape_from_config(
    ngo_filter=["Hnutí DUHA"],
    resume=False
)

# Update session
sm.update_session_status(session_id, SessionStatus.COMPLETED)

# Query sessions
completed = sm.list_sessions(status=SessionStatus.COMPLETED)
resumable = sm.get_resumable_sessions()
```

## FAQ

**Q: Can I scrape multiple organizations in parallel?**
A: No, the scraper runs sequentially for politeness. But you can run multiple instances with different organizations.

**Q: How do I know if pagination seeds are needed?**
A: If you notice missing content from later pages, or the website has "Page X of Y" indicators.

**Q: Can I delete a session but keep the data?**
A: Yes, when deleting via the menu, choose "No" when asked to delete files.

**Q: What happens if I Ctrl+C during scraping?**
A: The session is marked as "interrupted" and can be resumed later.

**Q: How much disk space is needed?**
A: Depends on content. Estimate ~1-5MB per page including HTML and documents.

## Support

For issues or questions:
1. Run diagnostics: `python scripts/check_config.py`
2. Check logs in `data/runs/<session-id>/logs/`
3. Review session stats: `python scripts/scraper_menu.py` → option [8]

---

**Last Updated:** 2024-11-19
**Version:** 2.0 (Session Management Update)
