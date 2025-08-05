# Al Jazeera Scraper

Production-ready scraper following the exact n8n workflow: sitemap.xml → daily sitemaps → articles → content extraction.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full scraper (all sitemaps)
python3 scraper.py

# Run with limit (first 5 sitemaps only)
python3 scraper.py 5
```

## Workflow (Matches n8n Implementation)

1. **Fetch Main Sitemap** → `https://aljazeera.net/sitemap.xml`
2. **Parse Sitemap Index** → Extract daily sitemap URLs
3. **Process Daily Sitemaps** → Extract article URLs (batch processing)
4. **Scrape Articles** → Fetch HTML content with retries
5. **Extract Data** → Use BeautifulSoup with exact CSS selectors
6. **Clean Content** → Text cleaning matching n8n JavaScript

## Features

- ✅ **Exact n8n workflow replication**
- ✅ **Proxy support with automatic rotation**
- ✅ **Exponential backoff retries**
- ✅ **Structured logging with metrics**
- ✅ **Batch processing with concurrency control**
- ✅ **Graceful failure handling**
- ✅ **CSS selector-based extraction**
- ✅ **Content cleaning & validation**

## Failure Handling & Retries

- **HTTP Errors (429, 5xx):** Exponential backoff with retries
- **Network Timeouts:** Timeout escalation with retries  
- **Proxy Failures:** Leverages automatic proxy rotation
- **Rate Limiting:** Dynamic delay adjustment
- **Content Errors:** Graceful skip with detailed logging

## Logging Strategy

- **Structured logs** with request/response metrics
- **Proxy usage** with automatic rotation handling
- **Progress updates** with real-time statistics
- **Error categorization** (network, parsing, proxy, content)
- **Performance metrics** (articles/second, success rates)

## Configuration (settings.json)

Copy the example and customize:
```bash
cp settings.example.json settings.json
```

```json
{
  "proxy": {
    "url": "http://username:password@proxy.example.com:8080"
  },
  "scraper": {
    "max_concurrent_requests": 10,
    "request_delay": 1,
    "batch_size": 5,
    "base_url": "https://aljazeera.net"
  },
  "logging": {
    "level": "INFO",
    "file": "aljazeera_scraper.log"
  },
  "retry": {
    "max_retries": 3,
    "backoff_factor": 2.0
  }
}
```

**Note:** `settings.json` is git-ignored for security.

## Output Files

- `aljazeera_articles_{timestamp}.json` - Full results with stats
- `aljazeera_clean_{timestamp}.json` - Successfully extracted articles only

## Programmatic Usage

```python
from scraper import AlJazeeraScraper

scraper = AlJazeeraScraper()

# Full workflow
results = await scraper.scrape_all_sitemaps(limit_sitemaps=10)

# Individual steps  
main_sitemap = await scraper.fetch_main_sitemap()
articles = await scraper.scrape_article_batch(session, urls)
```