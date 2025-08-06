#!/usr/bin/env python3
"""
Al Jazeera scraper matching n8n workflow: sitemap.xml → daily sitemaps → articles → content extraction
"""

import asyncio
import logging
import os
import sys
import json
import random
import time
from typing import List, Optional, Dict, Any, Set
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import xmltodict
from bs4 import BeautifulSoup

@dataclass 
class RetryConfig:
    max_retries: int = 3
    backoff_factor: float = 2.0
    retry_statuses: Set[int] = None
    
    def __post_init__(self):
        if self.retry_statuses is None:
            self.retry_statuses = {429, 500, 502, 503, 504}

class AlJazeeraScraper:
    def __init__(self, settings_file: str = 'settings.json'):
        # Load configuration
        self.load_settings(settings_file)
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        
        # Setup components
        self.setup_logging()
        self.setup_session_config()
        self.setup_retry_config()
        
        # Stats tracking
        self.stats = {
            'sitemaps_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_failed': 0
        }
        
    def load_settings(self, settings_file: str):
        """Load configuration from settings.json file"""
        settings_path = Path(__file__).parent / settings_file
        
        # Default settings
        default_settings = {
            "proxy": {"url": ""},
            "scraper": {
                "max_concurrent_requests": 10,
                "request_delay": 1,
                "batch_size": 5,
                "base_url": "https://aljazeera.net",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            },
            "logging": {"level": "INFO", "file": "aljazeera_scraper.log"},
            "retry": {"max_retries": 3, "backoff_factor": 2.0, "retry_statuses": [429, 500, 502, 503, 504]}
        }
        
        try:
            if settings_path.exists():
                with open(settings_path, 'r') as f:
                    settings = json.load(f)
            else:
                settings = default_settings
                # Create settings file with defaults
                with open(settings_path, 'w') as f:
                    json.dump(default_settings, f, indent=2)
                print(f"Created default settings file: {settings_path}")
        except Exception as e:
            print(f"Error loading settings: {e}. Using defaults.")
            settings = default_settings
            
        # Apply settings
        self.proxy_url = settings["proxy"]["url"]
        self.base_url = settings["scraper"]["base_url"]
        self.max_concurrent = settings["scraper"]["max_concurrent_requests"]
        self.request_delay = settings["scraper"]["request_delay"]
        self.batch_size = settings["scraper"]["batch_size"]
        self.user_agent = settings["scraper"]["user_agent"]
        self.log_level = settings["logging"]["level"]
        self.log_file = settings["logging"]["file"]
        self.retry_settings = settings["retry"]
        
    def setup_logging(self):
        """Enhanced logging with structured format"""
        log_level = getattr(logging, self.log_level.upper())
        
        # Create formatter with more detail
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Setup logger
        self.logger = logging.getLogger('AlJazeeraScraper')
        self.logger.setLevel(log_level)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
        
        if self.proxy_url:
            self.logger.info(f"Using proxy with automatic rotation: {self.proxy_url[-30:]}")
        else:
            self.logger.info("No proxy configured")
        
    def setup_session_config(self):
        """Setup session configuration"""
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
    def setup_retry_config(self):
        """Setup retry configuration"""
        self.retry_config = RetryConfig(
            max_retries=self.retry_settings["max_retries"],
            backoff_factor=self.retry_settings["backoff_factor"],
            retry_statuses=set(self.retry_settings["retry_statuses"])
        )
        
    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        """Fetch URL with retry logic (proxy handles rotation automatically)"""
        
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                # Calculate delay for exponential backoff
                if attempt > 0:
                    delay = self.retry_config.backoff_factor ** (attempt - 1)
                    self.logger.info(f"Retry {attempt}/{self.retry_config.max_retries} for {url} after {delay}s delay")
                    await asyncio.sleep(delay)
                
                # Setup request
                kwargs = {
                    'headers': self.headers,
                    'timeout': aiohttp.ClientTimeout(total=30 + (attempt * 10)),  # Increase timeout on retries
                    'allow_redirects': True
                }
                
                if self.proxy_url:
                    kwargs['proxy'] = self.proxy_url
                    
                # Make request
                async with session.get(url, **kwargs) as response:
                    content = await response.text()
                    
                    if response.status == 200:
                        self.logger.info(f"SUCCESS {url} ({len(content)} chars)")
                        return {
                            'url': url,
                            'status': response.status,
                            'content': content,
                            'headers': dict(response.headers)
                        }
                    elif response.status in self.retry_config.retry_statuses:
                        self.logger.warning(f"HTTP {response.status} for {url} - will retry")
                        continue  # Retry
                    else:
                        self.logger.error(f"HTTP {response.status} for {url} - permanent error")
                        return {'url': url, 'status': response.status, 'error': f"HTTP {response.status}"}
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                if attempt == self.retry_config.max_retries:
                    return {'url': url, 'error': 'Timeout after retries'}
                    
            except Exception as e:
                self.logger.warning(f"Error for {url}: {str(e)} (attempt {attempt + 1})")
                if attempt == self.retry_config.max_retries:
                    return {'url': url, 'error': str(e)}
                    
        return {'url': url, 'error': 'Max retries exceeded'}
        
    async def fetch_main_sitemap(self) -> Optional[Dict[str, Any]]:
        """Step 1: Fetch main sitemap.xml to get list of daily sitemaps"""
        self.logger.info(f"Fetching main sitemap: {self.sitemap_url}")
        
        connector = aiohttp.TCPConnector(limit=1)
        async with aiohttp.ClientSession(connector=connector) as session:
            result = await self.fetch_with_retry(session, self.sitemap_url)
            
        if result and not result.get('error'):
            try:
                # Parse XML to get sitemap URLs
                xml_data = xmltodict.parse(result['content'])
                sitemaps = xml_data.get('sitemapindex', {}).get('sitemap', [])
                
                # Ensure it's a list
                if isinstance(sitemaps, dict):
                    sitemaps = [sitemaps]
                    
                sitemap_urls = [sitemap.get('loc') for sitemap in sitemaps if sitemap.get('loc')]
                
                self.logger.info(f"Found {len(sitemap_urls)} daily sitemaps")
                return {
                    'sitemap_urls': sitemap_urls,
                    'count': len(sitemap_urls)
                }
            except Exception as e:
                self.logger.error(f"Failed to parse main sitemap XML: {str(e)}")
                return None
        else:
            self.logger.error(f"Failed to fetch main sitemap: {result.get('error') if result else 'Unknown error'}")
            return None
            
    async def fetch_daily_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str) -> List[str]:
        """Step 2: Fetch individual daily sitemap and extract article URLs"""
        result = await self.fetch_with_retry(session, sitemap_url)
        
        if result and not result.get('error'):
            try:
                # Parse XML to get article URLs  
                xml_data = xmltodict.parse(result['content'])
                urls = xml_data.get('urlset', {}).get('url', [])
                
                # Ensure it's a list
                if isinstance(urls, dict):
                    urls = [urls]
                    
                article_urls = [url.get('loc') for url in urls if url.get('loc')]
                
                self.logger.info(f"Daily sitemap {sitemap_url} contains {len(article_urls)} articles")
                self.stats['sitemaps_processed'] += 1
                self.stats['articles_found'] += len(article_urls)
                
                return article_urls
            except Exception as e:
                self.logger.error(f"Failed to parse daily sitemap {sitemap_url}: {str(e)}")
                return []
        else:
            self.logger.error(f"Failed to fetch daily sitemap {sitemap_url}: {result.get('error') if result else 'Unknown error'}")
            return []
    def clean_html_content(self, html_content: str) -> str:
        """Step 4: Clean HTML content and extract text (matching n8n workflow)"""
        if not html_content:
            return ""
            
        # Remove script, style, and other non-visible tags
        cleaned_html = html_content
        
        # Remove script and style tags with their content
        cleaned_html = BeautifulSoup(cleaned_html, 'html.parser')
        for script in cleaned_html(["script", "style"]):
            script.decompose()
        
        # Get text and clean it
        text = cleaned_html.get_text()
        
        # Clean up text (matching your n8n JavaScript)
        text = (text
                .replace('\xa0', ' ')  # non-breaking space
                .replace('\u200b', '')  # zero-width space
                .replace('\n', ' ')     # newlines to spaces
                .replace('\t', ' ')     # tabs to spaces
                )
        
        # Collapse multiple spaces into single space
        import re
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def extract_article_data(self, html_content: str, url: str) -> Dict[str, Any]:
        """Step 3: Extract structured data from article HTML (matching n8n selectors)"""
        if not html_content:
            return {'url': url, 'error': 'No content'}
            
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract title
            title_elem = soup.select_one('#main-content-area > header > h1')
            title = title_elem.get_text().strip() if title_elem else ""
            
            # Extract topics/tags
            topic_elems = soup.select('#main-content-area > header > div > div > a')
            topics = [elem.get_text().strip() for elem in topic_elems]
            
            # Extract article paragraphs (HTML)
            paragraph_elems = soup.select('#main-content-area > div.wysiwyg.wysiwyg--all-content > *')
            
            aritcle_html = soup.select_one('#main-content-area > div.wysiwyg.wysiwyg--all-content')
            article_paragraphs_html = [str(elem) for elem in paragraph_elems]
            
            # Extract images
            img_elems = soup.select('main img')
            images = []
            for img in img_elems:
                images.append({
                    'url': img.get('src', ''),
                    'alt': img.get('alt', '')
                })
            
            # Extract metadata
            meta_elems = soup.select('head > meta')
            metadata = {}
            for meta in meta_elems:
                name = meta.get('name') or meta.get('property')
                content = meta.get('content')
                if name and content:
                    metadata[name] = content
            
            # Clean content text
            content_text = self.clean_html_content(' '.join(article_paragraphs_html))
            
            # Filter out empty content
            if not content_text or len(content_text.strip()) < 50:
                return {'url': url, 'error': 'Content too short or empty'}
            
            return {
                'url': url,
                'title': title,
                'topics': topics,
                'content': content_text,
                'content_html': aritcle_html,
                'images': images,
                'metadata': metadata,
                'content_length': len(content_text)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from {url}: {str(e)}")
            return {'url': url, 'error': f"Extraction failed: {str(e)}"}
            
    async def scrape_article_batch(self, session: aiohttp.ClientSession, article_urls: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of articles with concurrency control"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_article(url):
            async with semaphore:
                # Fetch article HTML
                result = await self.fetch_with_retry(session, url)
                
                if result and not result.get('error'):
                    # Extract structured data
                    article_data = self.extract_article_data(result['content'], url)
                    
                    if not article_data.get('error'):
                        self.stats['articles_scraped'] += 1
                        self.logger.info(f"✅ Extracted: {article_data.get('title', 'No title')[:50]}...")
                    else:
                        self.stats['articles_failed'] += 1
                        self.logger.warning(f"❌ Failed extraction: {url} - {article_data.get('error')}")
                        
                    # Add delay
                    if self.request_delay > 0:
                        await asyncio.sleep(self.request_delay + random.uniform(0, 1))
                        
                    return article_data
                else:
                    self.stats['articles_failed'] += 1
                    error_msg = result.get('error') if result else 'Unknown error'
                    self.logger.warning(f"❌ Failed fetch: {url} - {error_msg}")
                    return {'url': url, 'error': error_msg}
        
        # Process all articles in batch
        tasks = [process_article(url) for url in article_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        clean_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Task exception for {article_urls[i]}: {str(result)}")
                self.stats['articles_failed'] += 1
                clean_results.append({'url': article_urls[i], 'error': str(result)})
            else:
                clean_results.append(result)
                
        return clean_results
        
    async def scrape_all_sitemaps(self, limit_sitemaps: Optional[int] = None) -> Dict[str, Any]:
        """Main workflow: Fetch main sitemap → daily sitemaps → articles → extract data"""
        start_time = time.time()
        
        # Step 1: Get main sitemap
        self.logger.info("🚀 Starting Al Jazeera scraping workflow")
        main_sitemap = await self.fetch_main_sitemap()
        
        if not main_sitemap:
            return {'error': 'Failed to fetch main sitemap'}
            
        sitemap_urls = main_sitemap['sitemap_urls']
        
        # Apply limit if specified
        if limit_sitemaps:
            sitemap_urls = sitemap_urls[:limit_sitemaps]
            self.logger.info(f"Limited to first {limit_sitemaps} sitemaps")
            
        self.logger.info(f"Processing {len(sitemap_urls)} daily sitemaps")
        
        # Step 2: Process daily sitemaps in batches
        all_articles = []
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(0, len(sitemap_urls), self.batch_size):
                batch = sitemap_urls[i:i + self.batch_size]
                self.logger.info(f"Processing sitemap batch {i//self.batch_size + 1}/{(len(sitemap_urls) + self.batch_size - 1)//self.batch_size}")
                
                # Fetch daily sitemaps concurrently
                daily_tasks = [self.fetch_daily_sitemap(session, url) for url in batch]
                daily_results = await asyncio.gather(*daily_tasks)
                
                # Collect all article URLs from this batch
                batch_articles = []
                for article_urls in daily_results:
                    batch_articles.extend(article_urls)
                    
                if batch_articles:
                    self.logger.info(f"Found {len(batch_articles)} articles in batch, scraping...")
                    
                    # Step 3 & 4: Scrape articles and extract data
                    article_data = await self.scrape_article_batch(session, batch_articles)
                    all_articles.extend(article_data)
                    
                    # Progress update
                    elapsed = time.time() - start_time
                    self.logger.info(f"📊 Progress: {len(all_articles)} articles processed in {elapsed:.1f}s")
                    
        # Final stats
        elapsed = time.time() - start_time
        successful_articles = [a for a in all_articles if not a.get('error')]
        
        results = {
            'success': True,
            'stats': {
                **self.stats,
                'total_articles': len(all_articles),
                'successful_articles': len(successful_articles),
                'failed_articles': len(all_articles) - len(successful_articles),
                'processing_time': elapsed,
                'articles_per_second': len(all_articles) / elapsed if elapsed > 0 else 0
            },
            'articles': all_articles
        }
        
        self.logger.info(f"✅ Scraping complete! {len(successful_articles)}/{len(all_articles)} articles in {elapsed:.1f}s")
        return results

# CLI interface matching n8n workflow
async def main():
    scraper = AlJazeeraScraper()
    
    # Parse command line arguments
    limit_sitemaps = None
    if len(sys.argv) > 1:
        try:
            limit_sitemaps = int(sys.argv[1])
        except ValueError:
            print(f"Invalid limit: {sys.argv[1]}. Using default (no limit)")
    
    try:
        # Run the full workflow
        results = await scraper.scrape_all_sitemaps(limit_sitemaps=limit_sitemaps)
        
        if results.get('error'):
            print(f"\n❌ Scraping failed: {results['error']}")
            sys.exit(1)
            
        # Display results
        stats = results['stats']
        print(f"\n🎉 === Scraping Results ===")
        print(f"Sitemaps processed: {stats['sitemaps_processed']}")
        print(f"Articles found: {stats['articles_found']}")
        print(f"Articles scraped: {stats['successful_articles']}/{stats['total_articles']}")
        print(f"Failed articles: {stats['failed_articles']}")
        print(f"Processing time: {stats['processing_time']:.1f}s")
        print(f"Speed: {stats['articles_per_second']:.2f} articles/second")
        
        # Save results to file
        timestamp = int(time.time())
        output_file = f"aljazeera_articles_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        # Save just successful articles to separate file
        successful_articles = [a for a in results['articles'] if not a.get('error')]
        if successful_articles:
            clean_file = f"aljazeera_clean_{timestamp}.json"
            with open(clean_file, 'w', encoding='utf-8') as f:
                json.dump(successful_articles, f, indent=2, ensure_ascii=False)
            print(f"💾 Clean articles saved to: {clean_file}")
        
    except KeyboardInterrupt:
        print("\n⏹️ Scraping interrupted by user")
        print(f"Stats so far: {scraper.stats}")
    except Exception as e:
        print(f"\n💥 Scraping failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())