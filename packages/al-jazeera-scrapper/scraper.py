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
import re
import uuid 
from datetime import datetime, date, timezone
from typing import List, Optional, Dict, Any, Set, Tuple
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import threading

# Load environment variables from .env file if it exists
load_dotenv()

import aiohttp
import xmltodict
from bs4 import BeautifulSoup
import asyncpg  # type: ignore

sys.path.append(os.path.abspath('/Users/macmini/elysia-starter/alifbee/alifbee-corpus-stack/packages/al-jazeera-scrapper'))

# Prefer fast lxml parser if available
try:
    import lxml  # noqa: F401
    BS_PARSER = 'lxml'
except Exception:
    BS_PARSER = 'html.parser'

# Precompile commonly used regex patterns for speed
SENTENCE_SPLIT_RE = re.compile(r'([.!؟\?]\s|[.!؟\?]$)')
COLLAPSE_WS_RE = re.compile(r'\s+')
WORD_SPLIT_RE = re.compile(r'\s+')
PUNCT_EDGES_RE = re.compile(r'^[\W]+|[\W]+$')
EN_NUM_RE = re.compile(r'[a-zA-Z0-9]')
EN_LETTER_RE = re.compile(r'[a-zA-Z]')
ARABIC_DIACRITICS_RE = re.compile(r'[\u064B-\u065F\u0670]')

# Allow importing local packages (e.g., tashkil)
sys.path.append(str(Path(__file__).parent.parent))

# Supabase client no longer used; we connect directly via CONNECTION_STRING

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
        
        # Initialize direct DB access via CONNECTION_STRING (preferred)
        self.connection_string: Optional[str] = os.environ.get("CONNECTION_STRING")
        self.db_pool = None
        if self.connection_string and asyncpg:
            # Defer actual pool creation to first use to avoid blocking __init__
            self.db_enabled = True
            self.logger.info("Database writes enabled via CONNECTION_STRING")
        else:
            self.db_enabled = False
            if not self.connection_string:
                self.logger.warning("CONNECTION_STRING not found. Database storage will be disabled.")
            elif not asyncpg:
                self.logger.error("asyncpg is not installed; cannot use CONNECTION_STRING. Database storage disabled.")
        
        # Initialize TashkilUnified
        api_key = os.environ.get("GEMINI_API_KEY")
        tashkil_method = os.environ.get("TASHKIL_METHOD", "mishkal").lower()
        self.tashkil_method = tashkil_method
        
        if os.environ.get("TASHKIL_ENABLED", "1") != "1":
            self.tashkil_ai = None
            self.logger.info("Tashkil disabled via TASHKIL_ENABLED=0")
        elif tashkil_method == "mishkal":
            # For Mishkal, create a small thread pool and a per-thread Mishkal instance for concurrency
            self.tashkil_ai = None
            max_workers = 1000
            self._tashkil_executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=max_workers)
            # Thread-local store: each worker thread gets its own Mishkal instance (avoids cross-thread state)
            self._tashkil_thread_local = threading.local()
            self._tashkil_ai_thread = None  # Kept for compatibility; not used
            self.logger.info(f"Mishkal will be initialized lazily with per-thread instances (workers={max_workers})")
        elif tashkil_method == "gemini":
            if not api_key:
                self.logger.warning("GEMINI_API_KEY not found. Tashkil functionality will be disabled.")
                self.tashkil_ai = None
            else:
                # Use the same executor infra for Gemini calls to avoid blocking the event loop
                max_workers = 1000
                self._tashkil_executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=max_workers)
                self.tashkil_ai = TashkilUnified(method="gemini", api_key=api_key)
                self.logger.info(f"Initialized TashkilUnified with Gemini method (workers={max_workers})")
        else:
            self.logger.warning(f"Unknown TASHKIL_METHOD: {tashkil_method}. Using Gemini if API key available.")
            if api_key:
                max_workers = 1000
                self._tashkil_executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=max_workers)
                self.tashkil_ai = TashkilUnified(method="gemini", api_key=api_key)
                self.logger.info(f"Initialized TashkilUnified with Gemini method (workers={max_workers})")
            else:
                self.tashkil_ai = None
        
        # Stats tracking
        self.stats = {
            'sitemaps_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_failed': 0,
            'sentences_processed': 0,
            'words_processed': 0,
            'articles_skipped': 0
        }

        # Track existing articles to avoid re-scraping (filled from Supabase once)
        self.existing_article_urls: Set[str] = set()
        self.existing_article_slugs: Set[str] = set()
        
    def parse_datetime_str(self, value: Any) -> Optional[datetime]:
        """Parse various datetime/date string formats into timezone-aware datetime (UTC)."""
        try:
            if isinstance(value, datetime):
                # Ensure tz-aware; assume UTC if naive
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            if isinstance(value, date):
                return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            if not isinstance(value, str):
                return None
            s = value.strip()
            if not s:
                return None
            # Normalize Zulu suffix
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            # Try ISO8601 first
            try:
                dt = datetime.fromisoformat(s)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
            # Common fallbacks
            fmts = [
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f%z',
                '%Y-%m-%d %H:%M:%S%z',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    # If no tz info in format, assume UTC
                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def to_naive_utc(self, value: Optional[datetime]) -> Optional[datetime]:
        """Convert datetime to naive UTC (drop tzinfo after converting to UTC)."""
        if not isinstance(value, datetime):
            return None
        if value.tzinfo:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

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
            
    def split_into_sentences(self, text: str) -> List[str]:
        """Split Arabic text into sentences"""
        if not text:
            return []
            
        # Split by sentence endings followed by space or end of string
        sentences = SENTENCE_SPLIT_RE.split(text)
        
        # Combine sentence with its ending punctuation
        result = []
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                result.append(sentences[i] + sentences[i+1].strip())
            else:
                result.append(sentences[i])
                
        # Add the last part if it's not empty and wasn't added
        if len(sentences) % 2 != 0 and sentences[-1].strip():
            result.append(sentences[-1].strip())
            
        # Filter out empty sentences
        return [s.strip() for s in result if s.strip()]
        
    def extract_words(self, text: str) -> List[str]:
        """Extract individual words from Arabic text"""
        if not text:
            return []
            
        # Split by whitespace and remove empty strings
        words = [word.strip() for word in WORD_SPLIT_RE.split(text) if word.strip()]
        
        # Remove punctuation from words and filter out non-Arabic words
        clean_words = []
        for word in words:
            # Remove punctuation at start/end of word
            clean_word = PUNCT_EDGES_RE.sub('', word)
            
            # Skip words containing English letters or numbers
            if EN_NUM_RE.search(clean_word):
                continue
                
            # Only include non-empty words
            if clean_word:
                clean_words.append(clean_word)
                
        return clean_words
        
    def extract_word_without_tashkil(self, word_with_tashkil: str) -> str:
        """Remove tashkil (diacritics) from Arabic word"""
        return ARABIC_DIACRITICS_RE.sub('', word_with_tashkil)
        
    def generate_word_id(self, word: str) -> str:
        """Generate a deterministic ID for a word based on its content"""
        # Use a hash of the word as the ID
        import hashlib
        # Create a UUID based on the hash of the word
        word_hash = hashlib.md5(word.encode('utf-8')).hexdigest()
        return str(uuid.UUID(word_hash))
        
    def setup_session_config(self):
        """Setup session configuration"""
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
    def setup_retry_config(self):
        """Setup retry configuration"""
        self.retry_config = RetryConfig(
            max_retries=self.retry_settings["max_retries"],
            backoff_factor=self.retry_settings["backoff_factor"],
            retry_statuses=set(self.retry_settings["retry_statuses"])
        )

    def get_slug_from_url(self, url: str) -> str:
        """Generate slug from article URL to match DB slug logic"""
        try:
            parsed_url = urlparse(url)
            return parsed_url.path.strip('/').split('/')[-1]
        except Exception:
            return ""

    async def load_existing_articles(self) -> None:
        """Load existing article URLs/slugs from Supabase once to skip duplicates"""
        if not self.db_enabled:
            self.logger.info("DB disabled; skipping existing articles prefetch")
            return
        try:
            self.logger.info("Fetching existing articles from DB (single query)...")
            pool = await self.get_db_pool()
            if not pool:
                return
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT url, slug FROM articles")
                for row in rows:
                    url_val = row.get("url") if isinstance(row, dict) else row["url"]
                    slug_val = row.get("slug") if isinstance(row, dict) else row["slug"]
                    if url_val:
                        self.existing_article_urls.add(url_val)
                    if slug_val:
                        self.existing_article_slugs.add(slug_val)
            self.logger.info(f"Loaded {len(self.existing_article_urls)} existing article URLs and {len(self.existing_article_slugs)} slugs")
        except Exception as e:
            self.logger.warning(f"Failed to fetch existing articles: {str(e)}")

    async def get_db_pool(self):
        """Lazily create and return an asyncpg pool."""
        if not self.db_enabled:
            return None
        if self.db_pool is None:
            try:
                self.db_pool = await asyncpg.create_pool(self.connection_string, min_size=1, max_size=5)
            except Exception as e:
                self.logger.error(f"Failed to create DB pool: {str(e)}")
                self.db_enabled = False
                return None
        return self.db_pool
        
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
                        self.logger.debug(f"SUCCESS {url} ({len(content)} chars)")
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
        cleaned_html = BeautifulSoup(cleaned_html, BS_PARSER)
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
        text = COLLAPSE_WS_RE.sub(' ', text).strip()
        
        return text
        
    def clean_tashkil_output(self, text: str) -> str:
        """Clean up the tashkil output by removing AI instructions and XML tags"""
        # Remove any XML/HTML-like tags that might be in the response
        # text = re.sub(r'<task>.*?</task>', '', text, flags=re.DOTALL)
        # text = re.sub(r'<instructions>.*?</instructions>', '', text, flags=re.DOTALL)
        # text = re.sub(r'<article_input>.*?</article_input>', '', text, flags=re.DOTALL)
        
        # Remove any remaining XML/HTML tags
        # text = re.sub(r'<[^>]+>', '', text)
        
        # # Remove lines that might contain AI instructions
        # lines_to_remove = [
        #     "You are an expert in Arabic diacritization",
        #     "Your task is to add accurate",
        #     "Please follow these rules",
        #     "Do not modify the words",
        #     "Preserve the original structure",
        #     "Add diacritics to every word",
        #     "Here is the text with diacritics:",
        #     "Text with diacritics:"
        # ]
        
        # for line in lines_to_remove:
        #     text = re.sub(r'.*' + re.escape(line) + r'.*\n?', '', text, flags=re.IGNORECASE)
        
        # # Clean up extra whitespace
        # text = COLLAPSE_WS_RE.sub(' ', text).strip()
        
        return text
        
    def filter_non_arabic(self, text: str) -> str:
        """Filter out English letters from sentence text (preserve digits)"""
        # Replace English letters with spaces, keep digits
        text = EN_LETTER_RE.sub(' ', text)
        
        # Clean up extra whitespace created by replacements
        text = COLLAPSE_WS_RE.sub(' ', text).strip()
        
        return text
    
    async def process_tashkil(self, text: str) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process text through TashkilAI and prepare database records.
        Ensures 1:1 sentence alignment by falling back to per-sentence diacritization when needed.
        """
        async def tashkeel_once(input_text: str) -> str:
            if not input_text:
                return ""
            if getattr(self, 'tashkil_method', '') == 'mishkal' and hasattr(self, '_tashkil_executor'):
                loop = asyncio.get_running_loop()
                def ensure_mishkal_and_tashkeel():
                    # One Mishkal instance per worker thread
                    local_store = getattr(self, '_tashkil_thread_local', None)
                    if local_store is None:
                        self._tashkil_thread_local = threading.local()
                        local_store = self._tashkil_thread_local
                    if not hasattr(local_store, 'tashkil_ai') or local_store.tashkil_ai is None:
                        from tashkil.tashkil_unified import TashkilUnified as _TU  # type: ignore
                        local_store.tashkil_ai = _TU(method="mishkal")
                    return local_store.tashkil_ai.tashkeel(input_text)
                return await loop.run_in_executor(self._tashkil_executor, ensure_mishkal_and_tashkeel)
            elif self.tashkil_ai:
                loop = asyncio.get_running_loop()
                # Route to the shared executor to use the same package for both methods
                if hasattr(self, '_tashkil_executor') and self._tashkil_executor is not None:
                    return await loop.run_in_executor(self._tashkil_executor, self.tashkil_ai.tashkeel, input_text)
                return await asyncio.to_thread(self.tashkil_ai.tashkeel, input_text)
            else:
                return input_text

        # 1) Split original into sentences
        original_sentences = self.split_into_sentences(text)

        # 2) Try full-article diacritization first
        full_tashkil_text = text
        tashkil_sentences: List[str] = []
        if self.tashkil_ai or getattr(self, 'tashkil_method', '') == 'mishkal':
            try:
                self.logger.info(f"Adding tashkil to full article ({len(text)} chars)...")
                #raw_tashkil_output = await tashkeel_once(text)
                raw_tashkil_output = ""
                full_tashkil_text = self.clean_tashkil_output(raw_tashkil_output)
                tashkil_sentences = self.split_into_sentences(full_tashkil_text)
            except Exception as e:
                self.logger.error(f"Error adding tashkil to full article: {str(e)}")
                full_tashkil_text = text
                tashkil_sentences = []
        else:
            full_tashkil_text = text
            tashkil_sentences = []

        # 3) If sentence counts mismatch, fall back to per-sentence diacritization to guarantee alignment
        if len(original_sentences) != len(tashkil_sentences) or not tashkil_sentences:
            if self.tashkil_ai or getattr(self, 'tashkil_method', '') == 'mishkal':
                self.logger.warning(
                    f"Sentence count mismatch (orig={len(original_sentences)} vs tash={len(tashkil_sentences)}). Falling back to per-sentence."
                )
                # Run per-sentence tashkeel concurrently using the existing executor
                try:
                    tasks = [tashkeel_once(s) for s in original_sentences]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    tashkil_sentences = []
                    for idx, res in enumerate(results):
                        if isinstance(res, Exception):
                            # Preserve alignment on failure
                            tashkil_sentences.append(original_sentences[idx])
                        else:
                            tashkil_sentences.append(self.clean_tashkil_output(res))
                except Exception:
                    # Fallback to sequential processing if gather fails unexpectedly
                    tashkil_sentences = []
                    for s in original_sentences:
                        try:
                            raw = await tashkeel_once(s)
                            tashkil_sentences.append(self.clean_tashkil_output(raw))
                        except Exception:
                            tashkil_sentences.append(s)
                full_tashkil_text = " ".join(tashkil_sentences)
            else:
                # No tashkil engine configured; mirror original to maintain alignment
                tashkil_sentences = list(original_sentences)
                full_tashkil_text = text

        # 4) Build records
        self.stats['sentences_processed'] += len(original_sentences)
        sentence_records: List[Dict[str, Any]] = []
        word_records: List[Dict[str, Any]] = []

        for i, (original_sentence, tashkil_sentence) in enumerate(zip(original_sentences, tashkil_sentences)):
            temp_sentence_id = str(i)
            filtered_original = self.filter_non_arabic(original_sentence)
            filtered_tashkil = self.filter_non_arabic(tashkil_sentence)
            sentence_records.append({
                "id": temp_sentence_id,
                "text": filtered_original,
                "tashkil": filtered_tashkil
            })
            words_with_tashkil = self.extract_words(tashkil_sentence)
            self.stats['words_processed'] += len(words_with_tashkil)
            for word_with_tashkil in words_with_tashkil:
                word_without_tashkil = self.extract_word_without_tashkil(word_with_tashkil)
                word_records.append({
                    "word": word_with_tashkil,
                    "word_without_ichkal": word_without_tashkil,
                    "sentence_id": temp_sentence_id
                })

        return full_tashkil_text, sentence_records, word_records
    
    def extract_article_data(self, html_content: str, url: str) -> Dict[str, Any]:
        """Step 3: Extract structured data from article HTML (matching n8n selectors)"""
        if not html_content:
            return {'url': url, 'error': 'No content'}
            
        try:
            soup = BeautifulSoup(html_content, BS_PARSER)
            
            # Extract title
            title_elem = soup.select_one('#main-content-area > header > h1')
            title = title_elem.get_text().strip() if title_elem else ""
            
            # Extract topics/tags (primary UI chips)
            topic_elems = soup.select('#main-content-area > header > div > div > a')
            topics = [elem.get_text().strip() for elem in topic_elems]
            
            # Extract article paragraphs (HTML)
            paragraph_elems = soup.select('#main-content-area > div.wysiwyg.wysiwyg--all-content > *')
            
            article_html = soup.select_one('#main-content-area > div.wysiwyg.wysiwyg--all-content')
            article_paragraphs_html = [str(elem) for elem in paragraph_elems]
            
            # Extract images
            img_elems = soup.select('main img')
            images = []
            for img in img_elems:
                img_url = img.get('src', '')
                # Add base URL if the image URL is relative
                if img_url and not img_url.startswith('http'):
                    if img_url.startswith('/'):
                        img_url = f"https://www.aljazeera.net{img_url}"
                    else:
                        img_url = f"https://www.aljazeera.net/{img_url}"
                
                images.append({
                    'url': img_url,
                    'alt': img.get('alt', '')
                })
            
            # Extract metadata
            meta_elems = soup.select('head > meta')
            metadata: Dict[str, Any] = {}
            for meta in meta_elems:
                key = meta.get('name') or meta.get('property') or meta.get('itemprop')
                content = meta.get('content')
                if key and content:
                    # Aggregate duplicates as list (e.g., multiple article:tag entries)
                    existing = metadata.get(key)
                    if existing is None:
                        metadata[key] = content
                    elif isinstance(existing, list):
                        existing.append(content)
                    else:
                        metadata[key] = [existing, content]

            # Include JSON-LD blocks under metadata["jsonld"]
            jsonld_blocks = []
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    # Some sites include multiple JSON objects in a single script tag
                    parsed = json.loads(script.string or "{}")
                    jsonld_blocks.append(parsed)
                except Exception:
                    # Ignore malformed JSON-LD
                    continue
            if jsonld_blocks:
                metadata["jsonld"] = jsonld_blocks if len(jsonld_blocks) > 1 else jsonld_blocks[0]
            
            # Title fallbacks from metadata/JSON-LD/<title>
            if not title:
                title_candidates: List[str] = []
                for k in [
                    'og:title', 'twitter:title', 'title', 'pageTitle'
                ]:
                    v = metadata.get(k)
                    if isinstance(v, list):
                        title_candidates.extend([x for x in v if isinstance(x, str) and x.strip()])
                    elif isinstance(v, str) and v.strip():
                        title_candidates.append(v)
                # JSON-LD headline
                try:
                    jsonld_source = metadata.get('jsonld')
                    jsonld_candidates = jsonld_source if isinstance(jsonld_source, list) else [jsonld_source] if jsonld_source else []
                    for block in jsonld_candidates:
                        if isinstance(block, dict):
                            hl = block.get('headline')
                            if isinstance(hl, str) and hl.strip():
                                title_candidates.append(hl)
                except Exception:
                    pass
                if not title_candidates:
                    try:
                        if soup.title and soup.title.string:
                            title_candidates.append(soup.title.string.strip())
                    except Exception:
                        pass
                if title_candidates:
                    title = title_candidates[0]

            # Clean content text
            content_text = self.clean_html_content(' '.join(article_paragraphs_html))
            
            # Filter out empty content
            if not content_text or len(content_text.strip()) < 50:
                return {'url': url, 'error': 'Content too short or empty'}
            
            # Generate slug from URL
            parsed_url = urlparse(url)
            slug = parsed_url.path.strip('/').split('/')[-1]
            
            # Extract published date from metadata (with broader fallbacks)
            published_at_raw = (
                metadata.get('article:published_time')
                or metadata.get('og:published_time')
                or metadata.get('publishedDate')
                or metadata.get('datePublished')
            )
            if not published_at_raw:
                try:
                    # Try JSON-LD datePublished
                    jsonld_source = metadata.get('jsonld')
                    jsonld_candidates = jsonld_source if isinstance(jsonld_source, list) else [jsonld_source] if jsonld_source else []
                    for block in jsonld_candidates:
                        if isinstance(block, dict):
                            published_at_raw = (
                                block.get('datePublished')
                                or block.get('dateCreated')
                                or block.get('uploadDate')
                            )
                            if published_at_raw:
                                break
                except Exception:
                    pass
            published_at_dt = self.to_naive_utc(self.parse_datetime_str(published_at_raw))

            # Fallback topics from metadata (e.g., multiple article:tag, topics, tags)
            if not topics:
                topics_collected: List[str] = []
                for key in ['article:tag', 'topics', 'tags', 'taxonomy-tags', 'primaryTag']:
                    val = metadata.get(key)
                    if isinstance(val, list):
                        for v in val:
                            if isinstance(v, str) and v.strip():
                                # split comma-separated values
                                topics_collected.extend([x.strip() for x in v.split(',') if x.strip()])
                    elif isinstance(val, str) and val.strip():
                        topics_collected.extend([x.strip() for x in val.split(',') if x.strip()])
                # de-duplicate while preserving order
                seen = set()
                topics = [t for t in topics_collected if not (t in seen or seen.add(t))]

            # Derive categories from multiple sources: meta, breadcrumbs, URL
            categories_set: Set[str] = set()
            # 1) Meta-based sections
            for key in ['article:section', 'og:section', 'section', 'article:category', 'pageSection', 'primaryTopic', 'topics', 'where']:
                val = metadata.get(key)
                if isinstance(val, list):
                    for v in val:
                        if isinstance(v, str) and v.strip():
                            parts = [p.strip() for p in v.split(',') if p.strip()]
                            for p in parts:
                                categories_set.add(p)
                elif isinstance(val, str) and val.strip():
                    parts = [p.strip() for p in val.split(',') if p.strip()]
                    for p in parts:
                        categories_set.add(p)

            # 2) Breadcrumbs
            breadcrumb_selectors = [
                'nav[aria-label="breadcrumb"] a',
                'nav.breadcrumb a',
                'ol.breadcrumb li a',
                '.o-breadcrumb__list a',
                '.c-breadcrumb a',
                '#main-content-area nav a'
            ]
            for sel in breadcrumb_selectors:
                try:
                    for a in soup.select(sel):
                        text = (a.get_text() or '').strip()
                        if text and text not in {'Home', 'الرئيسية', 'الصفحة الرئيسية'}:
                            categories_set.add(text)
                except Exception:
                    continue

            # 3) URL-based inference (first 1-2 segments excluding slug)
            try:
                parsed_url = urlparse(url)
                path_parts = [p for p in parsed_url.path.strip('/').split('/') if p]
                if len(path_parts) >= 2:
                    # Exclude last part (slug)
                    inferred = path_parts[:-1][:2]
                    for seg in inferred:
                        if seg and not seg.isdigit():
                            categories_set.add(seg)
            except Exception:
                pass
            
            # Extract image URLs (already processed with full URLs) + meta/JSON-LD fallbacks
            image_urls = [img['url'] for img in images if img['url']]
            # Meta image candidates
            for k in ['og:image', 'twitter:image:src', 'image']:
                val = metadata.get(k)
                if isinstance(val, list):
                    for v in val:
                        if isinstance(v, str) and v.strip():
                            image_urls.append(v.strip())
                elif isinstance(val, str) and val.strip():
                    image_urls.append(val.strip())
            # JSON-LD images
            try:
                jsonld_source = metadata.get('jsonld')
                jsonld_candidates = jsonld_source if isinstance(jsonld_source, list) else [jsonld_source] if jsonld_source else []
                for block in jsonld_candidates:
                    if isinstance(block, dict) and 'image' in block:
                        img_block = block['image']
                        if isinstance(img_block, list):
                            for it in img_block:
                                if isinstance(it, dict) and isinstance(it.get('url'), str):
                                    image_urls.append(it['url'])
                                elif isinstance(it, str):
                                    image_urls.append(it)
                        elif isinstance(img_block, dict) and isinstance(img_block.get('url'), str):
                            image_urls.append(img_block['url'])
                        elif isinstance(img_block, str):
                            image_urls.append(img_block)
            except Exception:
                pass
            # Normalize relative image URLs
            normalized_images: List[str] = []
            for u in image_urls:
                if u and not u.startswith('http'):
                    if u.startswith('/'):
                        normalized_images.append(f"https://www.aljazeera.net{u}")
                    else:
                        normalized_images.append(f"https://www.aljazeera.net/{u}")
                else:
                    normalized_images.append(u)
            # De-duplicate while preserving order
            seen_imgs = set()
            image_urls = [u for u in normalized_images if u and not (u in seen_imgs or seen_imgs.add(u))]
            
            return {
                'url': url,
                'title': title,
                'topics': topics,
                'content': content_text,
                'content_html': str(article_html) if article_html else "",
                'images': images,
                'image_urls': image_urls,
                'metadata': metadata,
                'slug': slug,
                'published_at': published_at_dt,
                'tags': topics,  # Using topics as tags
                'categories': list(categories_set),
                'content_length': len(content_text)
                # Let Supabase generate the ID
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from {url}: {str(e)}")
            return {'url': url, 'error': f"Extraction failed: {str(e)}"}
            
    async def save_to_json(self, article_data: Dict[str, Any], sentences: List[Dict[str, Any]], words: List[Dict[str, Any]]) -> str:
        """Save article data, sentences, and words to a JSON file in the data directory"""
        # File writing disabled for now
        self.logger.info("File writing disabled; skipping JSON save")
        """
        try:
            # Original file-writing logic (disabled)
            complete_data = {
                "article": {
                    "url": article_data["url"],
                    "title": article_data["title"],
                    "metadata": article_data["metadata"],
                    "slug": article_data["slug"],
                    "published_at": article_data["published_at"],
                    "tags": article_data["tags"],
                    "categories": article_data["categories"],
                    "image_urls": article_data["image_urls"],
                    "content": article_data["content"],
                    "tashkil": article_data.get("tashkil", "")
                },
                "sentences": sentences,
                "words": words
            }
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            timestamp = int(time.time())
            filename = f"article_{article_data['slug']}_{timestamp}.json"
            filepath = data_dir / filename
            json_str = json.dumps(complete_data, indent=2, ensure_ascii=False)
            await asyncio.to_thread(filepath.write_text, json_str, 'utf-8')
            self.logger.info(f"Saved article data to {filepath}")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"Failed to save article data to JSON: {str(e)}")
            return ""
        """
        return ""
    
    async def store_in_supabase(self, article_data: Dict[str, Any], sentences: List[Dict[str, Any]], words: List[Dict[str, Any]]) -> bool:
        """Store article data, sentences, and words using direct Postgres connection (CONNECTION_STRING)."""
        if not self.db_enabled:
            self.logger.warning("Database storage is disabled. Skipping database storage.")
            return False
        try:
            pool = await self.get_db_pool()
            if not pool:
                return False
            async with pool.acquire() as conn:
                # Use a transaction to batch operations for speed and consistency
                async with conn.transaction():
                    # Store or update article, then get id
                    # Normalize complex types. If DB columns are arrays/jsonb, use native types; else JSON-encode.
                    metadata_val = article_data.get("metadata")
                    tags_val = article_data.get("tags")
                    categories_val = article_data.get("categories")
                    image_urls_val = article_data.get("image_urls")
                    # Detect if connection expects arrays for tags/categories/image_urls by trying to use them as arrays.
                    # We will pass lists directly; if DB expects text, it will error and be caught.
                    article_record = {
                        "url": article_data["url"],
                        "title": article_data["title"],
                        "metadata": metadata_val if isinstance(metadata_val, (dict, list)) else metadata_val,
                        "slug": article_data["slug"],
                        "published_at": article_data["published_at"],
                        "tags": tags_val if isinstance(tags_val, list) else ([] if tags_val is None else [str(tags_val)]),
                        "categories": categories_val if isinstance(categories_val, list) else ([] if categories_val is None else [str(categories_val)]),
                        "image_urls": image_urls_val if isinstance(image_urls_val, list) else ([] if image_urls_val is None else [str(image_urls_val)]),
                        "content": article_data["content"],
                        "tashkil": article_data.get("tashkil", "")
                    }
                    existing = await conn.fetchrow("SELECT id FROM articles WHERE url = $1", article_record["url"])
                    if existing:
                        article_id = existing["id"]
                        # Use explicit casts to jsonb/array where appropriate
                        await conn.execute(
                            "UPDATE articles SET title=$2, metadata=$3::jsonb, slug=$4, published_at=$5, tags=$6::text[], categories=$7::text[], image_urls=$8::text[], content=$9, tashkil=$10 WHERE id=$1",
                            article_id,
                            article_record["title"],
                            json.dumps(article_record["metadata"], ensure_ascii=False) if isinstance(article_record["metadata"], (dict, list)) else article_record["metadata"],
                            article_record["slug"],
                            article_record["published_at"],
                            article_record["tags"],
                            article_record["categories"],
                            article_record["image_urls"],
                            article_record["content"],
                            article_record["tashkil"],
                        )
                    else:
                        article_id = await conn.fetchval(
                            "INSERT INTO articles (url,title,metadata,slug,published_at,tags,categories,image_urls,content,tashkil) VALUES ($1,$2,$3::jsonb,$4,$5,$6::text[],$7::text[],$8::text[],$9,$10) RETURNING id",
                            article_record["url"],
                            article_record["title"],
                            json.dumps(article_record["metadata"], ensure_ascii=False) if isinstance(article_record["metadata"], (dict, list)) else article_record["metadata"],
                            article_record["slug"],
                            article_record["published_at"],
                            article_record["tags"],
                            article_record["categories"],
                            article_record["image_urls"],
                            article_record["content"],
                            article_record["tashkil"],
                        )
                    self.logger.info(f"Stored article with ID {article_id}: {article_data['title'][:30]}...")

                    # Insert sentences and collect their IDs
                    sentence_id_map: Dict[str, Any] = {}
                    for sentence in sentences:
                        temp_id = sentence.pop("id", None)
                        sid = await conn.fetchval(
                            "INSERT INTO sentences (article_id, text, tashkil) VALUES ($1,$2,$3) RETURNING id",
                            article_id,
                            sentence["text"],
                            sentence["tashkil"],
                        )
                        if temp_id is not None:
                            sentence_id_map[temp_id] = sid
                    self.logger.info(f"Stored {len(sentences)} sentences")

                    # Prepare and insert words
                    processed_words = []
                    for word in words:
                        temp_sentence_id = word.pop("sentence_id", None)
                        if temp_sentence_id in sentence_id_map:
                            sentence_id = sentence_id_map[temp_sentence_id]
                            w_id = self.generate_word_id(word["word"] + str(len(processed_words)))
                            processed_words.append((w_id, word["word"], word["word_without_ichkal"], sentence_id))

                    if processed_words:
                        await conn.executemany(
                            "INSERT INTO word_with_ichkal (id, word, word_without_ichkal, sentence_id) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                            processed_words,
                        )
                        self.logger.info(f"Stored {len(processed_words)} words")

                    return True
        except Exception as e:
            self.logger.error(f"Failed to store data in database: {str(e)}")
            return False
    
    async def scrape_article_batch(self, session: aiohttp.ClientSession, article_urls: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of articles with concurrency control"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_article(url):
            async with semaphore:
                # Deduplicate: skip if already present in Supabase or seen in this run
                slug = self.get_slug_from_url(url)
                if url in self.existing_article_urls or (slug and slug in self.existing_article_slugs):
                    self.stats['articles_skipped'] += 1
                    self.logger.info(f"⏭️ Skipping already-scraped article: {url}")
                    processed = {'url': url, 'skipped': True}
                    # Move optional delay outside the semaphore
                    if self.request_delay > 0:
                        await asyncio.sleep(self.request_delay + random.uniform(0, 1))
                    return processed
                else:
                    # Reserve this URL/slug to avoid duplicate processing within the same run
                    self.existing_article_urls.add(url)
                    if slug:
                        self.existing_article_slugs.add(slug)
                # Fetch article HTML
                result = await self.fetch_with_retry(session, url)
                
                if result and not result.get('error'):
                    # Extract structured data
                    article_data = self.extract_article_data(result['content'], url)
                    
                    if not article_data.get('error'):
                        self.stats['articles_scraped'] += 1
                        self.logger.info(f"✅ Extracted: {article_data.get('title', 'No title')[:50]}...")
                        
                        # Process content with tashkil
                        try:
                            # Add tashkil to content
                            tashkil_text, sentences, words = await self.process_tashkil(article_data['content'])
                            article_data['tashkil'] = tashkil_text
                            
                            # Store in Supabase (file saving disabled)
                            await self.store_in_supabase(article_data, sentences, words)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing tashkil: {str(e)}")
                    else:
                        self.stats['articles_failed'] += 1
                        self.logger.warning(f"❌ Failed extraction: {url} - {article_data.get('error')}")
                    processed = article_data
                else:
                    self.stats['articles_failed'] += 1
                    error_msg = result.get('error') if result else 'Unknown error'
                    self.logger.warning(f"❌ Failed fetch: {url} - {error_msg}")
                    processed = {'url': url, 'error': error_msg}
            # Move optional politeness delay outside the semaphore so slots are not blocked
            if self.request_delay > 0:
                await asyncio.sleep(self.request_delay + random.uniform(0, 1))
            return processed
        
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
        # Prefetch existing articles once to enable skipping already-scraped items
        await self.load_existing_articles()
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
        successful_articles = [a for a in all_articles if not a.get('error') and not a.get('skipped')]
        
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
    # Check for environment variables (warning only, not required)
    if not os.environ.get("CONNECTION_STRING"):
        print("\n\033[93mWarning: CONNECTION_STRING environment variable is not set.\033[0m")
        print("Database storage will be disabled. To enable it, set:")
        print("  export CONNECTION_STRING=postgresql://USER:PASSWORD@HOST:PORT/DB?sslmode=require")
        
    # Check for tashkil configuration
    tashkil_method = os.environ.get("TASHKIL_METHOD", "gemini").lower()
    api_key = os.environ.get("GEMINI_API_KEY")
    
    print(f"\n\033[94m=== Tashkil Configuration ===\033[0m")
    print(f"Method: {tashkil_method.upper()}")
    
    if tashkil_method == "gemini":
        if not api_key:
            print("\n\033[93mWarning: GEMINI_API_KEY environment variable is not set.\033[0m")
            print("Tashkil functionality will be disabled. To enable it, set:")
            print("  export GEMINI_API_KEY=your_gemini_api_key")
            print("Continuing without tashkil capability...\n")
        else:
            print(f"✓ Gemini API key configured")
            print(f"✓ Using Gemini 2.5 Flash Lite for Arabic diacritization")
    elif tashkil_method == "mishkal":
        print(f"✓ Using Mishkal library for Arabic diacritization")
        print("Note: Install Mishkal with: pip install mishkal pyarabic")
    else:
        print(f"\n\033[93mWarning: Unknown TASHKIL_METHOD: {tashkil_method}\033[0m")
        print("Valid options: 'gemini' or 'mishkal'")
        print("Defaulting to Gemini if API key available...\n")
    
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
        print(f"Articles skipped (already scraped): {stats.get('articles_skipped', 0)}")
        print(f"Sentences processed: {stats.get('sentences_processed', 0)}")
        print(f"Words processed: {stats.get('words_processed', 0)}")
        print(f"Processing time: {stats['processing_time']:.1f}s")
        print(f"Speed: {stats['articles_per_second']:.2f} articles/second")
        
        """
        File writing disabled below. Keeping logic commented for future use.
        # Create data directory if it doesn't exist
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        # Save results to file
        timestamp = int(time.time())
        output_file = data_dir / f"aljazeera_articles_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Results saved to: {output_file}")
        # Save just successful articles to separate file
        successful_articles = [a for a in results['articles'] if not a.get('error')]
        if successful_articles:
            clean_file = data_dir / f"aljazeera_clean_{timestamp}.json"
            with open(clean_file, 'w', encoding='utf-8') as f:
                json.dump(successful_articles, f, indent=2, ensure_ascii=False)
            print(f"💾 Clean articles saved to: {clean_file}")
        """
            
        print(f"\n💾 Data stored in Supabase database according to schema")
        
    except KeyboardInterrupt:
        print("\n⏹️ Scraping interrupted by user")
        print(f"Stats so far: {scraper.stats}")
    except Exception as e:
        print(f"\n💥 Scraping failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Use uvloop if available for faster event loop
    try:
        import uvloop  # type: ignore
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass
    asyncio.run(main())