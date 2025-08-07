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
from typing import List, Optional, Dict, Any, Set, Tuple
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

import aiohttp
import xmltodict
from bs4 import BeautifulSoup

# Import TashkilUnified from tashkil package
sys.path.append(str(Path(__file__).parent.parent))
from tashkil.tashkil_unified import TashkilUnified

from supabase import create_client, Client

# Initialize Supabase client - will be set up in __init__ if environment variables are present
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = None  # Will be initialized in the class if URL and key are available

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
        
        # Initialize Supabase
        global supabase
        if url and key:
            try:
                supabase = create_client(url, key)
                self.logger.info("Supabase client initialized successfully")
                self.supabase_enabled = True
            except Exception as e:
                self.logger.error(f"Failed to initialize Supabase client: {str(e)}")
                self.supabase_enabled = False
        else:
            self.logger.warning("SUPABASE_URL or SUPABASE_KEY not found. Database storage will be disabled.")
            self.supabase_enabled = False
        
        # Initialize TashkilUnified
        api_key = os.environ.get("GEMINI_API_KEY")
        tashkil_method = os.environ.get("TASHKIL_METHOD", "gemini").lower()
        
        if tashkil_method == "mishkal":
            try:
                self.tashkil_ai = TashkilUnified(method="mishkal")
                self.logger.info("Initialized TashkilUnified with Mishkal method")
            except Exception as e:
                self.logger.warning(f"Failed to initialize Mishkal: {str(e)}. Falling back to Gemini.")
                if api_key:
                    self.tashkil_ai = TashkilUnified(method="gemini", api_key=api_key)
                    self.logger.info("Initialized TashkilUnified with Gemini method (fallback)")
                else:
                    self.logger.warning("GEMINI_API_KEY not found. Tashkil functionality will be disabled.")
                    self.tashkil_ai = None
        elif tashkil_method == "gemini":
            if not api_key:
                self.logger.warning("GEMINI_API_KEY not found. Tashkil functionality will be disabled.")
                self.tashkil_ai = None
            else:
                self.tashkil_ai = TashkilUnified(method="gemini", api_key=api_key)
                self.logger.info("Initialized TashkilUnified with Gemini method")
        else:
            self.logger.warning(f"Unknown TASHKIL_METHOD: {tashkil_method}. Using Gemini if API key available.")
            if api_key:
                self.tashkil_ai = TashkilUnified(method="gemini", api_key=api_key)
                self.logger.info("Initialized TashkilUnified with Gemini method")
            else:
                self.tashkil_ai = None
        
        # Stats tracking
        self.stats = {
            'sitemaps_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_failed': 0,
            'sentences_processed': 0,
            'words_processed': 0
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
            
    def split_into_sentences(self, text: str) -> List[str]:
        """Split Arabic text into sentences"""
        if not text:
            return []
            
        # Arabic sentence endings: period, question mark, exclamation mark
        sentence_endings = r'[.!؟\?]'
        
        # Split by sentence endings followed by space or end of string
        sentences = re.split(f'({sentence_endings}\\s|{sentence_endings}$)', text)
        
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
        words = [word.strip() for word in re.split(r'\s+', text) if word.strip()]
        
        # Remove punctuation from words and filter out non-Arabic words
        clean_words = []
        for word in words:
            # Remove punctuation at start/end of word
            clean_word = re.sub(r'^[\W]+|[\W]+$', '', word)
            
            # Skip words containing English letters or numbers
            if re.search(r'[a-zA-Z0-9]', clean_word):
                continue
                
            # Only include non-empty words
            if clean_word:
                clean_words.append(clean_word)
                
        return clean_words
        
    def extract_word_without_tashkil(self, word_with_tashkil: str) -> str:
        """Remove tashkil (diacritics) from Arabic word"""
        # Arabic diacritics Unicode ranges
        arabic_diacritics = re.compile(r'[\u064B-\u065F\u0670]')
        return arabic_diacritics.sub('', word_with_tashkil)
        
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
        
    def clean_tashkil_output(self, text: str) -> str:
        """Clean up the tashkil output by removing AI instructions and XML tags"""
        # Remove any XML/HTML-like tags that might be in the response
        text = re.sub(r'<task>.*?</task>', '', text, flags=re.DOTALL)
        text = re.sub(r'<instructions>.*?</instructions>', '', text, flags=re.DOTALL)
        text = re.sub(r'<article_input>.*?</article_input>', '', text, flags=re.DOTALL)
        
        # Remove any remaining XML/HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove lines that might contain AI instructions
        lines_to_remove = [
            "You are an expert in Arabic diacritization",
            "Your task is to add accurate",
            "Please follow these rules",
            "Do not modify the words",
            "Preserve the original structure",
            "Add diacritics to every word",
            "Here is the text with diacritics:",
            "Text with diacritics:"
        ]
        
        for line in lines_to_remove:
            text = re.sub(r'.*' + re.escape(line) + r'.*\n?', '', text, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def filter_non_arabic(self, text: str) -> str:
        """Filter out numbers and English letters from text"""
        # Replace English letters and numbers with spaces
        text = re.sub(r'[a-zA-Z0-9]+', ' ', text)
        
        # Clean up extra whitespace created by replacements
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    async def process_tashkil(self, text: str) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process text through TashkilAI and prepare database records"""
        # Process the entire article at once with TashkilAI
        full_tashkil_text = ""
        if self.tashkil_ai:
            try:
                self.logger.info(f"Adding tashkil to full article ({len(text)} chars)...")
                raw_tashkil_output = self.tashkil_ai.tashkeel(text)
                # Clean up the output to remove any AI instructions
                full_tashkil_text = self.clean_tashkil_output(raw_tashkil_output)
                self.logger.info(f"Successfully added tashkil to full article")
            except Exception as e:
                self.logger.error(f"Error adding tashkil to full article: {str(e)}")
                full_tashkil_text = text  # Fallback to original
        else:
            full_tashkil_text = text  # No TashkilAI available
            
        # Note: We don't filter the full article text here to preserve it for JSON storage
        # Filtering will be applied to individual sentences
            
        # Split original and tashkil text into sentences
        original_sentences = self.split_into_sentences(text)
        tashkil_sentences = self.split_into_sentences(full_tashkil_text)
        
        # Make sure we have the same number of sentences
        # If not, fall back to original sentences
        if len(original_sentences) != len(tashkil_sentences):
            self.logger.warning(f"Sentence count mismatch: original={len(original_sentences)}, tashkil={len(tashkil_sentences)}")
            self.logger.warning("Using original sentences as fallback")
            tashkil_sentences = original_sentences
            
        self.stats['sentences_processed'] += len(original_sentences)
        
        # Process sentences and words
        sentence_records = []
        word_records = []
        
        # Process each sentence
        for i, (original_sentence, tashkil_sentence) in enumerate(zip(original_sentences, tashkil_sentences)):
            # Use index as temporary ID for reference
            temp_sentence_id = str(i)  # We'll let Supabase generate the real IDs
            
            # Filter out non-Arabic characters from sentences
            filtered_original = self.filter_non_arabic(original_sentence)
            filtered_tashkil = self.filter_non_arabic(tashkil_sentence)
            
            # Create sentence record (without article_id, will be set later)
            sentence_records.append({
                "id": temp_sentence_id,  # Temporary ID for reference
                "text": filtered_original,
                "tashkil": filtered_tashkil
            })
            
            # Process words in the sentence with tashkil
            words_with_tashkil = self.extract_words(tashkil_sentence)
            self.stats['words_processed'] += len(words_with_tashkil)
            
            for word_with_tashkil in words_with_tashkil:
                # Extract word without tashkil
                word_without_tashkil = self.extract_word_without_tashkil(word_with_tashkil)
                
                # Create word record (without real sentence_id, will be set later)
                word_records.append({
                    "word": word_with_tashkil,
                    "word_without_ichkal": word_without_tashkil,
                    "sentence_id": temp_sentence_id  # Temporary ID for reference
                })
        
        return full_tashkil_text, sentence_records, word_records
    
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
            
            # Generate slug from URL
            parsed_url = urlparse(url)
            slug = parsed_url.path.strip('/').split('/')[-1]
            
            # Extract published date from metadata
            published_at = metadata.get('article:published_time', None)
            
            # Extract image URLs (already processed with full URLs)
            image_urls = [img['url'] for img in images if img['url']]
            
            return {
                'url': url,
                'title': title,
                'topics': topics,
                'content': content_text,
                'content_html': article_html,
                'images': images,
                'image_urls': image_urls,
                'metadata': metadata,
                'slug': slug,
                'published_at': published_at,
                'tags': topics,  # Using topics as tags
                'categories': [],  # No specific category extraction
                'content_length': len(content_text)
                # Let Supabase generate the ID
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from {url}: {str(e)}")
            return {'url': url, 'error': f"Extraction failed: {str(e)}"}
            
    async def save_to_json(self, article_data: Dict[str, Any], sentences: List[Dict[str, Any]], words: List[Dict[str, Any]]) -> str:
        """Save article data, sentences, and words to a JSON file in the data directory"""
        try:
            # Create a complete data structure
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
            
            # Create data directory if it doesn't exist
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            # Create filename based on slug and timestamp
            timestamp = int(time.time())
            filename = f"article_{article_data['slug']}_{timestamp}.json"
            filepath = data_dir / filename
            
            # Write to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(complete_data, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Saved article data to {filepath}")
            return str(filepath)
            
        except Exception as e:
            self.logger.error(f"Failed to save article data to JSON: {str(e)}")
            return ""
    
    async def store_in_supabase(self, article_data: Dict[str, Any], sentences: List[Dict[str, Any]], words: List[Dict[str, Any]]) -> bool:
        """Store article data, sentences, and words in Supabase"""
        if not self.supabase_enabled or not supabase:
            self.logger.warning("Supabase storage is disabled. Skipping database storage.")
            return False
            
        try:
            # Store article (let Supabase generate the ID)
            article_record = {
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
            }
            
            # Insert article and get the generated ID
            article_result = supabase.table("articles").upsert(article_record).execute()
            
            # Extract the generated article ID
            if article_result and article_result.data and len(article_result.data) > 0:
                article_id = article_result.data[0].get('id')
                self.logger.info(f"Stored article in Supabase with ID {article_id}: {article_data['title'][:30]}...")
                
                # Create a mapping from temp IDs to sentences
                temp_id_to_sentence = {}
                for sentence in sentences:
                    temp_id = sentence.pop("id", None)  # Remove our temporary ID
                    sentence["article_id"] = article_id  # Set the actual article ID
                    temp_id_to_sentence[temp_id] = sentence
                
                # Insert sentences in batches
                batch_size = 50
                sentence_batches = []
                for i in range(0, len(sentences), batch_size):
                    sentence_batches.append(sentences[i:i + batch_size])
                
                # Track inserted sentences and their IDs
                sentence_id_map = {}  # Maps temp_id to real UUID
                
                # Insert each batch of sentences
                for batch in sentence_batches:
                    sentences_result = supabase.table("sentences").upsert(batch).execute()
                    
                    if sentences_result and sentences_result.data:
                        # Match the returned sentences with our original ones by position
                        for i, sentence_record in enumerate(sentences_result.data):
                            if i < len(batch):
                                # Find the temp_id for this sentence
                                for temp_id, sent in temp_id_to_sentence.items():
                                    if sent is batch[i]:  # Compare by reference
                                        sentence_id_map[temp_id] = sentence_record.get('id')
                                        break
                    
                    self.logger.info(f"Stored {len(batch)} sentences in Supabase")
                
                # Update word records with real sentence IDs and save all words (including duplicates)
                processed_words = []
                for word in words:
                    temp_sentence_id = word.pop("sentence_id", None)
                    if temp_sentence_id in sentence_id_map:
                        word["sentence_id"] = sentence_id_map[temp_sentence_id]
                        
                        # Generate a unique ID for each word instance (including duplicates)
                        word["id"] = self.generate_word_id(word["word"] + str(len(processed_words)))
                        
                        processed_words.append(word)
                
                self.logger.info(f"Processing {len(processed_words)} words (including duplicates)")
                
                # Insert words in batches
                for i in range(0, len(processed_words), batch_size):
                    batch = [w for w in processed_words[i:i + batch_size] if "sentence_id" in w]  # Only include words with valid sentence_id
                    if batch:  # Only insert if there are valid words
                        try:
                            words_result = supabase.table("word_with_ichkal").upsert(
                                batch,
                            ).execute()
                            self.logger.info(f"Stored {len(batch)} words in Supabase")
                        except Exception as word_error:
                            self.logger.warning(f"Error storing words batch: {str(word_error)}")
                            # Try inserting one by one to salvage what we can
                            for single_word in batch:
                                try:
                                    supabase.table("word_with_ichkal").upsert(
                                        [single_word],
                                    ).execute()
                                except Exception:
                                    # Skip this word if it fails
                                    pass
                
                return True
            else:
                self.logger.error("Failed to get article ID from Supabase response")
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to store data in Supabase: {str(e)}")
            return False
    
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
                        
                        # Process content with tashkil
                        try:
                            # Add tashkil to content
                            tashkil_text, sentences, words = await self.process_tashkil(article_data['content'])
                            article_data['tashkil'] = tashkil_text
                            
                            # First save to JSON file
                            json_file = await self.save_to_json(article_data, sentences, words)
                            
                            # Then store in Supabase if JSON save was successful
                            if json_file:
                                await self.store_in_supabase(article_data, sentences, words)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing tashkil: {str(e)}")
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
    # Check for environment variables (warning only, not required)
    if not url or not key:
        print("\n\033[93mWarning: SUPABASE_URL and/or SUPABASE_KEY environment variables are not set.\033[0m")
        print("Database storage will be disabled. To enable it, set:")
        print("  export SUPABASE_URL=your_supabase_url")
        print("  export SUPABASE_KEY=your_supabase_key")
        
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
        print(f"Sentences processed: {stats.get('sentences_processed', 0)}")
        print(f"Words processed: {stats.get('words_processed', 0)}")
        print(f"Processing time: {stats['processing_time']:.1f}s")
        print(f"Speed: {stats['articles_per_second']:.2f} articles/second")
        
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
    asyncio.run(main())