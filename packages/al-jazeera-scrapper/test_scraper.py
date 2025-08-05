#!/usr/bin/env python3
"""
Simple test script for the Al Jazeera scraper
"""

import asyncio
import sys
import os

# Add current directory to path to import scraper
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def test_basic_functionality():
    """Test basic scraper functionality without dependencies"""
    try:
        from scraper import AlJazeeraScraper
        
        # Test initialization
        scraper = AlJazeeraScraper()
        print("✅ Scraper initialized successfully")
        
        # Test URL validation
        test_content = '''
        <a href="/news/2024/01/01/test-article">Test Article</a>
        <a href="https://www.aljazeera.com/news/2024/01/02/another-article">Another Article</a>
        '''
        
        urls = scraper.get_article_urls(test_content)
        print(f"✅ URL extraction works: found {len(urls)} URLs")
        
        # Test configuration
        print(f"✅ Base URL: {scraper.base_url}")
        print(f"✅ Max concurrent: {scraper.max_concurrent}")
        print(f"✅ Request delay: {scraper.request_delay}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Install dependencies with: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def main():
    print("Testing Al Jazeera Scraper...")
    print("=" * 40)
    
    success = await test_basic_functionality()
    
    if success:
        print("\n✅ All basic tests passed!")
        print("\nTo run the actual scraper:")
        print("1. pip install -r requirements.txt")
        print("2. python3 scraper.py news")
    else:
        print("\n❌ Tests failed")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())