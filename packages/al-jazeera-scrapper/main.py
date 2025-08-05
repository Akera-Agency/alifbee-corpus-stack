from scraper import AlJazeeraScraper

scraper = AlJazeeraScraper()

# Full workflow
async def main():
    result = await scraper.scrape_all_sitemaps(limit_sitemaps=10)
    print(result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())