import logging
import asyncio
from urllib.parse import urljoin
from parsel import Selector
from motor.motor_asyncio import AsyncIOMotorClient

from utils.config import settings
from utils.database import get_database, setup_database_indexes
from utils.logging import setup_logging
from .spider import Spider, BASE_URL
from .parser import parse_book_page, parse_list_page

log = logging.getLogger(__name__)

async def process_book_page(spider: Spider, url: str):
    """
    Worker task to fetch, parse, and save a single book detail page.
    """
    try:
        full_url = url
        
        response = await spider.fetch_page(full_url)
        book = parse_book_page(response.text, full_url)
        
        if book:
            book_data = book.model_dump(
                mode="json", 
                by_alias=True, 
                exclude={'id'}
            )
            
            await spider.db.books.update_one(
                {"upc": book.upc},
                {"$set": book_data},
                upsert=True
            )
            log.info(f"Successfully saved book: {book.name}")
            
    except Exception as e:
        log.error(f"Failed to process book {url}: {e}")

async def run_crawler():
    """Main entrypoint for the crawler service"""
    # Setup
    db_client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = get_database(db_client)
    await setup_database_indexes(db)
    spider = Spider(db)

    # Resume from failure
    log.info("Fetching existing URLs to resume crawl...")
    existing_urls = {
        doc["source_url"] async for doc in db.books.find({}, {"source_url": 1})
    }
    log.info(f"Found {len(existing_urls)} existing books in database.")

    # Stage 1 crawl (discover all books)

    log.info("Starting stage 1: discover books")
    await spider.crawl_queue.put("index.html")
    pages_crawled = 0
    
    while not spider.crawl_queue.empty():
        page_url = await spider.crawl_queue.get()
        try:
            response = await spider.fetch_page(page_url)
            book_urls, next_page_url = parse_list_page(Selector(response.text))
            
            # Resolve the relative book URLs against the current page's URL to make them absolute.
            for rel_url in book_urls:
                abs_url = urljoin(str(response.url), rel_url)
                spider.book_urls_to_crawl.add(abs_url)
                
            if next_page_url:
                # Resolve the next page URL
                next_page_full_path = urljoin(str(response.url), next_page_url)
                await spider.crawl_queue.put(next_page_full_path)
            
            pages_crawled += 1
            log.info(f"Crawled list page {pages_crawled}. Found {len(book_urls)} books.")
        except Exception as e:
            log.error(f"Failed to crawl list page {page_url}: {e}")
            
    log.info(f"Stage 1 complete. Discovered {len(spider.book_urls_to_crawl)} total books.")

    # Stage 2 crawl: extract book data
    
    # Find books not in the existing_urls set
    urls_to_crawl = list(spider.book_urls_to_crawl - existing_urls)
    log.info(f"Starting stage 2: need to crawl {len(urls_to_crawl)} new books.")

    tasks = [process_book_page(spider, url) for url in urls_to_crawl]
    await asyncio.gather(*tasks)

    log.info("Crawl complete.")
    
    # Cleanup
    await spider.close()
    db_client.close()

if __name__ == "__main__":
    setup_logging()
    asyncio.run(run_crawler())