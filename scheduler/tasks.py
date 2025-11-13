import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from parsel import Selector
from urllib.parse import urljoin
from datetime import datetime, timezone
from utils.config import settings
from utils.database import get_database
from utils.email import send_alert_email 

from crawler.spider import Spider
from crawler.parser import (
    Book, ChangeLogEntry, parse_book_page, 
    parse_list_page
)

log = logging.getLogger(__name__)


async def log_changes(db: AsyncIOMotorDatabase, old_data: dict, new_data: Book):
    """
    Logs the differences between an old book record and a new one.
    """
    changes = []
    
    def check_field(field_name, old_val, new_val):
        if str(old_val) != str(new_val):
            changes.append(ChangeLogEntry(
                book_upc=new_data.upc,
                field_changed=field_name,
                old_value=str(old_val),
                new_value=str(new_val)
            ))

    check_field("name", old_data.get("name"), new_data.name)
    check_field("description", old_data.get("description"), new_data.description)
    check_field("category", old_data.get("category"), new_data.category)
    check_field("price_incl_tax", old_data.get("price_incl_tax"), new_data.price_incl_tax)
    check_field("price_excl_tax", old_data.get("price_excl_tax"), new_data.price_excl_tax)
    check_field("availability", old_data.get("availability"), new_data.availability)
    check_field("num_reviews", old_data.get("num_reviews"), new_data.num_reviews)
    check_field("rating", old_data.get("rating"), new_data.rating)
    check_field("image_url", old_data.get("image_url"), new_data.image_url)
        
    if changes:
        # This uses the fix from our previous conversation
        log_models = [
            c.model_dump(by_alias=True, exclude={'id'}) for c in changes
        ]
        
        await db.change_log.insert_many(log_models)
        log.info(f"Logged {len(changes)} changes for UPC {new_data.upc}.")

async def check_book_update(spider: Spider, db: AsyncIOMotorDatabase, old_book: dict) -> bool:
    """
    Worker task to fetch, parse, and update a single existing book.
    Returns True if a change was detected, False otherwise.
    """
    try:
        response = await spider.fetch_page(old_book["source_url"])
        new_book = parse_book_page(response.text, old_book["source_url"])
        
        if not new_book:
            log.warning(f"Failed to parse {old_book['source_url']}, skipping update.")
            return False # Return False: no change
        
        if new_book.data_fingerprint != old_book["data_fingerprint"]:
            log.warning(f"Change detected for UPC: {old_book['upc']}")
            
            await log_changes(db, old_book, new_book)
            
            await db.books.update_one(
                {"upc": new_book.upc},
                {"$set": {
                    "name": new_book.name,
                    "description": new_book.description,
                    "category": new_book.category,
                    "price_incl_tax": new_book.price_incl_tax,
                    "price_excl_tax": new_book.price_excl_tax,
                    "availability": new_book.availability,
                    "num_reviews": new_book.num_reviews,
                    "rating": new_book.rating,
                    "image_url": str(new_book.image_url),
                    "data_fingerprint": new_book.data_fingerprint,
                    "crawl_timestamp": new_book.crawl_timestamp,
                    "raw_html_snapshot": new_book.raw_html_snapshot,
                    "crawl_status": new_book.crawl_status
                }}
            )
            return True # Return True: change detected and updated
        
        return False # Return False: no change
                    
    except Exception as e:
        log.error(f"Error checking book UPC {old_book['upc']}", exc_info=True)
        return False # Return False: error


async def run_daily_change_detection():
    log.info("Starting daily change detection job...")
    db_client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = get_database(db_client)
    spider = Spider(db)
    
    # Stage 1: Discover all books
    log.info("Running discovery phase to find all current books...")
    all_current_book_urls = set()
    crawl_queue = asyncio.Queue()
    await crawl_queue.put("index.html")
    
    while not crawl_queue.empty():
        page_url = await crawl_queue.get()
        try:
            response = await spider.fetch_page(page_url)
            book_urls, next_page_url = parse_list_page(Selector(response.text))
            
            for rel_url in book_urls:
                abs_url = urljoin(str(response.url), rel_url)
                all_current_book_urls.add(abs_url)
                
            if next_page_url:
                next_page_full_path = urljoin(str(response.url), next_page_url)
                await crawl_queue.put(next_page_full_path)
        except Exception as e:
            log.error(f"Failed to crawl list page {page_url} during change detection: {e}", exc_info=True)
            
    log.info(f"Discovery complete. Found {len(all_current_book_urls)} books on site.")

    # Stage 2: Compare and Update
    log.info("Running update phase to check for changes...")
    
    known_books = {
        doc["source_url"]: doc 
        async for doc in db.books.find({}, {
            "upc": 1, "source_url": 1, "data_fingerprint": 1,
            "name": 1, "description": 1, "category": 1,
            "price_incl_tax": 1, "price_excl_tax": 1, 
            "availability": 1, "num_reviews": 1,
            "rating": 1, "image_url": 1
        })
    }
    log.info(f"Found {len(known_books)} books in database.")

    # Find new books
    new_book_urls = all_current_book_urls - known_books.keys()
    num_new_books = len(new_book_urls) # Store the count
    
    if new_book_urls:
        log.info(f"Found {num_new_books} new books. Crawling them...")
        for url in new_book_urls:
            try:
                response = await spider.fetch_page(url)
                book = parse_book_page(response.text, url)
                if book:
                    book_data = book.model_dump(
                        mode="json", 
                        by_alias=True, 
                        exclude={'id'}
                    )
                    await db.books.insert_one(book_data)
                    log.info(f"Saved new book: {book.name}")

                    # log as a "new book" change
                    new_book_log = ChangeLogEntry(
                        book_upc=book.upc,
                        field_changed="book_status", # Using a special field name
                        old_value="non-existent",
                        new_value="added"
                    )
                    await db.change_log.insert_one(
                        new_book_log.model_dump(by_alias=True, exclude={'id'})
                    )

            except Exception as e:
                log.error(f"Failed to save new book {url}: {e}", exc_info=True)

    # Check existing books for updates
    books_to_check = known_books.values()
    log.info(f"Checking {len(books_to_check)} existing books for updates...")
    
    update_tasks = [
        check_book_update(spider, db, old_book) 
        for old_book in books_to_check
    ]
    # Capture the results (True/False) from the update tasks
    update_results = await asyncio.gather(*update_tasks)
    
    # Count how many tasks returned True
    num_updated_books = sum(1 for res in update_results if res is True)
        
    if num_new_books > 0 or num_updated_books > 0:
        log.info(f"Changes detected. Sending email alert...")
        subject = f"Book Crawler Report - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        
        body_html = f"<h2>Daily Web Crawler Report</h2>"
        body_html += f"<p>The daily change detection job has completed successfully.</p>"
        body_html += f"<ul>"
        body_html += f"<li><b>New books found:</b> {num_new_books}</li>"
        body_html += f"<li><b>Existing books updated:</b> {num_updated_books}</li>"
        body_html += f"</ul>"
        body_html += f"<p>Check the API's <code>/changes/report</code> endpoint for a detailed list of changes.</p>"
        
        # Asynchronously call the email function
        await send_alert_email(subject, body_html)
    else:
        log.info("No changes detected. No email alert will be sent.")
                                    
    await spider.close()
    db_client.close()
    log.info("Daily change detection job complete.")