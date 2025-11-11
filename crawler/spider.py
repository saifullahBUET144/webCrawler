import logging
import asyncio
from typing import Optional, Set
import httpx
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type
)
from urllib.parse import urljoin

log = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES: Set[int] = {500, 502, 503, 504}
BASE_URL = "https://books.toscrape.com/"

class Spider:
    def __init__(self, db, concurrency: int = 20):
        self.db = db
        
        # Transport layer retry
        # Handles low-level connection errors/timeouts
        self.transport = httpx.AsyncHTTPTransport(retries=3)
        
        self.client = httpx.AsyncClient(
            transport=self.transport,
            base_url=BASE_URL,
            timeout=15.0,
            follow_redirects=True,
            headers={"User-Agent": "FilersKeepers-Crawler/1.0"}
        )
        
        # Concurrency limiting
        self.semaphore = asyncio.Semaphore(concurrency)
        
        # State for the crawl
        self.crawl_queue = asyncio.Queue()
        self.book_urls_to_crawl = set()

    # Application layer retry
    # Handles specific HTTP status codes
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True
    )
    async def fetch_page(self, url: str) -> httpx.Response:
        """Fetches a single page with concurrency limiting and retries."""
        async with self.semaphore:
            log.debug(f"Fetching: {url}")
            response = await self.client.get(url)
            
            # Raise an exception to trigger the tenacity @retry decorator
            if response.status_code in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
                
            return response

    async def close(self):
        """Closes the HTTPO client."""
        await self.client.aclose()