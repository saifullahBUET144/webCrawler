import pytest
from datetime import datetime
from scheduler.tasks import log_changes, check_book_update, run_daily_change_detection
from crawler.parser import Book
from crawler.spider import Spider
from pytest_httpx import HTTPXMock


# Sample HTML for testing
BOOK_HTML_V1 = """
<!DOCTYPE html>
<html>
<body>
    <ul class="breadcrumb">
        <li><a href="../index.html">Home</a></li>
        <li><a href="../category/books_1/index.html">Books</a></li>
        <li><a href="../category/books/fiction_10/index.html">Fiction</a></li>
    </ul>
    <div class="product_main">
        <h1>Test Book</h1>
        <p class="star-rating Three"></p>
    </div>
    <div id="product_description"></div>
    <p>Original description of the book.</p>
    <table class="table table-striped">
        <tr><th>UPC</th><td>test-upc-123</td></tr>
        <tr><th>Price (excl. tax)</th><td>£19.99</td></tr>
        <tr><th>Price (incl. tax)</th><td>£19.99</td></tr>
        <tr><th>Availability</th><td>In stock (10 available)</td></tr>
        <tr><th>Number of reviews</th><td>5</td></tr>
    </table>
    <div class="item active">
        <img src="../../media/test.jpg" alt="Test Book">
    </div>
</body>
</html>
"""

BOOK_HTML_V2_PRICE_CHANGE = """
<!DOCTYPE html>
<html>
<body>
    <ul class="breadcrumb">
        <li><a href="../index.html">Home</a></li>
        <li><a href="../category/books_1/index.html">Books</a></li>
        <li><a href="../category/books/fiction_10/index.html">Fiction</a></li>
    </ul>
    <div class="product_main">
        <h1>Test Book</h1>
        <p class="star-rating Three"></p>
    </div>
    <div id="product_description"></div>
    <p>Original description of the book.</p>
    <table class="table table-striped">
        <tr><th>UPC</th><td>test-upc-123</td></tr>
        <tr><th>Price (excl. tax)</th><td>£24.99</td></tr>
        <tr><th>Price (incl. tax)</th><td>£24.99</td></tr>
        <tr><th>Availability</th><td>In stock (10 available)</td></tr>
        <tr><th>Number of reviews</th><td>5</td></tr>
    </table>
    <div class="item active">
        <img src="../../media/test.jpg" alt="Test Book">
    </div>
</body>
</html>
"""

BOOK_HTML_V3_AVAILABILITY_CHANGE = """
<!DOCTYPE html>
<html>
<body>
    <ul class="breadcrumb">
        <li><a href="../index.html">Home</a></li>
        <li><a href="../category/books_1/index.html">Books</a></li>
        <li><a href="../category/books/fiction_10/index.html">Fiction</a></li>
    </ul>
    <div class="product_main">
        <h1>Test Book</h1>
        <p class="star-rating Three"></p>
    </div>
    <div id="product_description"></div>
    <p>Original description of the book.</p>
    <table class="table table-striped">
        <tr><th>UPC</th><td>test-upc-123</td></tr>
        <tr><th>Price (excl. tax)</th><td>£19.99</td></tr>
        <tr><th>Price (incl. tax)</th><td>£19.99</td></tr>
        <tr><th>Availability</th><td>Out of stock</td></tr>
        <tr><th>Number of reviews</th><td>5</td></tr>
    </table>
    <div class="item active">
        <img src="../../media/test.jpg" alt="Test Book">
    </div>
</body>
</html>
"""

LIST_PAGE_HTML = """
<!DOCTYPE html>
<html>
<body>
    <section>
        <article class="product_pod">
            <h3><a href="catalogue/test-book_001/index.html">Test Book</a></h3>
        </article>
    </section>
</body>
</html>
"""


class TestLogChanges:
    """Test the log_changes function."""
    
    @pytest.mark.asyncio
    async def test_log_price_change(self, test_db):
        """Test logging a price change."""
        old_data = {
            "upc": "test-123",
            "name": "Test Book",
            "description": None, 
            "category": "Fiction",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99, 
            "availability": "In stock",
            "num_reviews": 5,
            "rating": 4,
            "image_url": "https://example.com/image.jpg"
        }
        
        new_book = Book(
            upc="test-123",
            name="Test Book",
            category="Fiction",
            price_incl_tax=24.99,
            price_excl_tax=23.99,
            availability="In stock",
            num_reviews=5,
            rating=4,
            image_url="https://example.com/image.jpg",
            source_url="https://example.com/book",
            data_fingerprint="new-fingerprint",
            crawl_status="successful"
        )
        
        await log_changes(test_db, old_data, new_book)
        
        # Verify change was logged
        changes = await test_db.change_log.find({"book_upc": "test-123"}).to_list(None)
        assert len(changes) == 2  # Both incl and excl tax changed
        
        # Check price_incl_tax change
        price_change = next(c for c in changes if c["field_changed"] == "price_incl_tax")
        assert price_change["old_value"] == "19.99"
        assert price_change["new_value"] == "24.99"
    
    @pytest.mark.asyncio
    async def test_log_availability_change(self, test_db):
        """Test logging an availability change."""
        old_data = {
            "upc": "test-456",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99,
            "availability": "In stock", 
            "name": "Test Book",
            "description": None, 
            "category": "Fiction",
            "num_reviews": 5,
            "rating": 4,
            "image_url": "https://example.com/image.jpg"
        }
                
        new_book = Book(
            upc="test-456",
            name="Test Book",
            category="Fiction",
            price_incl_tax=19.99,
            price_excl_tax=18.99,
            availability="Out of stock",
            num_reviews=5,
            rating=4,
            image_url="https://example.com/image.jpg",
            source_url="https://example.com/book",
            data_fingerprint="new-fingerprint",
            crawl_status="successful"
        )
        
        await log_changes(test_db, old_data, new_book)
        
        # Verify change was logged
        changes = await test_db.change_log.find({"book_upc": "test-456"}).to_list(None)
        assert len(changes) == 1
        assert changes[0]["field_changed"] == "availability"
        assert changes[0]["old_value"] == "In stock"
        assert changes[0]["new_value"] == "Out of stock"
    
    @pytest.mark.asyncio
    async def test_log_multiple_changes(self, test_db):
        """Test logging multiple simultaneous changes."""
        old_data = {
            "upc": "test-789",
            "name": "Old Name",
            "description": "Old description",
            "category": "Fiction",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99,
            "availability": "In stock",
            "num_reviews": 5,
            "rating": 3,
            "image_url": "https://example.com/old.jpg"
        }
        
        new_book = Book(
            upc="test-789",
            name="New Name",
            category="Fiction",
            description="New description",
            price_incl_tax=24.99,
            price_excl_tax=23.99,
            availability="Out of stock",
            num_reviews=10,
            rating=4,
            image_url="https://example.com/new.jpg",
            source_url="https://example.com/book",
            data_fingerprint="new-fingerprint",
            crawl_status="successful"
        )
        
        await log_changes(test_db, old_data, new_book)
        
        # Verify all changes were logged
        changes = await test_db.change_log.find({"book_upc": "test-789"}).to_list(None)
        assert len(changes) > 0
        
        # Check that multiple fields were logged
        changed_fields = {c["field_changed"] for c in changes}
        assert "name" in changed_fields
        assert "price_incl_tax" in changed_fields
        assert "availability" in changed_fields
        assert "rating" in changed_fields
    
    @pytest.mark.asyncio
    async def test_log_no_changes(self, test_db):
        """Test that no logs are created when nothing changed."""
        old_data = {
            "upc": "test-000",
            "name": "Same Book",
            "description": "Same description",
            "category": "Fiction",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99,
            "availability": "In stock",
            "num_reviews": 5,
            "rating": 4,
            "image_url": "https://example.com/image.jpg"
        }
        
        new_book = Book(
            upc="test-000",
            name="Same Book",
            description="Same description",
            category="Fiction",
            price_incl_tax=19.99,
            price_excl_tax=18.99,
            availability="In stock",
            num_reviews=5,
            rating=4,
            image_url="https://example.com/image.jpg",
            source_url="https://example.com/book",
            data_fingerprint="same-fingerprint",
            crawl_status="successful"
        )
        
        await log_changes(test_db, old_data, new_book)
        
        # Verify no changes were logged
        changes = await test_db.change_log.find({"book_upc": "test-000"}).to_list(None)
        assert len(changes) == 0


class TestCheckBookUpdate:
    """Test the check_book_update function."""
    
    @pytest.mark.asyncio
    async def test_detect_price_change(self, test_db, httpx_mock: HTTPXMock):
        """Test that price changes are detected and logged."""
        # Setup: Insert original book
        from crawler.parser import parse_book_page
        original_book = parse_book_page(
            BOOK_HTML_V1,
            "https://books.toscrape.com/catalogue/test-book_001/index.html"
        )
        await test_db.books.insert_one(
            original_book.model_dump(mode="json", by_alias=True, exclude={'id'})
        )
        
        # Mock the HTTP response with changed price
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_001/index.html",
            html=BOOK_HTML_V2_PRICE_CHANGE,
            status_code=200
        )
        
        # Get the original book data
        old_book = await test_db.books.find_one({"upc": "test-upc-123"})
        
        # Run the update check
        spider = Spider(test_db)
        await check_book_update(spider, test_db, old_book)
        await spider.close()
        
        # Verify change was detected and logged
        changes = await test_db.change_log.find({"book_upc": "test-upc-123"}).to_list(None)
        assert len(changes) > 0
        
        # Verify price was updated in database
        updated_book = await test_db.books.find_one({"upc": "test-upc-123"})
        assert updated_book["price_incl_tax"] == 24.99
    
    @pytest.mark.asyncio
    async def test_detect_availability_change(self, test_db, httpx_mock: HTTPXMock):
        """Test that availability changes are detected."""
        # Setup: Insert original book
        from crawler.parser import parse_book_page
        original_book = parse_book_page(
            BOOK_HTML_V1,
            "https://books.toscrape.com/catalogue/test-book_002/index.html"
        )
        await test_db.books.insert_one(
            original_book.model_dump(mode="json", by_alias=True, exclude={'id'})
        )
        
        # Mock the HTTP response with changed availability
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_002/index.html",
            html=BOOK_HTML_V3_AVAILABILITY_CHANGE,
            status_code=200
        )
        
        # Get the original book data
        old_book = await test_db.books.find_one({"upc": "test-upc-123"})
        old_book["source_url"] = "https://books.toscrape.com/catalogue/test-book_002/index.html"
        
        # Run the update check
        spider = Spider(test_db)
        await check_book_update(spider, test_db, old_book)
        await spider.close()
        
        # Verify availability was updated
        updated_book = await test_db.books.find_one({"upc": "test-upc-123"})
        assert "Out of stock" in updated_book["availability"]
    
    @pytest.mark.asyncio
    async def test_no_change_detected(self, test_db, httpx_mock: HTTPXMock):
        """Test that identical book data doesn't create logs."""
        # Setup: Insert book
        from crawler.parser import parse_book_page
        original_book = parse_book_page(
            BOOK_HTML_V1,
            "https://books.toscrape.com/catalogue/test-book_003/index.html"
        )
        await test_db.books.insert_one(
            original_book.model_dump(mode="json", by_alias=True, exclude={'id'})
        )
        
        # Mock the HTTP response with identical data
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_003/index.html",
            html=BOOK_HTML_V1,
            status_code=200
        )
        
        # Get the original book data
        old_book = await test_db.books.find_one({"upc": "test-upc-123"})
        old_book["source_url"] = "https://books.toscrape.com/catalogue/test-book_003/index.html"
        
        # Run the update check
        spider = Spider(test_db)
        await check_book_update(spider, test_db, old_book)
        await spider.close()
        
        # Verify no changes were logged
        changes = await test_db.change_log.find({"book_upc": "test-upc-123"}).to_list(None)
        assert len(changes) == 0
    
    @pytest.mark.asyncio
    async def test_handle_parse_failure(self, test_db, httpx_mock: HTTPXMock):
        """Test handling of parse failures during update check."""
        # Setup: Insert book
        book_data = {
            "upc": "test-fail-123",
            "name": "Test Book",
            "category": "Fiction",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99,
            "availability": "In stock",
            "num_reviews": 5,
            "rating": 4,
            "image_url": "https://example.com/image.jpg",
            "source_url": "https://books.toscrape.com/catalogue/broken_999/index.html",
            "data_fingerprint": "abc123",
            "crawl_status": "successful",
            "raw_html_snapshot": ""
        }
        await test_db.books.insert_one(book_data)
        
        # Mock a broken HTML response
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/broken_999/index.html",
            html="<html><body>Broken content</body></html>",
            status_code=200
        )
        
        # Run the update check
        spider = Spider(test_db)
        await check_book_update(spider, test_db, book_data)
        await spider.close()
        
        # Verify the book wasn't updated
        book = await test_db.books.find_one({"upc": "test-fail-123"})
        assert book["price_incl_tax"] == 19.99  # Unchanged


class TestChangeDetectionFingerprinting:
    """Test fingerprint-based change detection."""
    
    @pytest.mark.asyncio
    async def test_fingerprint_changes_on_data_change(self):
        """Test that fingerprint changes when data changes."""
        from crawler.parser import _get_fingerprint
        
        data1 = {
            "name": "Book",
            "price_incl_tax": 19.99,
            "availability": "In stock"
        }
        
        data2 = {
            "name": "Book",
            "price_incl_tax": 24.99,  # Changed
            "availability": "In stock"
        }
        
        fp1 = _get_fingerprint(data1)
        fp2 = _get_fingerprint(data2)
        
        assert fp1 != fp2
    
    @pytest.mark.asyncio
    async def test_fingerprint_same_for_identical_data(self):
        """Test that fingerprint is consistent for identical data."""
        from crawler.parser import _get_fingerprint
        
        data = {
            "name": "Book",
            "price_incl_tax": 19.99,
            "availability": "In stock"
        }
        
        fp1 = _get_fingerprint(data)
        fp2 = _get_fingerprint(data)
        
        assert fp1 == fp2


class TestSchedulerIntegration:
    """Integration tests for the full scheduler workflow."""
    
    @pytest.mark.asyncio
    async def test_scheduler_detects_new_book(self, test_db, httpx_mock: HTTPXMock):
        """Test that scheduler detects and adds new books."""
        # Mock list page with one book
        httpx_mock.add_response(
            url="https://books.toscrape.com/index.html",
            html=LIST_PAGE_HTML,
            status_code=200
        )
        
        # Mock book detail page
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_001/index.html",
            html=BOOK_HTML_V1,
            status_code=200
        )
        
        # Run the change detection (should discover new book)
        await run_daily_change_detection()
        
        # Verify book was added
        book = await test_db.books.find_one({"upc": "test-upc-123"})
        assert book is not None
        assert book["name"] == "Test Book"
    
    @pytest.mark.asyncio
    async def test_scheduler_updates_changed_book(self, test_db, httpx_mock: HTTPXMock):
        """Test that scheduler updates books that have changed."""
        # Setup: Insert original book
        from crawler.parser import parse_book_page
        original_book = parse_book_page(
            BOOK_HTML_V1,
            "https://books.toscrape.com/catalogue/test-book_001/index.html"
        )
        await test_db.books.insert_one(
            original_book.model_dump(mode="json", by_alias=True, exclude={'id'})
        )
        
        # Mock list page
        httpx_mock.add_response(
            url="https://books.toscrape.com/index.html",
            html=LIST_PAGE_HTML,
            status_code=200
        )
        
        # Mock book detail with changed price
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_001/index.html",
            html=BOOK_HTML_V2_PRICE_CHANGE,
            status_code=200
        )
        
        # Run change detection
        await run_daily_change_detection()
        
        # Verify book was updated
        book = await test_db.books.find_one({"upc": "test-upc-123"})
        assert book["price_incl_tax"] == 24.99
        
        # Verify change was logged
        changes = await test_db.change_log.find({"book_upc": "test-upc-123"}).to_list(None)
        assert len(changes) > 0


class TestSchedulerEdgeCases:
    """Test edge cases in scheduler."""
    
    @pytest.mark.asyncio
    async def test_handle_missing_book_on_site(self, test_db, httpx_mock: HTTPXMock):
        """Test handling when a book exists in DB but not on site anymore."""
        # Insert a book that won't be on the site
        book_data = {
            "upc": "missing-book",
            "name": "Missing Book",
            "category": "Fiction",
            "price_incl_tax": 19.99,
            "price_excl_tax": 18.99,
            "availability": "In stock",
            "num_reviews": 5,
            "rating": 4,
            "image_url": "https://example.com/image.jpg",
            "source_url": "https://books.toscrape.com/catalogue/missing_999/index.html",
            "data_fingerprint": "abc123",
            "crawl_status": "successful",
            "raw_html_snapshot": ""
        }
        await test_db.books.insert_one(book_data)
        
        # Mock empty list page (no books)
        httpx_mock.add_response(
            url="https://books.toscrape.com/index.html",
            html="<html><body></body></html>",
            status_code=200
        )

        # Mock the 404 response for the book that was "removed"
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/missing_999/index.html",
            status_code=404
        )
        
        # Should not crash
        await run_daily_change_detection()
        
        # Book should still exist in DB
        book = await test_db.books.find_one({"upc": "missing-book"})
        assert book is not None