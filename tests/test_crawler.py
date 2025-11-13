import pytest
from parsel import Selector
from crawler.parser import (
    parse_book_page, 
    parse_list_page, 
    _clean_price, 
    _clean_rating,
    _get_fingerprint,
    Book
)
from crawler.spider import Spider
import httpx
from pytest_httpx import HTTPXMock


# Sample HTML for testing parser
SAMPLE_BOOK_HTML = """
<!DOCTYPE html>
<html lang="en">
<head><title>A Light in the Attic | Books to Scrape</title></head>
<body>
    <ul class="breadcrumb">
        <li><a href="../index.html">Home</a></li>
        <li><a href="../category/books_1/index.html">Books</a></li>
        <li><a href="../category/books/poetry_23/index.html">Poetry</a></li>
        <li class="active">A Light in the Attic</li>
    </ul>
    
    <div class="product_main">
        <h1>A Light in the Attic</h1>
        <p class="star-rating Three">
            <i class="icon-star"></i>
        </p>
    </div>
    
    <div id="product_description" class="sub-header"></div>
    <p>It's hard to imagine a world without A Light in the Attic.</p>
    
    <table class="table table-striped">
        <tr>
            <th>UPC</th>
            <td>a897fe39b1053632</td>
        </tr>
        <tr>
            <th>Product Type</th>
            <td>Books</td>
        </tr>
        <tr>
            <th>Price (excl. tax)</th>
            <td>£51.77</td>
        </tr>
        <tr>
            <th>Price (incl. tax)</th>
            <td>£51.77</td>
        </tr>
        <tr>
            <th>Tax</th>
            <td>£0.00</td>
        </tr>
        <tr>
            <th>Availability</th>
            <td>In stock (22 available)</td>
        </tr>
        <tr>
            <th>Number of reviews</th>
            <td>0</td>
        </tr>
    </table>
    
    <div class="item active">
        <img src="../../media/cache/2c/da/2cdad67c44b002e7ead0cc35693c0e8b.jpg" alt="A Light in the Attic">
    </div>
</body>
</html>
"""

SAMPLE_LIST_HTML = """
<!DOCTYPE html>
<html lang="en">
<body>
    <section>
        <article class="product_pod">
            <h3><a href="../../../catalogue/a-light-in-the-attic_1000/index.html" title="A Light in the Attic">A Light in the Attic</a></h3>
        </article>
        <article class="product_pod">
            <h3><a href="../../../catalogue/tipping-the-velvet_999/index.html" title="Tipping the Velvet">Tipping the Velvet</a></h3>
        </article>
        <article class="product_pod">
            <h3><a href="../../../catalogue/soumission_998/index.html" title="Soumission">Soumission</a></h3>
        </article>
    </section>
    
    <ul class="pager">
        <li class="next"><a href="page-2.html">next</a></li>
    </ul>
</body>
</html>
"""

SAMPLE_LIST_HTML_NO_NEXT = """
<!DOCTYPE html>
<html lang="en">
<body>
    <section>
        <article class="product_pod">
            <h3><a href="../../../catalogue/test-book_001/index.html" title="Test Book">Test Book</a></h3>
        </article>
    </section>
</body>
</html>
"""


class TestParserHelpers:
    """Test helper functions used in parsing."""
    
    def test_clean_price_valid(self):
        """Test price cleaning with valid input."""
        assert _clean_price("£25.99") == 25.99
        assert _clean_price("£0.00") == 0.0
        assert _clean_price("£100.50") == 100.50
    
    def test_clean_price_invalid(self):
        """Test price cleaning with invalid input."""
        assert _clean_price("invalid") == 0.0
        assert _clean_price("") == 0.0
        assert _clean_price(None) == 0.0
    
    def test_clean_rating_all_values(self):
        """Test rating cleaning for all valid ratings."""
        assert _clean_rating("star-rating One") == 1
        assert _clean_rating("star-rating Two") == 2
        assert _clean_rating("star-rating Three") == 3
        assert _clean_rating("star-rating Four") == 4
        assert _clean_rating("star-rating Five") == 5
    
    def test_clean_rating_invalid(self):
        """Test rating cleaning with invalid input."""
        assert _clean_rating("invalid-rating") == 0
        assert _clean_rating("") == 0
    
    def test_get_fingerprint_consistency(self):
        """Test that fingerprint generation is consistent."""
        data1 = {
            "name": "Test Book",
            "price_incl_tax": 25.99,
            "availability": "In stock"
        }
        data2 = {
            "name": "Test Book",
            "price_incl_tax": 25.99,
            "availability": "In stock"
        }
        assert _get_fingerprint(data1) == _get_fingerprint(data2)
    
    def test_get_fingerprint_different_data(self):
        """Test that different data produces different fingerprints."""
        data1 = {"price_incl_tax": 25.99}
        data2 = {"price_incl_tax": 26.99}
        assert _get_fingerprint(data1) != _get_fingerprint(data2)


class TestParseListPage:
    """Test the list page parser."""
    
    def test_parse_list_with_next(self):
        """Test parsing a list page with a next button."""
        selector = Selector(text=SAMPLE_LIST_HTML)
        book_urls, next_page = parse_list_page(selector)
        
        assert len(book_urls) == 3
        assert "../../../catalogue/a-light-in-the-attic_1000/index.html" in book_urls
        assert next_page == "page-2.html"
    
    def test_parse_list_without_next(self):
        """Test parsing a list page without a next button."""
        selector = Selector(text=SAMPLE_LIST_HTML_NO_NEXT)
        book_urls, next_page = parse_list_page(selector)
        
        assert len(book_urls) == 1
        assert next_page is None
    
    def test_parse_empty_list(self):
        """Test parsing an empty list page."""
        empty_html = "<html><body></body></html>"
        selector = Selector(text=empty_html)
        book_urls, next_page = parse_list_page(selector)
        
        assert len(book_urls) == 0
        assert next_page is None


class TestParseBookPage:
    """Test the book page parser."""
    
    def test_parse_valid_book(self):
        """Test parsing a valid book page."""
        source_url = "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"
        book = parse_book_page(SAMPLE_BOOK_HTML, source_url)
        
        assert book is not None
        assert isinstance(book, Book)
        assert book.upc == "a897fe39b1053632"
        assert book.name == "A Light in the Attic"
        assert book.category == "Poetry"
        assert book.price_incl_tax == 51.77
        assert book.price_excl_tax == 51.77
        assert book.availability == "In stock (22 available)"
        assert book.num_reviews == 0
        assert book.rating == 3
        assert book.crawl_status == "successful"
        assert "2cdad67c44b002e7ead0cc35693c0e8b.jpg" in str(book.image_url)
        assert str(book.source_url) == source_url
    
    def test_parse_book_with_description(self):
        """Test that book description is parsed correctly."""
        book = parse_book_page(SAMPLE_BOOK_HTML, "https://test.com")
        assert book.description == "It's hard to imagine a world without A Light in the Attic."
    
    def test_parse_book_fingerprint_generated(self):
        """Test that data fingerprint is generated."""
        book = parse_book_page(SAMPLE_BOOK_HTML, "https://test.com")
        assert book.data_fingerprint is not None
        assert len(book.data_fingerprint) == 64  # SHA-256 hash length
    
    def test_parse_book_html_snapshot_stored(self):
        """Test that raw HTML is stored."""
        book = parse_book_page(SAMPLE_BOOK_HTML, "https://test.com")
        assert book.raw_html_snapshot == SAMPLE_BOOK_HTML
    
    def test_parse_malformed_book(self):
        """Test parsing a malformed book page."""
        malformed_html = "<html><body><h1>Broken</h1></body></html>"
        book = parse_book_page(malformed_html, "https://test.com")
        
        # Should return None on parsing failure
        assert book is None


class TestSpider:
    """Test the Spider class."""
    
    @pytest.mark.asyncio
    async def test_spider_initialization(self, test_db):
        """Test that spider initializes correctly."""
        spider = Spider(test_db, concurrency=10)
        
        assert spider.db == test_db
        assert spider.semaphore._value == 10
        assert isinstance(spider.client, httpx.AsyncClient)
        assert spider.book_urls_to_crawl == set()
        
        await spider.close()
    
    @pytest.mark.asyncio
    async def test_spider_fetch_success(self, test_db, httpx_mock: HTTPXMock):
        """Test successful page fetch."""
        spider = Spider(test_db)
        
        # Mock the HTTP response
        httpx_mock.add_response(
            url="https://books.toscrape.com/index.html",
            html="<html><body>Success</body></html>",
            status_code=200
        )
        
        response = await spider.fetch_page("index.html")
        assert response.status_code == 200
        assert "Success" in response.text
        
        await spider.close()
    
    @pytest.mark.asyncio
    async def test_spider_fetch_with_retry(self, test_db, httpx_mock: HTTPXMock):
        """Test that spider retries on 5xx errors."""
        spider = Spider(test_db)
        
        # First request fails, second succeeds
        httpx_mock.add_response(status_code=503)
        httpx_mock.add_response(
            html="<html><body>Success after retry</body></html>",
            status_code=200
        )
        
        response = await spider.fetch_page("index.html")
        assert response.status_code == 200
        assert "Success after retry" in response.text
        
        await spider.close()
    
    @pytest.mark.asyncio
    async def test_spider_concurrency_limiting(self, test_db, httpx_mock: HTTPXMock):
        """Test that spider respects concurrency limits."""
        import asyncio
        
        spider = Spider(test_db, concurrency=2)
        
        # Mock multiple responses
        for i in range(5):
            httpx_mock.add_response(
                html=f"<html><body>Page {i}</body></html>",
                status_code=200
            )
        
        # Try to fetch 5 pages concurrently
        tasks = [spider.fetch_page(f"page-{i}.html") for i in range(5)]
        responses = await asyncio.gather(*tasks)
        
        assert len(responses) == 5
        assert all(r.status_code == 200 for r in responses)
        
        await spider.close()


class TestBookModel:
    """Test the Book Pydantic model."""
    
    def test_book_model_validation(self):
        """Test that Book model validates data correctly."""
        valid_data = {
            "upc": "test123",
            "name": "Test Book",
            "category": "Fiction",
            "price_incl_tax": 25.99,
            "price_excl_tax": 24.99,
            "availability": "In stock",
            "num_reviews": 10,
            "rating": 4,
            "image_url": "https://example.com/image.jpg",
            "source_url": "https://example.com/book",
            "data_fingerprint": "abc123",
            "crawl_status": "successful"
        }
        
        book = Book(**valid_data)
        assert book.name == "Test Book"
        assert book.rating == 4
    
    def test_book_model_invalid_rating(self):
        """Test that invalid rating raises validation error."""
        invalid_data = {
            "upc": "test123",
            "name": "Test Book",
            "category": "Fiction",
            "price_incl_tax": 25.99,
            "price_excl_tax": 24.99,
            "availability": "In stock",
            "num_reviews": 10,
            "rating": 6,  # Invalid: should be 0-5
            "image_url": "https://example.com/image.jpg",
            "source_url": "https://example.com/book",
            "data_fingerprint": "abc123",
            "crawl_status": "successful"
        }
        
        with pytest.raises(Exception):  # Pydantic validation error
            Book(**invalid_data)
    
    def test_book_model_alias_handling(self):
        """Test that _id alias works correctly."""
        from bson import ObjectId
        
        data = {
            "_id": ObjectId(),
            "upc": "test123",
            "name": "Test Book",
            "category": "Fiction",
            "price_incl_tax": 25.99,
            "price_excl_tax": 24.99,
            "availability": "In stock",
            "num_reviews": 10,
            "rating": 4,
            "image_url": "https://example.com/image.jpg",
            "source_url": "https://example.com/book",
            "data_fingerprint": "abc123",
            "crawl_status": "successful"
        }
        
        book = Book(**data)
        assert book.id is not None
        assert isinstance(book.id, str)