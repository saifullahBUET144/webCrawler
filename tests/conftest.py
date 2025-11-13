import pytest
from contextlib import asynccontextmanager
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from httpx import AsyncClient, ASGITransport
import bcrypt
from dotenv import load_dotenv 

load_dotenv()

from api.main import app
from utils.config import settings

# Override settings for testing
TEST_DB_NAME = "filerskeepers_test_db"
original_db_name = settings.MONGODB_DB_NAME
settings.MONGODB_DB_NAME = TEST_DB_NAME

# Generate a test API key and hash for testing
TEST_API_KEY = "test-secret-key-12345"
TEST_API_KEY_HASH = bcrypt.hashpw(TEST_API_KEY.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Override the valid API key hashes for testing
original_api_hashes = settings.VALID_API_KEY_HASHES
settings.VALID_API_KEY_HASHES = TEST_API_KEY_HASH

@pytest.fixture(scope="function")
async def db_client():
    """Session-scoped test database client."""
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    yield client
    # Drop the test database at the end of the session
    await client.drop_database(TEST_DB_NAME)
    client.close()


@pytest.fixture(scope="function")
async def test_db(db_client: AsyncIOMotorClient):
    """
    Function-scoped test database.
    Drops all collections after each test to ensure isolation.
    """
    db = db_client[TEST_DB_NAME]
    yield db
    # Clean up all collections after each test
    for collection_name in await db.list_collection_names():
        await db[collection_name].delete_many({})


@pytest.fixture(scope="function")
async def test_client(test_db: AsyncIOMotorDatabase):
    """
    A test client for the API, with the database dependency
    overridden to point to the test_db.
    """
    
    # Setup indexes for test database
    from utils.database import setup_database_indexes
    await setup_database_indexes(test_db)
    
    app.state.db_client = test_db.client
    app.state.db = test_db
    
    # Create async test client
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as client:
        yield client
    
    # Cleanup: remove the test state
    if hasattr(app.state, 'db'):
        delattr(app.state, 'db')
    if hasattr(app.state, 'db_client'):
        delattr(app.state, 'db_client')


@pytest.fixture
def auth_headers():
    """Returns headers with valid API key for authenticated requests."""
    return {"X-API-Key": TEST_API_KEY}


@pytest.fixture
def sample_book_data():
    """Returns sample book data for testing."""
    return {
        "upc": "test-upc-123456789",
        "name": "Test Book Title",
        "description": "This is a test book description.",
        "category": "Fiction",
        "price_incl_tax": 25.99,
        "price_excl_tax": 23.99,
        "availability": "In stock (10 available)",
        "num_reviews": 5,
        "rating": 4,
        "image_url": "https://books.toscrape.com/media/test.jpg",
        "source_url": "https://books.toscrape.com/catalogue/test-book_123/index.html",
        "crawl_status": "successful",
        "data_fingerprint": "abc123def456",
        "raw_html_snapshot": "<html><body>Test</body></html>"
    }


@pytest.fixture
def multiple_books_data():
    """Returns multiple sample books for testing filtering and sorting."""
    return [
        {
            "upc": "book-001",
            "name": "Book One",
            "description": "First book",
            "category": "Music",
            "price_incl_tax": 15.99,
            "price_excl_tax": 14.99,
            "availability": "In stock",
            "num_reviews": 10,
            "rating": 3,
            "image_url": "https://books.toscrape.com/media/book1.jpg",
            "source_url": "https://books.toscrape.com/catalogue/book-1_001/index.html",
            "crawl_status": "successful",
            "data_fingerprint": "fp001",
            "raw_html_snapshot": "<html>Book 1</html>"
        },
        {
            "upc": "book-002",
            "name": "Book Two",
            "description": "Second book",
            "category": "Poetry",
            "price_incl_tax": 25.99,
            "price_excl_tax": 24.99,
            "availability": "In stock",
            "num_reviews": 20,
            "rating": 5,
            "image_url": "https://books.toscrape.com/media/book2.jpg",
            "source_url": "https://books.toscrape.com/catalogue/book-2_002/index.html",
            "crawl_status": "successful",
            "data_fingerprint": "fp002",
            "raw_html_snapshot": "<html>Book 2</html>"
        },
        {
            "upc": "book-003",
            "name": "Book Three",
            "description": "Third book",
            "category": "Music",
            "price_incl_tax": 35.99,
            "price_excl_tax": 34.99,
            "availability": "Out of stock",
            "num_reviews": 15,
            "rating": 4,
            "image_url": "https://books.toscrape.com/media/book3.jpg",
            "source_url": "https://books.toscrape.com/catalogue/book-3_003/index.html",
            "crawl_status": "successful",
            "data_fingerprint": "fp003",
            "raw_html_snapshot": "<html>Book 3</html>"
        },
    ]


@pytest.fixture
def sample_change_log_data():
    """Returns sample change log entries."""
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    return [
        {
            "book_upc": "book-001",
            "timestamp": now - timedelta(hours=1),
            "field_changed": "price_incl_tax",
            "old_value": "15.99",
            "new_value": "16.99"
        },
        {
            "book_upc": "book-002",
            "timestamp": now - timedelta(hours=2),
            "field_changed": "availability",
            "old_value": "In stock",
            "new_value": "Out of stock"
        },
        {
            "book_upc": "book-001",
            "timestamp": now - timedelta(days=2),
            "field_changed": "rating",
            "old_value": "3",
            "new_value": "4"
        }
    ]


# Cleanup after all tests
def pytest_sessionfinish(session, exitstatus):
    """Restore original settings after test session."""
    settings.MONGODB_DB_NAME = original_db_name
    settings.VALID_API_KEY_HASHES = original_api_hashes