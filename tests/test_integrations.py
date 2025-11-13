import pytest
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pytest_httpx import HTTPXMock

from utils.config import settings
from utils.database import get_database


class TestCrawlerIntegration:
    """Integration tests for the full crawler workflow."""
    
    @pytest.mark.asyncio
    async def test_full_crawler_run(self, test_db, httpx_mock: HTTPXMock):
        """Test the complete crawler workflow."""
        from crawler.main import run_crawler
        
        # Mock the list page
        httpx_mock.add_response(
            url="https://books.toscrape.com/index.html",
            html="""
            <html><body>
                <section>
                    <article class="product_pod">
                        <h3><a href="catalogue/test-book_001/index.html">Test Book</a></h3>
                    </article>
                </section>
            </body></html>
            """,
            status_code=200
        )
        
        # Mock the book detail page
        httpx_mock.add_response(
            url="https://books.toscrape.com/catalogue/test-book_001/index.html",
            html="""
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
                <p>Test description</p>
                <table class="table table-striped">
                    <tr><th>UPC</th><td>test-upc-123</td></tr>
                    <tr><th>Price (excl. tax)</th><td>£19.99</td></tr>
                    <tr><th>Price (incl. tax)</th><td>£19.99</td></tr>
                    <tr><th>Availability</th><td>In stock</td></tr>
                    <tr><th>Number of reviews</th><td>5</td></tr>
                </table>
                <div class="item active">
                    <img src="../../media/test.jpg" alt="Test">
                </div>
            </body>
            </html>
            """,
            status_code=200
        )
        
        # Temporarily override settings to use test database
        original_db_name = settings.MONGODB_DB_NAME
        settings.MONGODB_DB_NAME = test_db.name
        
        try:
            # Run the crawler
            await run_crawler()
            
            # Verify book was saved
            book = await test_db.books.find_one({"upc": "test-upc-123"})
            assert book is not None
            assert book["name"] == "Test Book"
        finally:
            # Restore original settings
            settings.MONGODB_DB_NAME = original_db_name


class TestSchedulerIntegration:
    """Integration tests for scheduler tasks."""
    
    @pytest.mark.asyncio
    async def test_scheduler_main_setup(self, test_db):
        """Test that scheduler initializes correctly."""
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
        from scheduler.tasks import run_daily_change_detection
        
        # Create and start scheduler
        scheduler = AsyncIOScheduler()
        
        # Add the job
        scheduler.add_job(
            run_daily_change_detection,
            trigger=CronTrigger(hour=3, minute=0),
            name="Daily Book Change Detection"
        )
        
        # Verify job was added
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].name == "Daily Book Change Detection"
        
        scheduler.start()
        await asyncio.sleep(0.1)
        scheduler.shutdown(wait=False)

class TestSchedulerMain:
    """Test scheduler main module."""
    
    @pytest.mark.asyncio
    async def test_scheduler_main_initialization(self):
        """Test that scheduler main can be initialized and shut down gracefully."""
        import asyncio
        from scheduler.main import main
        
        # Create a task that will run main()
        main_task = asyncio.create_task(main())
        await asyncio.sleep(0.5)
        
        # Cancel it (simulates KeyboardInterrupt)
        main_task.cancel()
        
        # Wait for it to finish
        try:
            await main_task
        except asyncio.CancelledError:
            pass
        
        assert True

class TestAPILifespan:
    """Test API startup and shutdown."""
    
    @pytest.mark.asyncio
    async def test_api_lifespan(self, test_db):
        """Test that API lifespan context manager works."""
        from api.main import lifespan, app
        from fastapi import FastAPI
        
        test_app = FastAPI()
        
        async with lifespan(test_app):
            # Verify database connection was established
            assert hasattr(test_app.state, 'db_client')
            assert hasattr(test_app.state, 'db')
            
            # Verify database can be queried
            collections = await test_app.state.db.list_collection_names()
            assert isinstance(collections, list)


class TestSecurityEdgeCases:
    """Test security module edge cases."""
    
    @pytest.mark.asyncio
    async def test_api_key_with_exception(self):
        """Test API key validation when bcrypt throws exception."""
        from api.security import get_api_key
        from fastapi import HTTPException
        import api.security as security_module
        
        # Save original hashes
        original_hashes = security_module.VALID_HASHES if hasattr(security_module, 'VALID_HASHES') else []
        
        try:
            # Set an invalid hash that will cause bcrypt to fail
            security_module.VALID_HASHES = ['$2b$12$invalid']
            
            # Should raise unauthorized exception
            with pytest.raises(HTTPException) as exc_info:
                await get_api_key("any-key")
            
            assert exc_info.value.status_code == 401
        finally:
            # Restore original hashes
            security_module.VALID_HASHES = original_hashes
    
    @pytest.mark.asyncio
    async def test_api_key_empty_hash_list(self):
        """Test API key validation with empty hash list."""
        from api.security import get_api_key
        from fastapi import HTTPException
        import api.security as security_module
        
        # Save original
        original_hashes = security_module.VALID_HASHES if hasattr(security_module, 'VALID_HASHES') else []
        
        try:
            # Set empty list
            security_module.VALID_HASHES = []
            
            # Should raise unauthorized
            with pytest.raises(HTTPException) as exc_info:
                await get_api_key("any-key")
            
            assert exc_info.value.status_code == 401
        finally:
            security_module.VALID_HASHES = original_hashes


class TestDatabaseEdgeCases:
    """Test database utility edge cases."""
    
    @pytest.mark.asyncio
    async def test_get_database_without_client(self):
        """Test get_database creates its own client when none provided."""
        from utils.database import get_database
        
        # This should create its own client
        db = get_database(client=None)
        
        assert db is not None
        assert db.name == settings.MONGODB_DB_NAME