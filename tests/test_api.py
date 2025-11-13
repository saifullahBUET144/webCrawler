import pytest
from fastapi import status
from datetime import datetime, timedelta


class TestRootEndpoint:
    """Test the root endpoint."""
    
    @pytest.mark.asyncio
    async def test_read_root(self, test_client):
        """Test that root endpoint returns welcome message."""
        response = await test_client.get("/")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "message" in data
        assert "FilersKeepers" in data["message"]


class TestAuthentication:
    """Test API authentication."""
    
    @pytest.mark.asyncio
    async def test_books_without_api_key(self, test_client):
        """Test that requests without API key are rejected."""
        response = await test_client.get("/books/")
        assert response.status_code == status.HTTP_403_FORBIDDEN
    
    @pytest.mark.asyncio
    async def test_books_with_invalid_api_key(self, test_client):
        """Test that requests with invalid API key are rejected."""
        headers = {"X-API-Key": "invalid-key-12345"}
        response = await test_client.get("/books/", headers=headers)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    @pytest.mark.asyncio
    async def test_books_with_valid_api_key(self, test_client, auth_headers, test_db):
        """Test that requests with valid API key are accepted."""
        response = await test_client.get("/books/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
    
    @pytest.mark.asyncio
    async def test_changes_without_api_key(self, test_client):
        """Test that changes endpoint requires authentication."""
        response = await test_client.get("/changes/")
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestBooksListEndpoint:
    """Test GET /books endpoint."""
    
    @pytest.mark.asyncio
    async def test_list_empty_books(self, test_client, auth_headers, test_db):
        """Test listing books when database is empty."""
        response = await test_client.get("/books/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
    
    @pytest.mark.asyncio
    async def test_list_books_with_data(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test listing books with data in database."""
        # Insert test data
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get("/books/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 3
        
        # Verify book structure
        book = data[0]
        assert "upc" in book
        assert "name" in book
        assert "price_incl_tax" in book
        assert "rating" in book
    
    @pytest.mark.asyncio
    async def test_filter_by_category(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test filtering books by category."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?category=Music",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 2
        assert all(book["category"] == "Music" for book in data)
    
    @pytest.mark.asyncio
    async def test_filter_by_rating(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test filtering books by rating."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?rating=5",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["rating"] == 5
    
    @pytest.mark.asyncio
    async def test_filter_by_price_range(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test filtering books by price range."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?min_price=20&max_price=30",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert 20 <= data[0]["price_incl_tax"] <= 30
    
    @pytest.mark.asyncio
    async def test_filter_by_min_price(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test filtering books by minimum price only."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?min_price=30",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert all(book["price_incl_tax"] >= 30 for book in data)
    
    @pytest.mark.asyncio
    async def test_filter_by_max_price(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test filtering books by maximum price only."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?max_price=20",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert all(book["price_incl_tax"] <= 20 for book in data)
    
    @pytest.mark.asyncio
    async def test_combined_filters(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test combining multiple filters."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?category=Music&min_price=30",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["category"] == "Music"
        assert data[0]["price_incl_tax"] >= 30
    
    @pytest.mark.asyncio
    async def test_sort_by_price_ascending(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test sorting books by price ascending."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?sort_by=price_incl_tax&sort_desc=false",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        prices = [book["price_incl_tax"] for book in data]
        assert prices == sorted(prices)
    
    @pytest.mark.asyncio
    async def test_sort_by_price_descending(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test sorting books by price descending."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?sort_by=price_incl_tax&sort_desc=true",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        prices = [book["price_incl_tax"] for book in data]
        assert prices == sorted(prices, reverse=True)
    
    @pytest.mark.asyncio
    async def test_sort_by_rating(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test sorting books by rating."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?sort_by=rating&sort_desc=true",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        ratings = [book["rating"] for book in data]
        assert ratings == sorted(ratings, reverse=True)
    
    @pytest.mark.asyncio
    async def test_sort_by_reviews(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test sorting books by number of reviews."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?sort_by=num_reviews&sort_desc=true",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        reviews = [book["num_reviews"] for book in data]
        assert reviews == sorted(reviews, reverse=True)
    
    @pytest.mark.asyncio
    async def test_pagination_first_page(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test pagination - first page."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?page=1&limit=2",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 2
    
    @pytest.mark.asyncio
    async def test_pagination_second_page(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test pagination - second page."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?page=2&limit=2",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 1  # Only 1 book left on page 2
    
    @pytest.mark.asyncio
    async def test_pagination_invalid_page(self, test_client, auth_headers, test_db):
        """Test pagination with invalid page number."""
        response = await test_client.get(
            "/books/?page=0",
            headers=auth_headers
        )
        # Should return 422 for invalid query parameter
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    @pytest.mark.asyncio
    async def test_pagination_empty_page(self, test_client, auth_headers, test_db, multiple_books_data):
        """Test pagination beyond available data."""
        await test_db.books.insert_many(multiple_books_data)
        
        response = await test_client.get(
            "/books/?page=10&limit=10",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 0


class TestBookDetailEndpoint:
    """Test GET /books/{upc} endpoint."""
    
    @pytest.mark.asyncio
    async def test_get_existing_book(self, test_client, auth_headers, test_db, sample_book_data):
        """Test retrieving an existing book by UPC."""
        await test_db.books.insert_one(sample_book_data)
        
        response = await test_client.get(
            f"/books/{sample_book_data['upc']}",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["upc"] == sample_book_data["upc"]
        assert data["name"] == sample_book_data["name"]
        assert data["price_incl_tax"] == sample_book_data["price_incl_tax"]
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_book(self, test_client, auth_headers, test_db):
        """Test retrieving a book that doesn't exist."""
        response = await test_client.get(
            "/books/nonexistent-upc-12345",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = response.json()
        assert "detail" in data
    
    @pytest.mark.asyncio
    async def test_get_book_without_auth(self, test_client, sample_book_data, test_db):
        """Test that book detail requires authentication."""
        await test_db.books.insert_one(sample_book_data)
        
        response = await test_client.get(f"/books/{sample_book_data['upc']}")
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestChangesEndpoint:
    """Test GET /changes endpoint."""
    
    @pytest.mark.asyncio
    async def test_get_recent_changes_empty(self, test_client, auth_headers, test_db):
        """Test getting changes when there are none."""
        response = await test_client.get("/changes/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
    
    @pytest.mark.asyncio
    async def test_get_recent_changes_with_data(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test getting recent changes with data."""
        await test_db.change_log.insert_many(sample_change_log_data)
        
        response = await test_client.get("/changes/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 3
        
        # Verify structure
        change = data[0]
        assert "book_upc" in change
        assert "timestamp" in change
        assert "field_changed" in change
        assert "old_value" in change
        assert "new_value" in change
    
    @pytest.mark.asyncio
    async def test_get_recent_changes_with_limit(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test getting recent changes with custom limit."""
        await test_db.change_log.insert_many(sample_change_log_data)
        
        response = await test_client.get("/changes/?limit=2", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 2
    
    @pytest.mark.asyncio
    async def test_get_changes_sorted_by_timestamp(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test that changes are sorted by timestamp (most recent first)."""
        await test_db.change_log.insert_many(sample_change_log_data)
        
        response = await test_client.get("/changes/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Verify descending order by timestamp
        timestamps = [change["timestamp"] for change in data]
        assert timestamps == sorted(timestamps, reverse=True)


class TestChangesReportEndpoint:
    """Test GET /changes/report endpoint."""
    
    @pytest.mark.asyncio
    async def test_get_daily_report_json_empty(self, test_client, auth_headers, test_db):
        """Test getting daily report in JSON format when empty."""
        response = await test_client.get(
            "/changes/report?format=json",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
    
    @pytest.mark.asyncio
    async def test_get_daily_report_json_with_data(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test getting daily report in JSON format with recent data."""
        # Insert only recent changes (within 24 hours)
        recent_changes = [c for c in sample_change_log_data if 
                         (datetime.utcnow() - c["timestamp"]).days < 1]
        await test_db.change_log.insert_many(recent_changes)
        
        response = await test_client.get(
            "/changes/report?format=json",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 2  # Two changes in last 24 hours
    
    @pytest.mark.asyncio
    async def test_get_daily_report_csv_empty(self, test_client, auth_headers, test_db):
        """Test getting daily report in CSV format when empty."""
        response = await test_client.get(
            "/changes/report?format=csv",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        assert "text/csv" in response.headers["content-type"]
    
    @pytest.mark.asyncio
    async def test_get_daily_report_csv_with_data(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test getting daily report in CSV format with data."""
        # Insert only recent changes
        recent_changes = [c for c in sample_change_log_data if 
                         (datetime.utcnow() - c["timestamp"]).days < 1]
        await test_db.change_log.insert_many(recent_changes)
        
        response = await test_client.get(
            "/changes/report?format=csv",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        assert "text/csv" in response.headers["content-type"]
        assert "attachment" in response.headers["content-disposition"]
        
        # Verify CSV structure
        content = response.text
        assert "book_upc" in content
        assert "timestamp" in content
        assert "field_changed" in content
    
    @pytest.mark.asyncio
    async def test_daily_report_excludes_old_changes(self, test_client, auth_headers, test_db, sample_change_log_data):
        """Test that daily report only includes changes from last 24 hours."""
        await test_db.change_log.insert_many(sample_change_log_data)
        
        response = await test_client.get(
            "/changes/report?format=json",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Should only include 2 changes (not the one from 2 days ago)
        assert len(data) == 2
        
        # Verify all returned changes are within 24 hours
        now = datetime.utcnow()
        for change in data:
            change_time = datetime.fromisoformat(change["timestamp"])
            time_diff = now - change_time
            assert time_diff.days < 1


class TestRateLimiting:
    """Test API rate limiting."""
    
    @pytest.mark.asyncio
    async def test_rate_limit_not_exceeded(self, test_client, auth_headers):
        """Test that normal usage doesn't trigger rate limit."""
        # Make a few requests
        for _ in range(5):
            response = await test_client.get("/books/", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK


class TestOpenAPIDocumentation:
    """Test OpenAPI/Swagger documentation."""
    
    @pytest.mark.asyncio
    async def test_openapi_json_available(self, test_client):
        """Test that OpenAPI JSON schema is available."""
        response = await test_client.get("/openapi.json")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        assert "openapi" in data
        assert "info" in data
        assert "paths" in data
    
    @pytest.mark.asyncio
    async def test_openapi_has_all_endpoints(self, test_client):
        """Test that all endpoints are documented."""
        response = await test_client.get("/openapi.json")
        data = response.json()
        paths = data["paths"]
        
        # Verify all required endpoints are documented
        assert "/books/" in paths
        assert "/books/{upc}" in paths
        assert "/changes/" in paths
        assert "/changes/report" in paths
    
    @pytest.mark.asyncio
    async def test_openapi_has_security_scheme(self, test_client):
        """Test that security scheme is documented."""
        response = await test_client.get("/openapi.json")
        data = response.json()
        
        # Should have security definitions
        assert "components" in data
        assert "securitySchemes" in data["components"]