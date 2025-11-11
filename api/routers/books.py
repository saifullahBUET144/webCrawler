import logging
from enum import Enum
from fastapi import APIRouter, Query, Depends, Request, HTTPException, status
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from crawler.parser import Book
from ..security import get_api_key

log = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(get_api_key)])

class BookSortKey(str, Enum):
    """Enum for valid sort_by parameters."""
    rating = "rating"
    price = "price_incl_tax"
    reviews = "num_reviews"

@router.get("/", response_model=List[Book])
async def list_books(
    request: Request,
    # Filtering Params
    category: Optional[str] = Query(None, description="Filter by category"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, gt=0, description="Maximum price"),
    rating: Optional[int] = Query(None, ge=1, le=5, description="Filter by rating (1-5)"),
    
    # Sorting Params
    sort_by: BookSortKey = Query(BookSortKey.rating, description="Field to sort by"),
    sort_desc: bool = Query(True, description="Sort descending"),
    
    # Pagination Params
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page")
):
    """
    Get a paginated, filtered, and sorted list of all books.
    """
    db: AsyncIOMotorDatabase = request.app.state.db

    # Build Dynamic Filter Query
    filter_query = {}
    if category:
        filter_query["category"] = category
    if rating:
        filter_query["rating"] = rating
        
    price_filter = {}
    if min_price is not None:
        price_filter["$gte"] = min_price
    if max_price is not None:
        price_filter["$lte"] = max_price
    if price_filter:
        filter_query["price_incl_tax"] = price_filter
        
    log.debug(f"Executing query with filters: {filter_query}")
    
    # Calculate Pagination
    skip = (page - 1) * limit
    
    # Define Sort Order
    sort_key = sort_by.value
    sort_direction = -1 if sort_desc else 1
    
    # Execute Query
    cursor = db.books.find(filter_query).sort(
        [(sort_key, sort_direction), ("_id", 1)]
    ).skip(skip).limit(limit)

    books = [Book(**doc) async for doc in cursor]
    return books

@router.get("/{upc}", response_model=Book)
async def get_book_by_upc(
    upc: str,
    request: Request
):
    """
    Get full details for a single book by its UPC.
    We use the UPC as the public-facing unique identifier.
    """
    db: AsyncIOMotorDatabase = request.app.state.db
    book = await db.books.find_one({"upc": upc})
    
    if book:
        return Book(**book)
        
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Book with UPC '{upc}' not found."
    )