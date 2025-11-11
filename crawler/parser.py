import logging
import hashlib
import json
from pydantic import (
    BaseModel, Field, HttpUrl, ConfigDict,
    BeforeValidator
)
from typing import Optional, List, Annotated
from datetime import datetime
from parsel import Selector
from urllib.parse import urljoin
from bson import ObjectId

log = logging.getLogger(__name__)

# Handle MongoDB ObjectId
PyObjectId = Annotated[
    str,
    BeforeValidator(lambda v: str(v) if isinstance(v, ObjectId) else v)
]

class Book(BaseModel):
    """
    Pydantic model for a single book, fulfilling all Part 1
    data requirements.
    """
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    upc: str = Field(..., max_length=100)
    name: str = Field(...)
    description: Optional[str] = None
    category: str
    price_incl_tax: float
    price_excl_tax: float
    availability: str
    num_reviews: int
    rating: int = Field(..., ge=0, le=5)
    image_url: HttpUrl
    source_url: HttpUrl 
    crawl_status: str

    # Metadata
    crawl_timestamp: datetime = Field(default_factory=datetime.utcnow)
    data_fingerprint: str  
    raw_html_snapshot: str = ""

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_schema_extra={
            "example": {
                "upc": "a897fe39b1053632",
                "name": "A Light in the Attic",
                "price_incl_tax": 51.77,
                "rating": 3,
                "crawl_status": "successful"
            }
        }
    )

class ChangeLogEntry(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    book_upc: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    field_changed: str
    old_value: str
    new_value: str

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True
    )

# Parsing

def _get_fingerprint(data: dict) -> str:
    data_subset = {
        "price_incl_tax": data.get("price_incl_tax"),
        "price_excl_tax": data.get("price_excl_tax"),
        "availability": data.get("availability"),
    }
    canonical_json = json.dumps(
        data_subset, sort_keys=True
    ).encode('utf-8')
    return hashlib.sha256(canonical_json).hexdigest()

def _clean_price(price_str: str) -> float:
    try:
        return float(price_str.replace("£", ""))
    except (ValueError, TypeError):
        return 0.0

def _clean_rating(rating_class: str) -> int:
    mapping = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    for word, num in mapping.items():
        if word in rating_class:
            return num
    return 0

def parse_list_page(selector: Selector) -> (List[str], Optional[str]):
    book_urls = []
    for book_pod in selector.css("article.product_pod"):
        url = book_pod.css("h3 a::attr(href)").get()
        if url:
            book_urls.append(url)
            
    next_page_url = selector.css("li.next a::attr(href)").get()
    return book_urls, next_page_url

def parse_book_page(html_content: str, source_url: str) -> Optional[Book]:
    """
    Parses a book detail page and returns a populated Book model.
    """
    selector = Selector(text=html_content)
    
    try:
        table_data = {}
        for row in selector.css("table.table tr"):
            key = row.css("th::text").get()
            value = row.css("td::text").get()
            if key and value:
                table_data[key] = value.strip()

        data = {
            "upc": table_data.get("UPC"),
            "name": selector.css("div.product_main h1::text").get().strip(),
            "description": selector.css("#product_description + p::text").get(),
            "category": selector.xpath(
                "//ul[@class='breadcrumb']/li[3]/a/text()"
            ).get(),
            "price_incl_tax": _clean_price(
                table_data.get("Price (incl. tax)")
            ),
            "price_excl_tax": _clean_price(
                table_data.get("Price (excl. tax)")
            ),
            "availability": table_data.get("Availability"),
            "num_reviews": int(table_data.get("Number of reviews", 0)),
            "rating": _clean_rating(
                selector.css("p.star-rating::attr(class)").get()
            ),
            "image_url": urljoin(
                source_url,
                selector.css("div.item.active img::attr(src)").get()
            ),
            "source_url": source_url,
            "raw_html_snapshot": html_content,
            "crawl_status": "successful"
        }

        data["data_fingerprint"] = _get_fingerprint(data)
        
        return Book(**data)
        
    except Exception as e:
        log.error(f"Failed to parse book page {source_url}: {e}")
        return None