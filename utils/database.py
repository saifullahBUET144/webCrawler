import logging
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from .config import settings

log = logging.getLogger(__name__)

def get_database(client: AsyncIOMotorClient = None) -> AsyncIOMotorDatabase:
    """Gets the database instance."""
    if client is None:
        client = AsyncIOMotorClient(settings.MONGODB_URI)
    
    # Returns the database instance
    return client[settings.MONGODB_DB_NAME]

async def setup_database_indexes(db: AsyncIOMotorDatabase):
    """
    Creates necessary indexes on collections, if they don't exist.
    This function is idempotent.
    """
    log.info("Setting up database indexes...")
        
    # Create a unique index on 'upc' for deduplication
    await db.books.create_index("upc", unique=True)
    
    # Create a unique index on 'source_url' to prevent re-crawling
    await db.books.create_index("source_url", unique=True)
    
    # Create a compound index to optimize API filtering/sorting
    await db.books.create_index([
        ("category", 1),
        ("price_incl_tax", 1),
        ("rating", -1)
    ])
    
    log.info("Database indexes are set.")