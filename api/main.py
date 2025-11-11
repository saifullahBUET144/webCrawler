import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from motor.motor_asyncio import AsyncIOMotorClient
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from utils.logging import setup_logging
from utils.config import settings
from utils.database import setup_database_indexes
from .routers import books, changes

setup_logging()
log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application startup and shutdown events.
    """
    # startup
    log.info("API service starting up...")
    app.state.db_client = AsyncIOMotorClient(settings.MONGODB_URI)
    
    # Gets the database object.
    app.state.db = app.state.db_client[settings.MONGODB_DB_NAME]
    
    await setup_database_indexes(app.state.db)
    log.info("Connected to MongoDB and setup indexes.")
    
    yield
    
    # shutdown
    log.info("API service shutting down...")
    app.state.db_client.close()
    log.info("Closed MongoDB connection.")

app = FastAPI(
    title="FilersKeepers Book API",
    description="API for accessing crawled book data.",
    version="1.0.0",
    lifespan=lifespan
)

# Rate Limiting
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100/hour"]
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


app.include_router(books.router, prefix="/books", tags=["Books"])
app.include_router(changes.router, prefix="/changes", tags=["Changes"])

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to the FilersKeepers API"}