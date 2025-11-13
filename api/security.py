import logging
import bcrypt
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from utils.config import settings

log = logging.getLogger(__name__)

# Define the header
api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(
    key: str = Security(api_key_header)
) -> str:
    """
    Dependency to verify the X-API-Key using bcrypt directly.
    """
    # Load the valid hashed keys from settings
    VALID_HASHES = settings.VALID_API_KEY_HASHES.split(',')

    try:
        # Convert the incoming plain-text key to bytes
        key_bytes = key.encode('utf-8')
        
        for valid_hash_str in VALID_HASHES:
            if not valid_hash_str:
                continue
            
            # Convert the stored hash string back to bytes
            valid_hash_bytes = valid_hash_str.encode('utf-8')
            
            if bcrypt.checkpw(key_bytes, valid_hash_bytes):
                return key  # Key is valid
                
    except Exception as e:
        log.error(f"Error during API key verification: {e}")
        pass 
    
    log.warning(f"Invalid API key provided.")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key",
    )