import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGODB_URI: str
    MONGODB_DB_NAME: str = "filerskeepers"
    
    # A comma-separated list of BCRYPT-HASHED API keys
    VALID_API_KEY_HASHES: str 
    
    LOG_LEVEL: str = "INFO"

    # Email Settings
    SENDGRID_API_KEY: str = ""
    ALERT_SENDER_EMAIL: str = ""
    ALERT_RECIPIENT_EMAIL: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()