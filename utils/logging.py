import logging.config
import sys
from.config import settings

def setup_logging():
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "default",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "app.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "formatter": "default",
            },
        },
        "loggers": {
            "": {  # Root logger
                "level": settings.LOG_LEVEL.upper(),
                "handlers": ["console", "file"],
                "propagate": True,
            },
            "httpx": {
                "level": "WARNING", # Quiets noisy HTTPO logs
                "handlers": ["console", "file"],
                "propagate": False,
            },
            "apscheduler": {
                "level": "INFO",
                "handlers": ["console", "file"],
                "propagate": False,
            }
        },
    }
    logging.config.dictConfig(LOGGING_CONFIG)