# app/logger.py
import logging
import os
from pythonjsonlogger import jsonlogger

def get_logger(name: str = __name__) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(level)
    return logger
