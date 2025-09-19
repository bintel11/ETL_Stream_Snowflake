# app/utils.py
import json

def to_json(data):
    """Convert Python object to JSON string."""
    return json.dumps(data)

def safe_get(d, key, default=None):
    """Safe dictionary get with default."""
    return d.get(key, default) if isinstance(d, dict) else default
