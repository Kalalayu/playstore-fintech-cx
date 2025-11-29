# src/utils.py
import re
from dateutil import parser

def clean_text(text):
    """Normalize text: strip and remove extra spaces"""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text

def normalize_date(date_str):
    """Convert date to YYYY-MM-DD format"""
    try:
        return parser.parse(date_str).date().isoformat()
    except:
        return None
