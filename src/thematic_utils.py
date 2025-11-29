import pandas as pd

THEME_KEYWORDS = {
    "customer_service": ["support", "help", "agent", "service", "representative"],
    "security": ["fraud", "hack", "scam", "secure", "verification"],
    "performance": ["slow", "loading", "crash", "responsive", "freeze"],
    "usability": ["easy", "intuitive", "navigation", "UI", "UX"],
    "features": ["transfer", "payment", "deposit", "balance", "transactions"],
}

def identify_themes(text):
    if not isinstance(text, str):
        return []
    text_lower = text.lower()
    detected = []
    for theme, keywords in THEME_KEYWORDS.items():
        if any(k in text_lower for k in keywords):
            detected.append(theme)
    return detected

def apply_themes(df):
    df["themes"] = df["review"].apply(identify_themes)
    return df
