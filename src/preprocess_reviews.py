# src/preprocess_reviews.py
import pandas as pd
from utils import clean_text, normalize_date

RAW_CSV = "data/raw/playstore_reviews_raw.csv"
CLEAN_CSV = "data/processed/playstore_reviews_clean.csv"

def run():
    df = pd.read_csv(RAW_CSV)

    # Clean text
    df["review"] = df["review"].apply(clean_text)

    # Remove empty reviews
    df = df[df["review"].str.len() > 3]

    # Remove rows with missing rating
    df = df[df["rating"].notnull()]

    # Normalize date
    df["date"] = df["date"].apply(normalize_date)

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["review"])
    after = len(df)
    print(f"Removed {before - after} duplicate reviews")

    # Save cleaned CSV
    df.to_csv(CLEAN_CSV, index=False)
    print(f"Saved clean CSV → {CLEAN_CSV}")
    print(f"Remaining rows: {len(df)}")

if __name__ == "__main__":
    run()
