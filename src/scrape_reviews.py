"""
Google Play Store Review Scraper - Task 1: Data Collection

This script scrapes user reviews from Google Play Store for three Ethiopian banks.
Target: 400+ reviews per bank (1200 total minimum)
"""

import os
import time
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from google_play_scraper import reviews, Sort

# Import helper functions from utils.py (same folder)
from utils import clean_text, normalize_date

# --- App Configuration ---
APP_IDS = {
    "CBE": "com.combanketh.mobilebanking",
    "BOA": "com.bankofabyssinia.cashgo",
    "Dashen": "com.cr2.amolelight"
}

BANK_NAMES = {
    "CBE": "Commercial Bank of Ethiopia",
    "BOA": "Bank of Abyssinia",
    "Dashen": "Dashen Bank"
}

SCRAPING_CONFIG = {
    "reviews_per_bank": 400,   # Minimum reviews per bank
    "lang": "en",
    "country": "us",
    "max_retries": 3
}

DATA_PATHS = {
    "raw": "data/raw",
    "processed": "data/processed",
    "raw_reviews": "data/raw/playstore_reviews_raw.csv"
}


class PlayStoreScraper:
    """Scraper class for Google Play Store reviews"""

    def __init__(self):
        self.app_ids = APP_IDS
        self.bank_names = BANK_NAMES
        self.reviews_per_bank = SCRAPING_CONFIG['reviews_per_bank']
        self.lang = SCRAPING_CONFIG['lang']
        self.country = SCRAPING_CONFIG['country']
        self.max_retries = SCRAPING_CONFIG['max_retries']

    def scrape_reviews_for_app(self, app_id, count=400):
        """Scrape reviews for a specific app with retry logic"""
        for attempt in range(self.max_retries):
            try:
                result, _ = reviews(
                    app_id,
                    lang=self.lang,
                    country=self.country,
                    sort=Sort.NEWEST,
                    count=count,
                    filter_score_with=None
                )
                return result
            except Exception as e:
                print(f"Attempt {attempt+1} failed for {app_id}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(5)
                else:
                    return []

    def process_reviews(self, reviews_data, bank_code):
        """Process raw reviews into a structured format"""
        processed = []
        for review in reviews_data:
            processed.append({
                "review_id": review.get("reviewId", ""),
                "review_text": clean_text(review.get("content", "")),
                "rating": review.get("score", 0),
                "review_date": normalize_date(review.get("at", datetime.now())),
                "user_name": review.get("userName", "Anonymous"),
                "thumbs_up": review.get("thumbsUpCount", 0),
                "reply_content": review.get("replyContent", None),
                "bank_code": bank_code,
                "bank_name": self.bank_names[bank_code],
                "app_version": review.get("reviewCreatedVersion", "N/A"),
                "source": "Google Play"
            })
        return processed

    def scrape_all_banks(self):
        """Main method: scrape reviews for all banks"""
        all_reviews = []

        print("="*60)
        print("Starting Google Play Store Review Scraper")
        print("="*60)

        for bank_code, app_id in tqdm(self.app_ids.items(), desc="Scraping Banks"):
            print(f"\nScraping {self.bank_names[bank_code]} ({app_id})...")
            reviews_data = self.scrape_reviews_for_app(app_id, self.reviews_per_bank)
            if reviews_data:
                processed = self.process_reviews(reviews_data, bank_code)
                all_reviews.extend(processed)
                print(f"Collected {len(processed)} reviews for {self.bank_names[bank_code]}")
            else:
                print(f"WARNING: No reviews collected for {self.bank_names[bank_code]}")
            time.sleep(2)  # polite delay

        # Save raw reviews CSV
        if all_reviews:
            os.makedirs(DATA_PATHS['raw'], exist_ok=True)
            df = pd.DataFrame(all_reviews)
            df.to_csv(DATA_PATHS['raw_reviews'], index=False)
            print(f"\nScraping complete! Total reviews collected: {len(df)}")
            print(f"Saved raw reviews to: {DATA_PATHS['raw_reviews']}")
            return df
        else:
            print("ERROR: No reviews collected!")
            return pd.DataFrame()

    def display_sample_reviews(self, df, n=3):
        """Display sample reviews per bank"""
        print("\nSample Reviews:")
        for bank_code in self.bank_names.keys():
            bank_df = df[df["bank_code"] == bank_code]
            if not bank_df.empty:
                print(f"\n{self.bank_names[bank_code]}:")
                for idx, row in bank_df.head(n).iterrows():
                    print(f"\nRating: {'⭐'*row['rating']}")
                    print(f"Review: {row['review_text'][:200]}...")
                    print(f"Date: {row['review_date']}")


def main():
    scraper = PlayStoreScraper()
    df = scraper.scrape_all_banks()
    if not df.empty:
        scraper.display_sample_reviews(df)
    return df


if __name__ == "__main__":
    reviews_df = main()
