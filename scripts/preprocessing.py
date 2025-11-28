"""
Data Preprocessing Script
Task 1: Data Preprocessing

This script cleans and preprocesses the scraped reviews data.
- Handles missing values
- Normalizes dates
- Cleans text data (removes non-English/Amharic characters)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime
import re
from config import DATA_PATHS


class ReviewPreprocessor:
    """Preprocessor class for review data"""

    def __init__(self, input_path=None, output_path=None):
        self.input_path = input_path or DATA_PATHS['raw_reviews']
        self.output_path = output_path or DATA_PATHS['processed_reviews']
        self.df = None
        self.stats = {}

    def load_data(self):
        """Load raw reviews data"""
        print("Loading raw data...")
        try:
            self.df = pd.read_csv(self.input_path)
            print(f"Loaded {len(self.df)} reviews")
            self.stats['original_count'] = len(self.df)
            return True
        except FileNotFoundError:
            print(f"ERROR: File not found: {self.input_path}")
            return False
        except Exception as e:
            print(f"ERROR: Failed to load data: {str(e)}")
            return False

    def check_missing_data(self):
        """Check for missing data"""
        print("\n[1/6] Checking for missing data...")
        missing = self.df.isnull().sum()
        missing_pct = (missing / len(self.df)) * 100

        print("\nMissing values:")
        for col in missing.index:
            if missing[col] > 0:
                print(f"  {col}: {missing[col]} ({missing_pct[col]:.2f}%)")

        self.stats['missing_before'] = missing.to_dict()
        critical_cols = ['review_text', 'rating', 'bank_name']
        missing_critical = self.df[critical_cols].isnull().sum()
        if missing_critical.sum() > 0:
            print("\nWARNING: Missing values in critical columns:")
            print(missing_critical[missing_critical > 0])

    def handle_missing_values(self):
        """Handle missing values"""
        print("\n[2/6] Handling missing values...")
        critical_cols = ['review_text', 'rating', 'bank_name']
        before_count = len(self.df)
        self.df = self.df.dropna(subset=critical_cols)
        removed = before_count - len(self.df)
        if removed > 0:
            print(f"Removed {removed} rows with missing critical values")

        self.df['user_name'] = self.df['user_name'].fillna('Anonymous')
        self.df['thumbs_up'] = self.df['thumbs_up'].fillna(0)
        self.df['reply_content'] = self.df['reply_content'].fillna('')

        self.stats['rows_removed_missing'] = removed
        self.stats['count_after_missing'] = len(self.df)

    def normalize_dates(self):
        """Normalize date formats to YYYY-MM-DD"""
        print("\n[3/6] Normalizing dates...")
        try:
            self.df['review_date'] = pd.to_datetime(self.df['review_date'])
            self.df['review_date'] = self.df['review_date'].dt.date
            self.df['review_year'] = pd.to_datetime(self.df['review_date']).dt.year
            self.df['review_month'] = pd.to_datetime(self.df['review_date']).dt.month
            print(f"Date range: {self.df['review_date'].min()} to {self.df['review_date'].max()}")
        except Exception as e:
            print(f"WARNING: Error normalizing dates: {str(e)}")

    def clean_text(self):
        """Clean review text and remove non-English characters"""
        print("\n[4/6] Cleaning text...")

        def clean_review_text(text):
            if pd.isna(text) or text == '':
                return ''
            text = str(text).strip()
            text = re.sub(r'\s+', ' ', text) 
            text = re.sub(r"[^A-Za-z0-9 .,!?'-]", '', text)  
            return text

        self.df['review_text'] = self.df['review_text'].apply(clean_review_text)

        before = len(self.df)
        self.df = self.df[self.df['review_text'].str.len() > 0]
        removed = before - len(self.df)
        if removed > 0:
            print(f"Removed {removed} reviews with empty or invalid text")

        self.df['text_length'] = self.df['review_text'].str.len()
        self.stats['empty_reviews_removed'] = removed
        self.stats['count_after_cleaning'] = len(self.df)

    def validate_ratings(self):
        """Validate rating values (should be 1-5)"""
        print("\n[5/6] Validating ratings...")
        invalid = self.df[(self.df['rating'] < 1) | (self.df['rating'] > 5)]
        if len(invalid) > 0:
            print(f"WARNING: Found {len(invalid)} reviews with invalid ratings")
            self.df = self.df[(self.df['rating'] >= 1) & (self.df['rating'] <= 5)]
        else:
            print("All ratings are valid (1-5)")
        self.stats['invalid_ratings_removed'] = len(invalid)

    def prepare_final_output(self):
        """Prepare final output format"""
        print("\n[6/6] Preparing final output...")
        output_columns = [
            'review_id', 'review_text', 'rating', 'review_date',
            'review_year', 'review_month', 'bank_code', 'bank_name',
            'user_name', 'thumbs_up', 'text_length', 'source'
        ]
        output_columns = [col for col in output_columns if col in self.df.columns]
        self.df = self.df[output_columns]
        self.df = self.df.sort_values(['bank_code', 'review_date'], ascending=[True, False])
        self.df = self.df.reset_index(drop=True)
        print(f"Final dataset: {len(self.df)} reviews")

    def save_data(self):
        """Save processed data"""
        print("\nSaving processed data...")
        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            self.df.to_csv(self.output_path, index=False)
            print(f"Data saved to: {self.output_path}")
            self.stats['final_count'] = len(self.df)
            return True
        except Exception as e:
            print(f"ERROR: Failed to save data: {str(e)}")
            return False

    def generate_report(self):
        """Generate preprocessing report"""
        print("\n" + "=" * 60)
        print("PREPROCESSING REPORT")
        print("=" * 60)

        print(f"\nOriginal records: {self.stats.get('original_count', 0)}")
        print(f"Records with missing critical data: {self.stats.get('rows_removed_missing', 0)}")
        print(f"Empty reviews removed: {self.stats.get('empty_reviews_removed', 0)}")
        print(f"Invalid ratings removed: {self.stats.get('invalid_ratings_removed', 0)}")
        print(f"Final records: {self.stats.get('final_count', 0)}")

        if self.stats.get('original_count', 0) > 0:
            retention_rate = (self.stats.get('final_count', 0) / self.stats.get('original_count', 1)) * 100
            error_rate = 100 - retention_rate
            print(f"\nData retention rate: {retention_rate:.2f}%")
            print(f"Data error rate: {error_rate:.2f}%")
            if error_rate < 5:
                print("✓ Data quality: EXCELLENT (<5% errors)")
            elif error_rate < 10:
                print("✓ Data quality: GOOD (<10% errors)")
            else:
                print("⚠ Data quality: NEEDS ATTENTION (>10% errors)")

        if self.df is not None:
            print("\nReviews per bank:")
            bank_counts = self.df['bank_name'].value_counts()
            for bank, count in bank_counts.items():
                print(f"  {bank}: {count}")

            print("\nRating distribution:")
            rating_counts = self.df['rating'].value_counts().sort_index(ascending=False)
            for rating, count in rating_counts.items():
                pct = (count / len(self.df)) * 100
                print(f"  {'⭐' * int(rating)}: {count} ({pct:.1f}%)")

            print(f"\nDate range: {self.df['review_date'].min()} to {self.df['review_date'].max()}")
            print(f"\nText statistics:")
            print(f"  Average length: {self.df['text_length'].mean():.0f} characters")
            print(f"  Median length: {self.df['text_length'].median():.0f} characters")
            print(f"  Min length: {self.df['text_length'].min()}")
            print(f"  Max length: {self.df['text_length'].max()}")

    def process(self):
        """Run complete preprocessing pipeline"""
        print("=" * 60)
        print("STARTING DATA PREPROCESSING")
        print("=" * 60)

        if not self.load_data():
            return False

        self.check_missing_data()
        self.handle_missing_values()
        self.normalize_dates()
        self.clean_text()
        self.validate_ratings()
        self.prepare_final_output()

        if self.save_data():
            self.generate_report()
            return True
        return False


def main():
    """Main execution function"""
    preprocessor = ReviewPreprocessor()
    success = preprocessor.process()
    if success:
        print("\n✓ Preprocessing completed successfully!")
        return preprocessor.df
    else:
        print("\n✗ Preprocessing failed!")
        return None


if __name__ == "__main__":
    processed_df = main()
