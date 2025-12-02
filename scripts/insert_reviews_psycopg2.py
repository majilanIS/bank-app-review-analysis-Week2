#!/usr/bin/env python3
"""
Insert cleaned reviews CSV into Postgres (bank_reviews).

Usage:
  python insert_reviews_psycopg2.py --csv data/processed/final_cleaned_reviewed.csv \
    --db-url "postgresql://user:pass@localhost:5432/bank_reviews"
"""

import argparse
import csv
import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

BATCH_SIZE = 1000


def ensure_bank(conn, bank_name, app_name):
    """Find bank_id by name, or insert if missing."""
    with conn.cursor() as cur:
        cur.execute("SELECT bank_id FROM banks WHERE bank_name = %s", (bank_name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO banks (bank_name, app_name) VALUES (%s, %s) RETURNING bank_id",
            (bank_name, app_name)
        )
        return cur.fetchone()[0]


def load_csv_rows(path):
    """Yield normalized rows from CSV."""
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            yield {
                "review_id": r.get("review_id") or r.get("id"),
                "review_text": r.get("review_text") or r.get("review") or "",
                "rating": int(r["rating"]) if r.get("rating") else None,
                "review_date": datetime.fromisoformat(r["review_date"]).date() if r.get("review_date") else None,
                "bank_name": r.get("bank_name") or r.get("bank") or "",
                "app_name": r.get("app_name") or "",
                "sentiment_label": r.get("sentiment_label"),
                "sentiment_score": float(r["sentiment_score"]) if r.get("sentiment_score") else None,
                "source": r.get("source") or "google_play"
            }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to cleaned reviews CSV")
    parser.add_argument("--db-url", required=True, help="Postgres connection URL")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print("CSV not found:", args.csv)
        sys.exit(1)

    conn = psycopg2.connect(args.db_url)
    conn.autocommit = False

    try:
        rows_buffer = []
        bank_id_cache = {}
        total = 0

        for row in load_csv_rows(args.csv):

            bank_key = row["bank_name"].strip()
            if not bank_key:
                print("Skipping row with no bank_name:", row["review_id"])
                continue

            if bank_key not in bank_id_cache:
                bank_id_cache[bank_key] = ensure_bank(
                    conn, bank_key, row.get("app_name")
                )

            bank_id = bank_id_cache[bank_key]

            rows_buffer.append((
                row["review_id"], bank_id, row["review_text"], row["rating"],
                row["review_date"], row["sentiment_label"], row["sentiment_score"],
                row["source"]
            ))

            # Batch insert when buffer is full
            if len(rows_buffer) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    sql = """
                    INSERT INTO reviews (
                        review_id, bank_id, review_text, rating, review_date,
                        sentiment_label, sentiment_score, source
                    )
                    VALUES %s
                    ON CONFLICT (review_id) DO UPDATE SET
                        review_text = EXCLUDED.review_text,
                        rating = EXCLUDED.rating,
                        review_date = EXCLUDED.review_date,
                        sentiment_label = EXCLUDED.sentiment_label,
                        sentiment_score = EXCLUDED.sentiment_score,
                        source = EXCLUDED.source;
                    """
                    execute_values(cur, sql, rows_buffer)
                conn.commit()

                total += len(rows_buffer)
                print(f"Inserted/Updated {total} rows...")
                rows_buffer = []

        # Final flush
        if rows_buffer:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO reviews (
                    review_id, bank_id, review_text, rating, review_date,
                    sentiment_label, sentiment_score, source
                )
                VALUES %s
                ON CONFLICT (review_id) DO UPDATE SET
                    review_text = EXCLUDED.review_text,
                    rating = EXCLUDED.rating,
                    review_date = EXCLUDED.review_date,
                    sentiment_label = EXCLUDED.sentiment_label,
                    sentiment_score = EXCLUDED.sentiment_score,
                    source = EXCLUDED.source;
                """
                execute_values(cur, sql, rows_buffer)
            conn.commit()

            total += len(rows_buffer)
            print(f"✅ Inserted/Updated {total} rows (final).")

        # Summary
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reviews;")
            print("Total reviews in DB:", cur.fetchone()[0])

            cur.execute("""
                SELECT b.bank_name, COUNT(*)
                FROM reviews r
                JOIN banks b ON r.bank_id = b.bank_id
                GROUP BY b.bank_name;
            """)
            print("Reviews per bank:")
            for row in cur.fetchall():
                print("   ", row)

    except Exception as e:
        conn.rollback()
        print("Error during insert:", e)
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()