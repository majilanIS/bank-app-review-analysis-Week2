# Task 3 — Store Cleaned Data in PostgreSQL

## Overview

This task involves creating a PostgreSQL database to store cleaned mobile banking review data. The database persists processed review data for analytics, reporting, and dashboards.

**Database name:** `bank_reviews`  
**Tables:** `banks`, `reviews`

- `banks` stores: `bank_id`, `bank_name`, `app_name`
- `reviews` stores: `review_id`, `bank_id (FK)`, `review_text`, `rating`, `review_date`, `sentiment_label`, `sentiment_score`, `source`, `raw_data`

---

## Database Setup and Commands

### 1️⃣ Install PostgreSQL

- Download and install PostgreSQL from: [https://www.postgresql.org/download/](https://www.postgresql.org/download/)
- Follow platform-specific instructions (Windows, macOS, Linux).

---

### 2️⃣ Create the Database

```bash
# Create a new PostgreSQL database named 'bank_reviews'
createdb bank_reviews

# Connect to the database using psql CLI
psql -U postgres -d bank_reviews


-- Create 'banks' table
CREATE TABLE banks (
    bank_id SERIAL PRIMARY KEY,
    bank_name TEXT UNIQUE NOT NULL,
    app_name TEXT
);

-- Create 'reviews' table
CREATE TABLE reviews (
    review_id TEXT PRIMARY KEY,
    bank_id INTEGER REFERENCES banks(bank_id),
    review_text TEXT,
    rating INTEGER,
    review_date DATE,
    sentiment_label TEXT,
    sentiment_score FLOAT,
    source TEXT,
    raw_data JSONB
);

# Run the Python script to insert cleaned review data from CSV
python insert_reviews_psycopg2.py \
    --csv data/processed/final_cleaned_reviewed.csv \
    --db-url "postgresql://user:pass@localhost:5432/bank_reviews"

# creating the reviews_dump
pg_dump -U postgres -d bank_reviews > bank_reviews_dump.sql
```
