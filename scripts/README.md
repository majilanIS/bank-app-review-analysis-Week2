# 📜 Scripts Documentation

This folder contains all core Python scripts used in the **Bank App Review Analysis** project.  
The scripts automate downloading data, cleaning it, preparing it for analysis, and saving the results into the `data/` directory.

---

# 📁 Scripts Included

### 1️⃣ `scraper.py`

**Purpose:**  
Fetches Google Play Store reviews for Ethiopian bank mobile apps (CBE, Dashen, Abyssinia).

**Key Features:**

- Scrapes up to N reviews per bank
- Retrieves app metadata (rating, total installs, total reviews)
- Saves raw reviews to:
