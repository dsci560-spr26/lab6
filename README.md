# DSCI 560 - Lab 6: Oil Wells Data Wrangling

## Overview

This repository contains the PDF OCR and data extraction pipeline for Lab 6.

The goal of this module is to extract structured well information from scanned oil well PDF documents and prepare the data for database insertion and web visualization.

⸻

## Scope of This Module

This part of the project focuses on:
	•	Batch OCR processing of scanned PDF files
	•	Extraction of Well File Number
	•	Extraction of Well Name
	•	Basic missing value handling
	•	Exporting structured results to CSV

⸻

## Project Structure

```
lab6/
├── batch_extract.py              # v1: PDF OCR extraction (page 1 only)
├── batch_extract_v2.py           # v2: pdftotext full-text extraction (all pages)
├── batch_scrape.py               # Web scraping from drillingedge.com
├── extracted_results.csv         # v1 OCR output
├── extracted_wells_v2.csv        # v2 well data (77 rows, 11 columns)
├── extracted_stimulations_v2.csv # v2 stimulation data (40 rows)
├── final_enriched_results.csv    # v1 OCR + scraping merged
├── schema.sql                    # MySQL DDL — creates database & 3 tables
├── load_to_db.py                 # v1 ETL script
├── load_to_db_v2.py              # v2 ETL script — reads v2 CSVs → MySQL
├── queries.sql                   # Demo queries for verification & presentation
└── README.md
```

Raw PDF files are intentionally excluded from this repository.

⸻

## Requirements
	•	Python 3.10+
	•	pytesseract
	•	pdf2image
	•	poppler
	•	tesseract OCR

macOS Setup (Homebrew)

brew install tesseract
brew install poppler

Python Dependencies

pip install pytesseract pdf2image

⸻

## How to Run
	1.	Place all PDF files inside a folder named:
Lab6_PDFs
	2.	Run:
python batch_extract.py

The script will:
	•	Process each PDF file
	•	Extract well file number and well name
	•	Handle errors gracefully
	•	Generate extracted_results.csv

⸻

## Implementation Notes
	•	Only the first page of each PDF is processed for performance.
	•	Missing values are labeled as N/A.
	•	Generated CSV files are excluded from version control.

---

## Database Pipeline

### Data Collected

From 77 PDF files, we extracted and stored data into 3 MySQL tables:

- **`wells`** (77 rows) — Well name, API#, operator, county, state, latitude, longitude
- **`stimulations`** (40 rows) — Stimulation date, formation, treatment type, proppant, pressure, stages, volume
- **`scraped_info`** (19 rows) — Well status, operator, location, oil/gas production (from drillingedge.com)

Data cleaning: missing strings are stored as `N/A`, missing numbers as `0`, deduplicated by PDF file.

### How to Load into MySQL

```bash
# 1. Install dependencies
brew install mysql poppler
brew services start mysql
pip install mysql-connector-python pandas

# 2. Load CSV data into MySQL (data already extracted in CSV files)
python load_to_db_v2.py --rebuild

# 3. If your MySQL has a password:
MYSQL_PASSWORD="yourpassword" python load_to_db_v2.py --rebuild
```

### Example Queries

```sql
USE oil_wells_db;

-- Count all wells
SELECT COUNT(*) FROM wells;

-- Wells with coordinates (for map visualization)
SELECT well_name, api_no, latitude, longitude, county
FROM wells WHERE latitude != 0;

-- Well count by operator
SELECT operator, COUNT(*) AS cnt FROM wells
WHERE operator != 'N/A' GROUP BY operator ORDER BY cnt DESC;

-- Wells with stimulation data
SELECT w.well_name, s.date_stimulated, s.stimulated_formation, s.type_treatment, s.lbs_proppant
FROM wells w JOIN stimulations s ON s.well_id = w.id;

-- Full view: wells + scraped info
SELECT w.pdf_file, w.well_name, w.api_no, w.county, w.latitude, w.longitude,
       si.well_status, si.scraped_operator, si.oil_produced
FROM wells w LEFT JOIN scraped_info si ON si.well_id = w.id
ORDER BY w.id;
```
