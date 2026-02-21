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
├── batch_extract.py           # PDF OCR extraction
├── batch_scrape.py            # Web scraping from drillingedge.com
├── extracted_results.csv      # Raw OCR output
├── final_enriched_results.csv # OCR + scraping merged (77 rows)
├── schema.sql                 # MySQL DDL — creates database & 3 tables
├── load_to_db.py              # ETL script — CSV → clean → MySQL
├── queries.sql                # Demo queries for verification & presentation
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

## Database Pipeline (schema.sql + load_to_db.py + queries.sql)

### Pipeline Overview

```
PDF files ──→ batch_extract.py ──→ extracted_results.csv
                                          │
                                          ▼
                                   batch_scrape.py ──→ final_enriched_results.csv
                                                              │
                                                              ▼
                                                       load_to_db.py ──→ MySQL (oil_wells_db)
                                                                              │
                                                                              ▼
                                                                        Part 2: Map Visualization
```

### Database Schema (3 tables)

| Table | Source | Description | Rows |
|-------|--------|-------------|------|
| `wells` | PDF OCR (page 1) | Well basic info: name, API#, county, state, lat/lon | 77 |
| `stimulations` | PDF OCR (page 2) | Frac data: proppant, pressure, stages, etc. | 0 (TODO) |
| `scraped_info` | drillingedge.com | Status, operator, oil/gas production | 19 |

See `schema.sql` for full column definitions.

### How to Run

```bash
# 1. Install MySQL (macOS)
brew install mysql
brew services start mysql

# 2. Install Python dependencies
pip install mysql-connector-python pandas

# 3. Load data (full rebuild)
python load_to_db.py --rebuild

# 4. Verify
mysql -u root oil_wells_db < queries.sql
```

If your MySQL has a password:
```bash
MYSQL_PASSWORD="yourpassword" python load_to_db.py --rebuild
```

### Data Cleaning Rules

- `N/A`, empty strings, `nan`, `ERROR` → stored as `NULL`
- API# normalized to `xx-xxx-xxxxx` format
- `Scraped_Location` ("McKenzie County, ND") split into `county` + `state`
- `Oil_Produced = 2025` flagged as likely year (not production), stored as `NULL`
- Deduplicated on `PDF_File` (one PDF = one well)

### Current Data Gaps

Fields **required by the assignment** but not yet in the CSV:

| Missing Field | Where It Should Come From |
|---------------|--------------------------|
| Operator, Enseco Job#, Job Type | PDF page 1 OCR |
| Latitude, Longitude, Datum | PDF page 1 OCR — **needed for Part 2 map** |
| Well Surface Hole Location (SHL) | PDF page 1 OCR |
| All stimulation fields | PDF page 2 OCR |
| Well Type, Closest City | Web scraping |
| Oil/Gas Production (real values) | Web scraping (current values are all "2025") |

**To update**: improve extraction, regenerate `final_enriched_results.csv`, then re-run:
```bash
python load_to_db.py --rebuild
```
