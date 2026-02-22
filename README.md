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

### Pipeline Overview

There are two versions of the pipeline. **Use v2** (recommended):

```
v1 (original):
  PDF page 1 ──→ batch_extract.py (OCR) ──→ extracted_results.csv
                                                     │
                                              batch_scrape.py ──→ final_enriched_results.csv
                                                                          │
                                                                   load_to_db.py ──→ MySQL

v2 (improved):
  PDF all pages ──→ batch_extract_v2.py (pdftotext) ──→ extracted_wells_v2.csv
                                                         extracted_stimulations_v2.csv
                                                                  │
                         final_enriched_results.csv (scraping) ───┤
                                                                  │
                                                           load_to_db_v2.py ──→ MySQL (oil_wells_db)
                                                                                       │
                                                                                 Part 2: Map Visualization
```

### v1 vs v2 Comparison

| Field | v1 (batch_extract.py) | v2 (batch_extract_v2.py) |
|-------|----------------------|--------------------------|
| Well Name | 20/77 | 68/77 |
| API# | 3/77 | 69/77 |
| Operator | 0/77 | 76/77 |
| County | 19/77 (scraping only) | 77/77 |
| Latitude/Longitude | 0/77 | 66/77 |
| Stimulation records | 0 | 40 |

Key difference: v1 uses OCR (pytesseract) on page 1 only. v2 discovered that the PDFs are **native text** (not scanned images), so it uses `pdftotext` to extract the full text from all pages (~175 pages per PDF on average).

### Database Schema (3 tables)

| Table | Source | Description | Rows |
|-------|--------|-------------|------|
| `wells` | PDF extraction | Well info: name, API#, operator, county, lat/lon | 77 |
| `stimulations` | PDF extraction | Frac data: proppant, pressure, stages, etc. | 40 |
| `scraped_info` | drillingedge.com | Status, operator, oil/gas production | 19 |

See `schema.sql` for full column definitions.

### How to Run (v2)

```bash
# 1. Install dependencies
brew install mysql poppler
brew services start mysql
pip install mysql-connector-python pandas

# 2. Extract data from PDFs (specify your PDF folder path)
python batch_extract_v2.py --pdf-folder /path/to/DSCI560_Lab5

# 3. Load into MySQL
python load_to_db_v2.py --rebuild

# 4. Verify
mysql -u root oil_wells_db < queries.sql
```

If your MySQL has a password:
```bash
MYSQL_PASSWORD="yourpassword" python load_to_db_v2.py --rebuild
```

### Data Cleaning Rules (per assignment Section 5)

- Missing string fields → stored as `N/A`
- Missing numeric fields → stored as `0`
- HTML tags and special characters removed
- Deduplicated on `PDF_File` (one PDF = one well)

### Known Limitations

- Some well names extracted as form labels (e.g., "24-HOUR PRODUCTION RATE") due to pdftotext multi-column layout
- A few longitude values are incorrect due to DMS parsing issues on certain PDF formats
- Stimulation field values can be swapped (Top/Bottom, Formation/Stages) due to column interleaving in pdftotext output
- `Oil_Produced` from scraping is all "2025" (year, not barrels) — stored as-is per assignment requirement
