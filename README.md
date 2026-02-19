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

lab6/
├── batch_extract.py
├── test_ocr.py
├── .gitignore
└── README.md

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
