#!/usr/bin/env python3
"""
Lab 6 – ETL Pipeline
Reads final_enriched_results.csv, cleans the data, and loads it into MySQL.

Usage:
    python load_to_db.py                # incremental (skip existing pdf_file)
    python load_to_db.py --rebuild      # drop & recreate tables, full reload

Dependencies:
    pip install mysql-connector-python pandas
"""

import argparse
import os
import sys
from pathlib import Path

import mysql.connector
import pandas as pd

# Configuration – override with environment variables if needed
DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "oil_wells_db"),
}

CSV_PATH = Path(__file__).parent / "final_enriched_results.csv"

# Values treated as missing
MISSING_STRINGS = {"N/A", "n/a", "NA", "na", "nan", "NaN", "ERROR", "error", ""}


def clean_str(value):
    """Clean a string field: missing → 'N/A', otherwise strip whitespace."""
    if value is None:
        return "N/A"
    if isinstance(value, float) and pd.isna(value):
        return "N/A"
    s = str(value).strip()
    if s in MISSING_STRINGS:
        return "N/A"
    return s


def clean_num(value):
    """Clean a numeric field: missing or unparseable → 0."""
    if value is None:
        return 0
    if isinstance(value, float) and pd.isna(value):
        return 0
    s = str(value).strip()
    if s in MISSING_STRINGS:
        return 0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0


# Schema DDL
SCHEMA_SQL = (Path(__file__).parent / "schema.sql").read_text()


def parse_sql_stmts(sql_text):
    """Split SQL text into executable statements, stripping comments."""
    stmts = []
    for raw in sql_text.split(";"):
        lines = [l for l in raw.splitlines()
                 if l.strip() and not l.strip().startswith("--")]
        body = "\n".join(lines).strip()
        if body:
            stmts.append(body)
    return stmts


def run_etl(rebuild: bool):
    # 1. Read CSV
    if not CSV_PATH.exists():
        sys.exit(f"ERROR: CSV not found at {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, dtype=str)
    print(f"Read {len(df)} rows from {CSV_PATH.name}")

    # 2. Deduplicate on PDF_File
    before = len(df)
    df = df.drop_duplicates(subset=["PDF_File"], keep="first")
    if len(df) < before:
        print(f"  Dropped {before - len(df)} duplicate PDF_File rows")

    # 3. Connect to MySQL
    init_cfg = {k: v for k, v in DB_CONFIG.items() if k != "database"}
    conn_init = mysql.connector.connect(**init_cfg)
    cur_init = conn_init.cursor()
    cur_init.execute(
        f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cur_init.close()
    conn_init.close()

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    print(f"Connected to MySQL {DB_CONFIG['host']}:{DB_CONFIG['port']}"
          f" / {DB_CONFIG['database']}")

    # 4. Rebuild or create tables
    if rebuild:
        print("  --rebuild: dropping and recreating all tables …")
        for stmt in parse_sql_stmts(SCHEMA_SQL):
            upper = stmt.upper()
            if "CREATE DATABASE" in upper or upper.startswith("USE "):
                continue
            cur.execute(stmt)
        conn.commit()
    else:
        for stmt in parse_sql_stmts(SCHEMA_SQL):
            upper = stmt.upper()
            if "CREATE DATABASE" in upper or upper.startswith("USE "):
                continue
            if "DROP TABLE" in upper:
                continue
            try:
                cur.execute(stmt)
            except mysql.connector.errors.DatabaseError:
                pass
        conn.commit()

    # Get existing pdf_files for incremental skip
    existing_pdfs = set()
    if not rebuild:
        cur.execute("SELECT pdf_file FROM wells")
        existing_pdfs = {row[0] for row in cur.fetchall()}
        print(f"  Incremental mode: {len(existing_pdfs)} existing wells")

    # 5. Process rows
    wells_inserted = 0
    scraped_inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        pdf_file = str(row["PDF_File"]).strip()

        if pdf_file in existing_pdfs:
            skipped += 1
            continue

        # Clean well fields — missing strings become 'N/A'
        well_file_no = clean_str(row.get("Well_File_No"))
        well_name = clean_str(row.get("Well_Name"))
        api_no = clean_str(row.get("API_No"))

        # County/state from scraped location
        scraped_loc = clean_str(row.get("Scraped_Location"))
        county = "N/A"
        state = "N/A"
        if scraped_loc != "N/A":
            parts = [p.strip() for p in scraped_loc.split(",")]
            if len(parts) == 2:
                county, state = parts[0], parts[1]
            else:
                county = scraped_loc

        # INSERT into wells — fill all columns, N/A for missing strings, 0 for missing numbers
        operator = clean_str(row.get("Operator"))
        enseco_job_no = clean_str(row.get("Enseco_Job_No"))
        job_type = clean_str(row.get("Job_Type"))
        well_surface_hole_location = clean_str(row.get("Well_Surface_Hole_Location"))
        latitude = clean_num(row.get("Latitude"))
        longitude = clean_num(row.get("Longitude"))
        datum = clean_str(row.get("Datum"))

        cur.execute(
            """INSERT INTO wells
               (pdf_file, well_file_no, well_name, api_no, operator,
                enseco_job_no, job_type, county, state,
                well_surface_hole_location, latitude, longitude, datum)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (pdf_file, well_file_no, well_name, api_no, operator,
             enseco_job_no, job_type, county, state,
             well_surface_hole_location, latitude, longitude, datum),
        )
        well_id = cur.lastrowid
        wells_inserted += 1

        # Scraped info (only if there is any scraped data)
        scraped_api = clean_str(row.get("Scraped_API"))
        scraped_operator = clean_str(row.get("Scraped_Operator"))
        scraped_status = clean_str(row.get("Scraped_Status"))

        has_scraped = any(v != "N/A" for v in [
            scraped_api, scraped_operator, scraped_status, scraped_loc,
        ])

        if has_scraped:
            oil_produced = clean_num(row.get("Oil_Produced"))
            gas_produced = clean_num(row.get("Gas_Produced"))

            cur.execute(
                """INSERT INTO scraped_info
                   (well_id, scraped_api, scraped_operator, location,
                    well_status, oil_produced, gas_produced)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (well_id, scraped_api, scraped_operator, scraped_loc,
                 scraped_status, oil_produced, gas_produced),
            )
            scraped_inserted += 1

    conn.commit()

    # 6. Summary
    cur.execute("SELECT COUNT(*) FROM wells")
    total_wells = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM scraped_info")
    total_scraped = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stimulations")
    total_stim = cur.fetchone()[0]

    print(f"\nETL complete:")
    print(f"  wells       : {wells_inserted} inserted, {skipped} skipped "
          f"(total in DB: {total_wells})")
    print(f"  scraped_info: {scraped_inserted} inserted "
          f"(total in DB: {total_scraped})")
    print(f"  stimulations: {total_stim} (awaiting extraction)")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab6 ETL – CSV → MySQL")
    parser.add_argument("--rebuild", action="store_true",
                        help="Drop and recreate all tables before loading")
    args = parser.parse_args()
    run_etl(rebuild=args.rebuild)
