#!/usr/bin/env python3
"""
Lab 6 – ETL Pipeline v2
Reads v2 extraction CSVs + original scraping CSV, cleans data, and loads into MySQL.

Usage:
    python load_to_db_v2.py                # incremental
    python load_to_db_v2.py --rebuild      # drop & recreate tables, full reload

Dependencies:
    pip install mysql-connector-python pandas
"""

import argparse
import os
import sys
from pathlib import Path

import mysql.connector
import pandas as pd

# Configuration
DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "user":     os.getenv("MYSQL_USER", "labuser"),
    "password": os.getenv("MYSQL_PASSWORD", "labpass"),
    "database": os.getenv("MYSQL_DB", "oil_wells_db"),
}

BASE_DIR = Path(__file__).parent
WELLS_CSV = BASE_DIR / "extracted_wells_v2.csv"
STIM_CSV = BASE_DIR / "extracted_stimulations_v2.csv"
SCRAPED_CSV = BASE_DIR / "final_enriched_results.csv"

MISSING_STRINGS = {"N/A", "n/a", "NA", "na", "nan", "NaN", "ERROR", "error", ""}
SCHEMA_SQL = (BASE_DIR / "schema.sql").read_text()


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
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return 0


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
    # 1. Read CSVs
    if not WELLS_CSV.exists():
        sys.exit(f"ERROR: {WELLS_CSV.name} not found. Run batch_extract_v2.py first.")

    wells_df = pd.read_csv(WELLS_CSV, dtype=str)
    print(f"Read {len(wells_df)} rows from {WELLS_CSV.name}")

    stim_df = pd.DataFrame()
    if STIM_CSV.exists():
        stim_df = pd.read_csv(STIM_CSV, dtype=str)
        print(f"Read {len(stim_df)} rows from {STIM_CSV.name}")

    scraped_df = pd.DataFrame()
    if SCRAPED_CSV.exists():
        scraped_df = pd.read_csv(SCRAPED_CSV, dtype=str)
        print(f"Read {len(scraped_df)} rows from {SCRAPED_CSV.name}")

    # 2. Deduplicate wells on PDF_File
    wells_df = wells_df.drop_duplicates(subset=["PDF_File"], keep="first")

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

    # Build scraped data lookup by PDF_File
    scraped_lookup = {}
    if not scraped_df.empty:
        for _, row in scraped_df.iterrows():
            pdf = clean_str(row.get("PDF_File"))
            scraped_api = clean_str(row.get("Scraped_API"))
            scraped_op = clean_str(row.get("Scraped_Operator"))
            scraped_loc = clean_str(row.get("Scraped_Location"))
            scraped_st = clean_str(row.get("Scraped_Status"))
            has_data = any(v != "N/A" for v in [scraped_api, scraped_op, scraped_loc, scraped_st])
            if has_data:
                scraped_lookup[pdf] = {
                    "scraped_api": scraped_api,
                    "scraped_operator": scraped_op,
                    "location": scraped_loc,
                    "well_status": scraped_st,
                    "oil_produced": clean_num(row.get("Oil_Produced")),
                    "gas_produced": clean_num(row.get("Gas_Produced")),
                }

    # Get existing pdf_files for incremental skip
    existing_pdfs = set()
    if not rebuild:
        cur.execute("SELECT pdf_file FROM wells")
        existing_pdfs = {row[0] for row in cur.fetchall()}
        print(f"  Incremental mode: {len(existing_pdfs)} existing wells")

    # 5. Insert wells
    wells_inserted = 0
    skipped = 0
    pdf_to_well_id = {}

    for _, row in wells_df.iterrows():
        pdf_file = str(row["PDF_File"]).strip()

        if pdf_file in existing_pdfs:
            skipped += 1
            continue

        cur.execute(
            """INSERT INTO wells
               (pdf_file, well_file_no, well_name, api_no, operator,
                enseco_job_no, job_type, county, state,
                well_surface_hole_location, latitude, longitude, datum)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                pdf_file,
                clean_str(row.get("Well_File_No")),
                clean_str(row.get("Well_Name")),
                clean_str(row.get("API_No")),
                clean_str(row.get("Operator")),
                "N/A",  # enseco_job_no — not in these PDFs
                "N/A",  # job_type — not in these PDFs
                clean_str(row.get("County")),
                clean_str(row.get("State")),
                "N/A",  # well_surface_hole_location
                clean_num(row.get("Latitude")),
                clean_num(row.get("Longitude")),
                "N/A",  # datum
            ),
        )
        well_id = cur.lastrowid
        pdf_to_well_id[pdf_file] = well_id
        wells_inserted += 1

    conn.commit()

    # 6. Insert stimulations
    stim_inserted = 0
    if not stim_df.empty:
        for _, row in stim_df.iterrows():
            pdf_file = str(row["PDF_File"]).strip()
            well_id = pdf_to_well_id.get(pdf_file)
            if not well_id:
                continue

            # Convert date from MM/DD/YYYY to YYYY-MM-DD
            date_raw = clean_str(row.get("Date_Stimulated"))
            date_val = "N/A"
            if date_raw != "N/A":
                try:
                    parts = date_raw.split("/")
                    if len(parts) == 3:
                        date_val = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                except Exception:
                    pass

            cur.execute(
                """INSERT INTO stimulations
                   (well_id, date_stimulated, stimulated_formation,
                    type_treatment, acid_pct, lbs_proppant,
                    top_ft, bottom_ft, stimulation_stages,
                    volume, volume_units,
                    max_treatment_pressure_psi, max_treatment_rate_bbls_min,
                    details)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    well_id,
                    date_val if date_val != "N/A" else None,
                    clean_str(row.get("Stimulated_Formation")),
                    clean_str(row.get("Type_Treatment")),
                    min(clean_num(row.get("Acid_Pct")), 100),  # acid % capped at 100
                    clean_num(row.get("Lbs_Proppant")),
                    clean_num(row.get("Top_Ft")),
                    clean_num(row.get("Bottom_Ft")),
                    int(clean_num(row.get("Stimulation_Stages"))) or None,
                    clean_num(row.get("Volume")),
                    clean_str(row.get("Volume_Units")),
                    clean_num(row.get("Max_Treatment_Pressure_PSI")),
                    clean_num(row.get("Max_Treatment_Rate_Bbls_Min")),
                    clean_str(row.get("Details")),
                ),
            )
            stim_inserted += 1

    conn.commit()

    # 7. Insert scraped_info
    scraped_inserted = 0
    for pdf_file, data in scraped_lookup.items():
        well_id = pdf_to_well_id.get(pdf_file)
        if not well_id:
            continue
        cur.execute(
            """INSERT INTO scraped_info
               (well_id, scraped_api, scraped_operator, location,
                well_status, oil_produced, gas_produced)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                well_id,
                data["scraped_api"],
                data["scraped_operator"],
                data["location"],
                data["well_status"],
                data["oil_produced"],
                data["gas_produced"],
            ),
        )
        scraped_inserted += 1

    conn.commit()

    # 8. Summary
    cur.execute("SELECT COUNT(*) FROM wells")
    total_wells = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stimulations")
    total_stim = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM scraped_info")
    total_scraped = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM wells WHERE latitude != 0")
    wells_with_latlon = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM wells WHERE api_no != 'N/A'")
    wells_with_api = cur.fetchone()[0]

    print(f"\nETL complete:")
    print(f"  wells       : {wells_inserted} inserted, {skipped} skipped (total: {total_wells})")
    print(f"  stimulations: {stim_inserted} inserted (total: {total_stim})")
    print(f"  scraped_info: {scraped_inserted} inserted (total: {total_scraped})")
    print(f"\n  Wells with API#:    {wells_with_api}/{total_wells}")
    print(f"  Wells with lat/lon: {wells_with_latlon}/{total_wells}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lab6 ETL v2 – CSV → MySQL")
    parser.add_argument("--rebuild", action="store_true",
                        help="Drop and recreate all tables before loading")
    args = parser.parse_args()
    run_etl(rebuild=args.rebuild)
