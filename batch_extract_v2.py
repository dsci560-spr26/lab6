#!/usr/bin/env python3
"""
Lab 6 – PDF Data Extraction v2
Uses pdftotext (poppler) instead of OCR to extract structured data from oil well PDFs.
Processes full text of each PDF (not just page 1).

Outputs:
    extracted_wells_v2.csv        — one row per PDF (well basic info + lat/lon)
    extracted_stimulations_v2.csv — one row per stimulation record

Usage:
    python batch_extract_v2.py
    python batch_extract_v2.py --pdf-folder /path/to/pdfs

Dependencies:
    - pdftotext (brew install poppler)
    - pandas
"""

import argparse
import csv
import os
import re
import subprocess
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_PDF_FOLDER = Path(__file__).parent / "Lab6_PDFs"
PDFTOTEXT = "/opt/homebrew/bin/pdftotext"

WELL_CSV = Path(__file__).parent / "extracted_wells_v2.csv"
STIM_CSV = Path(__file__).parent / "extracted_stimulations_v2.csv"

WELL_COLUMNS = [
    "PDF_File", "Well_File_No", "Well_Name", "API_No",
    "Operator", "County", "State", "Field", "Pool",
    "Latitude", "Longitude",
]

STIM_COLUMNS = [
    "PDF_File", "Date_Stimulated", "Stimulated_Formation",
    "Type_Treatment", "Acid_Pct", "Lbs_Proppant",
    "Top_Ft", "Bottom_Ft", "Stimulation_Stages",
    "Volume", "Volume_Units",
    "Max_Treatment_Pressure_PSI", "Max_Treatment_Rate_Bbls_Min",
    "Details",
]


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def pdf_to_text(pdf_path):
    """Extract full text from a PDF using pdftotext."""
    try:
        result = subprocess.run(
            [PDFTOTEXT, str(pdf_path), "-"],
            capture_output=True, text=True, timeout=60,
        )
        return result.stdout
    except Exception as e:
        print(f"  ERROR extracting text: {e}")
        return ""


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------

def first_match(pattern, text, group=1, flags=0):
    """Return the first regex match or None."""
    m = re.search(pattern, text, flags)
    return m.group(group).strip() if m else None


def dms_to_decimal(degrees, minutes, seconds):
    """Convert DMS to decimal degrees."""
    return float(degrees) + float(minutes) / 60 + float(seconds) / 3600


def extract_lat_lon(text):
    """Extract latitude and longitude from survey certification pages."""
    lat = None
    lon = None

    # Pattern: 48° 2' 8.980 N  or  48° 04' 27.510 N
    lat_match = re.search(
        r"(\d{2,3})°\s*(\d{1,2})[''′]\s*([\d.]+)[\"″]?\s*N", text
    )
    lon_match = re.search(
        r"(\d{2,3})°\s*(\d{1,2})[''′]\s*([\d.]+)[\"″]?\s*W", text
    )

    if lat_match:
        lat = round(dms_to_decimal(
            lat_match.group(1), lat_match.group(2), lat_match.group(3)
        ), 6)
    if lon_match:
        lon = round(-dms_to_decimal(
            lon_match.group(1), lon_match.group(2), lon_match.group(3)
        ), 6)

    return lat, lon


def extract_well_name(text):
    """Extract well name from Form 4/6/8."""
    # Look for "Well Name and Number\n<name>"
    m = re.search(r"Well Name and Number\s*\n+(.+)", text)
    if m:
        name = m.group(1).strip()
        # Filter out form labels that sometimes appear
        if name and name not in ("Spacing Unit Description", "Qtr-Qtr") and len(name) > 2:
            # Clean OCR artifacts
            name = re.sub(r"[|!']", "", name).strip()
            return name
    return None


def extract_operator(text):
    """Extract operator/company name using multiple strategies."""
    # Known operators in these ND well files
    known_operators = [
        "Oasis Petroleum North America LLC",
        "Oasis Petroleum",
        "Continental Resources, Inc.",
        "Continental Resources",
        "Enerplus Resources USA Corporation",
        "Enerplus Resources",
        "RIM Operating, Inc",
        "RIM Operating, Inc.",
        "RIM OPERATING, INC.",
        "SM Energy",
        "Whiting Petroleum",
        "Hess Corporation",
        "Marathon Oil",
        "XTO Energy",
        "Burlington Resources",
    ]
    # Search for known operators (case-insensitive)
    for op in known_operators:
        if re.search(re.escape(op), text, re.IGNORECASE):
            return op

    # Fallback: try "Company\n...\n<name>" pattern
    for m in re.finditer(r"Company\s*\n.*?\n(.+)", text):
        val = m.group(1).strip()
        if val and "Telephone" not in val and len(val) > 3:
            return val

    return None


# North Dakota counties that appear in oil well PDFs
ND_COUNTIES = [
    "McKenzie", "Williams", "Mountrail", "Dunn", "Divide",
    "Burke", "Bottineau", "Renville", "Ward", "McLean",
    "Mercer", "Oliver", "Billings", "Stark", "Golden Valley",
    "Bowman", "Slope", "Adams", "Hettinger", "Grant",
]


def extract_county(text):
    """Extract county by searching for known ND county names."""
    for county in ND_COUNTIES:
        # Look for county name near "County" label or standalone
        if re.search(r"\b" + re.escape(county) + r"\b", text):
            return county
    return None


def extract_api(text):
    """Extract North Dakota API number (33-xxx-xxxxx)."""
    m = re.search(r"(33-\d{3}-\d{5})", text)
    return m.group(1) if m else None


def extract_field_pool(text):
    """Extract field and pool names."""
    field = None
    pool = None

    # Known fields in ND
    known_fields = ["Baker", "Tioga", "Charlson", "Sanish", "Blue Buttes",
                    "Indian Hill", "Spotted Horn", "Stony Mountain"]
    for f in known_fields:
        if re.search(r"\b" + re.escape(f) + r"\b", text):
            field = f
            break

    # Known pools
    known_pools = ["Bakken", "Three Forks", "3 Forks", "Duperow", "Madison"]
    for p in known_pools:
        if re.search(r"\b" + re.escape(p) + r"\b", text):
            pool = p
            break

    return field, pool


# ---------------------------------------------------------------------------
# Stimulation extraction
# ---------------------------------------------------------------------------

def extract_stimulations(text, pdf_file):
    """Extract all stimulation records from the text."""
    records = []

    # Find all stimulation blocks starting with "Date Stimulated\n<date>"
    blocks = re.split(r"(?=Date Stimulated\s*\n)", text)

    for block in blocks:
        # Must start with Date Stimulated and have an actual date
        date_m = re.match(
            r"Date Stimulated\s*\n\s*(\d{1,2}/\d{1,2}/\d{4})", block
        )
        if not date_m:
            continue

        date_val = date_m.group(1)

        # Extract the block (up to ~60 lines or next major section)
        lines = block[:3000]

        def get_field(label, numeric=False):
            """Extract a field value from the stimulation block."""
            pattern = re.escape(label) + r"\s*\n\s*(.+)"
            m = re.search(pattern, lines)
            if m:
                val = m.group(1).strip()
                # Clean OCR artifacts
                val = re.sub(r"^[|!I'\s]+", "", val).strip()
                if numeric:
                    # Extract first number
                    num_m = re.search(r"[\d,.]+", val)
                    return num_m.group(0).replace(",", "") if num_m else None
                return val if val and len(val) > 0 else None
            return None

        formation = get_field("Stimulated Formation")
        # Clean formation
        if formation:
            formation = re.sub(r"^[|!'\s]+", "", formation).strip()

        type_treatment = get_field("Type Treatment")
        if type_treatment:
            type_treatment = re.sub(r"^[|!'\s]+", "", type_treatment).strip()

        acid_pct = get_field("Acid %", numeric=True)
        if acid_pct == "" or acid_pct == "I":
            acid_pct = None

        lbs_proppant = get_field("Lbs Proppant", numeric=True)

        # Top/Bottom are tricky due to column layout
        top_ft = get_field("Top (Ft)", numeric=True)
        # "Bottom (Ft),Stimulation Stages" or "Bottom (Ft)" - handle both
        bottom_m = re.search(r"Bottom \(Ft\)[,\s]*(?:Stimulation Stages)?\s*\n\s*([\d,.]+)", lines)
        bottom_ft = bottom_m.group(1).replace(",", "") if bottom_m else None

        stim_stages_m = re.search(r"Stimulation Stages\s*\n\s*(\d+)", lines)
        if not stim_stages_m:
            # Sometimes on the same line as bottom: "20460\n50"
            if bottom_m:
                after = lines[bottom_m.end():]
                stages_m = re.match(r"\s*\n\s*(\d{1,3})\s*\n", after)
                if stages_m:
                    stim_stages = stages_m.group(1)
                else:
                    stim_stages = None
            else:
                stim_stages = None
        else:
            stim_stages = stim_stages_m.group(1)

        volume = get_field("Volume", numeric=True)
        volume_units = get_field("Volume Units")
        if volume_units:
            volume_units = re.sub(r"^[|!'\s]+", "", volume_units).strip()

        max_pressure = get_field("Maximum Treatment Pressure (PSI)", numeric=True)
        if not max_pressure:
            max_pressure = get_field("Maximum Treatment Pressure", numeric=True)

        max_rate = get_field("Maximum Treatment Rate (BBLS/Min)", numeric=True)
        if not max_rate:
            max_rate = get_field("Maximum Treatment Rate", numeric=True)

        # Details section (mesh info)
        details_parts = []
        for dm in re.finditer(r"(\d+(?:/\d+)?\s+(?:Mesh|White|Resin Coated|Ceramic|Sand)[^:]*:\s*[\d,]+)", lines):
            details_parts.append(dm.group(1).replace(",", ""))
        details = "; ".join(details_parts) if details_parts else None

        records.append({
            "PDF_File": pdf_file,
            "Date_Stimulated": date_val,
            "Stimulated_Formation": formation,
            "Type_Treatment": type_treatment,
            "Acid_Pct": acid_pct,
            "Lbs_Proppant": lbs_proppant,
            "Top_Ft": top_ft,
            "Bottom_Ft": bottom_ft,
            "Stimulation_Stages": stim_stages,
            "Volume": volume,
            "Volume_Units": volume_units,
            "Max_Treatment_Pressure_PSI": max_pressure,
            "Max_Treatment_Rate_Bbls_Min": max_rate,
            "Details": details,
        })

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_pdf(pdf_path):
    """Process a single PDF and return (well_dict, [stim_dicts])."""
    filename = pdf_path.name
    text = pdf_to_text(pdf_path)

    if not text.strip():
        print(f"  WARNING: empty text for {filename}")
        return None, []

    # Well File No from filename
    wfn_m = re.match(r"W(\d+)\.pdf", filename, re.IGNORECASE)
    well_file_no = wfn_m.group(1) if wfn_m else None

    # Extract fields
    well_name = extract_well_name(text)
    api_no = extract_api(text)
    operator = extract_operator(text)
    county = extract_county(text)
    field, pool = extract_field_pool(text)
    lat, lon = extract_lat_lon(text)

    # State is always ND (North Dakota Industrial Commission PDFs)
    state = "ND"

    well = {
        "PDF_File": filename,
        "Well_File_No": well_file_no or "",
        "Well_Name": well_name or "",
        "API_No": api_no or "",
        "Operator": operator or "",
        "County": county or "",
        "State": state,
        "Field": field or "",
        "Pool": pool or "",
        "Latitude": lat if lat else "",
        "Longitude": lon if lon else "",
    }

    stims = extract_stimulations(text, filename)

    return well, stims


def main():
    parser = argparse.ArgumentParser(description="Lab6 PDF Extraction v2")
    parser.add_argument("--pdf-folder", type=str, default=None,
                        help="Path to folder containing PDFs")
    args = parser.parse_args()

    pdf_folder = Path(args.pdf_folder) if args.pdf_folder else DEFAULT_PDF_FOLDER
    if not pdf_folder.exists():
        print(f"ERROR: PDF folder not found: {pdf_folder}")
        print("Use --pdf-folder /path/to/pdfs to specify location")
        return

    pdfs = sorted(pdf_folder.glob("*.pdf"))
    print(f"Found {len(pdfs)} PDFs in {pdf_folder}\n")

    all_wells = []
    all_stims = []

    for i, pdf_path in enumerate(pdfs):
        print(f"[{i+1}/{len(pdfs)}] {pdf_path.name}", end="")
        well, stims = process_pdf(pdf_path)

        if well:
            all_wells.append(well)
            status_parts = []
            if well["API_No"]:
                status_parts.append(f"API={well['API_No']}")
            if well["Latitude"]:
                status_parts.append(f"lat={well['Latitude']}")
            if stims:
                status_parts.append(f"stim={len(stims)}")
            print(f"  → {', '.join(status_parts) if status_parts else 'basic only'}")
        else:
            print("  → FAILED")

        all_stims.extend(stims)

    # Write wells CSV
    with open(WELL_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=WELL_COLUMNS)
        writer.writeheader()
        writer.writerows(all_wells)

    # Write stimulations CSV
    with open(STIM_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STIM_COLUMNS)
        writer.writeheader()
        writer.writerows(all_stims)

    # Summary
    wells_with_name = sum(1 for w in all_wells if w["Well_Name"])
    wells_with_api = sum(1 for w in all_wells if w["API_No"])
    wells_with_op = sum(1 for w in all_wells if w["Operator"])
    wells_with_county = sum(1 for w in all_wells if w["County"])
    wells_with_latlon = sum(1 for w in all_wells if w["Latitude"])

    print(f"\n{'='*60}")
    print(f"EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"  Total PDFs processed:    {len(all_wells)}")
    print(f"  With Well Name:          {wells_with_name}")
    print(f"  With API#:               {wells_with_api}")
    print(f"  With Operator:           {wells_with_op}")
    print(f"  With County:             {wells_with_county}")
    print(f"  With Lat/Lon:            {wells_with_latlon}")
    print(f"  Stimulation records:     {len(all_stims)}")
    print(f"{'='*60}")
    print(f"  Saved: {WELL_CSV.name}")
    print(f"  Saved: {STIM_CSV.name}")


if __name__ == "__main__":
    main()
