from pdf2image import convert_from_path
import pytesseract
import re

# Configure tesseract path (macOS Homebrew)
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"

PDF_FILE = "W11745.pdf"

print("Converting first page to image...")
images = convert_from_path(PDF_FILE, first_page=1, last_page=1)

print("Running OCR...")
text = pytesseract.image_to_string(images[0])

print("\n===== OCR Preview =====\n")
print(text[:800])

# Extract Well File Number
file_match = re.search(r'Well File No.*?(\d{4,6})', text, re.DOTALL)
well_file_no = file_match.group(1) if file_match else "N/A"

# Extract Well Name
well_match = re.search(
    r'Well Name and Number.*?\n(.+)',
    text,
    re.DOTALL
)

if well_match:
    full_line = well_match.group(1).strip()
    name_match = re.search(r'(.+?\d+-\d+H)', full_line)
    well_name = name_match.group(1).strip() if name_match else "N/A"
else:
    well_name = "N/A"

print("\n===== Extraction Result =====")
print("Well File No:", well_file_no)
print("Well Name:", well_name)