from pdf2image import convert_from_path
import pytesseract
import re

# Mac 必須指定 tesseract 路徑
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"

pdf_path = "W11745.pdf"   # 如果檔名不同請改這裡

print("Step 1: Converting first page of PDF...")
images = convert_from_path(pdf_path, first_page=1, last_page=1)

print("Step 2: Running OCR...")
text = pytesseract.image_to_string(images[0])

print("\n===== OCR TEXT (first 800 chars) =====\n")
print(text[:800])

# -----------------------------
# 1️⃣ 抓 Well File Number
# -----------------------------
print("\nSearching for Well File Number...")

file_match = re.search(r'Well File No.*?(\d{4,6})', text, re.DOTALL)

if file_match:
    well_file_no = file_match.group(1)
    print("Well File No Found:", well_file_no)
else:
    well_file_no = "N/A"
    print("Well File No not found.")

# -----------------------------
# 2️⃣ 抓 Well Name（終極穩定版）
# -----------------------------
print("\nSearching for Well Name...")

well_match = re.search(
    r'Well Name and Number.*?\n(.+)', 
    text, 
    re.DOTALL
)

if well_match:
    full_line = well_match.group(1).strip()
    
    # 抓到像 34-3H 這種結尾
    name_match = re.search(r'(.+?\d+-\d+H)', full_line)
    
    if name_match:
        well_name = name_match.group(1).strip()
    else:
        well_name = full_line.strip()
    
    print("Well Name Found:", well_name)
else:
    well_name = "N/A"
    print("Well Name not found.")

print("\n===== EXTRACTION SUMMARY =====")
print("Well File No:", well_file_no)
print("Well Name:", well_name)