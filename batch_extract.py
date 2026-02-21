import os
import re
import csv
from pdf2image import convert_from_path
import pytesseract

# Configure tesseract path (macOS Homebrew)
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"

PDF_FOLDER = "Lab6_PDFs"
OUTPUT_FILE = "extracted_results.csv"

results = []

print("Starting batch OCR processing...\n")

for filename in os.listdir(PDF_FOLDER):
    if not filename.lower().endswith(".pdf"):
        continue

    print(f"Processing: {filename}")

    file_path = os.path.join(PDF_FOLDER, filename)

    try:
        # Convert only first page for performance
        images = convert_from_path(file_path, first_page=1, last_page=1)
        text = pytesseract.image_to_string(images[0])

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

        # 新增：Extract API Number (擷取如 33-053-06057 格式的 API 號碼)
        api_match = re.search(r'API.*?(\d{2}-\d{3}-\d{5})', text)
        api_no = api_match.group(1) if api_match else "N/A"

        print(f"  Well File No: {well_file_no}")
        print(f"  Well Name: {well_name}")
        print(f"  API No: {api_no}")
        print("-" * 40)

        # 將 api_no 也加入 results 列表中
        results.append([filename, well_file_no, well_name, api_no])

    except Exception as e:
        print(f"  ERROR processing {filename}")
        print(f"  {e}")
        print("-" * 40)
        # 發生錯誤時，對應補上 4 個欄位的 ERROR
        results.append([filename, "ERROR", "ERROR", "ERROR"])

# Write results to CSV
with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    # 更新 Header，補上 API_No 欄位
    writer.writerow(["PDF_File", "Well_File_No", "Well_Name", "API_No"])
    writer.writerows(results)

print("\nBatch processing complete.")
print(f"Results saved to {OUTPUT_FILE}")