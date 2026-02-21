import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
import re

BASE_URL = "https://www.drillingedge.com"

# -----------------------
# Step 1: Get browser session cookies
# -----------------------
options = Options()
options.add_argument(f"--user-data-dir={os.path.expanduser('~/chrome_selenium_profile')}")
options.add_argument("--start-maximized")

driver = webdriver.Chrome(options=options)
driver.get(BASE_URL)
time.sleep(3)

selenium_cookies = driver.get_cookies()
driver.quit()

session = requests.Session()

for cookie in selenium_cookies:
    session.cookies.set(cookie['name'], cookie['value'])

# -----------------------
# Step 2: Load OCR results
# -----------------------
# 加入 keep_default_na=False，防止字串 "N/A" 被轉成浮點數 NaN
df = pd.read_csv("extracted_results.csv", keep_default_na=False)

df["Scraped_API"] = ""
df["Scraped_Operator"] = ""
df["Scraped_Location"] = ""
df["Scraped_Status"] = ""
df["Oil_Produced"] = ""
df["Gas_Produced"] = ""

# -----------------------
# Helper function
# -----------------------
def normalize(text):
    return re.sub(r'[^A-Z0-9]', '', str(text).upper())

# -----------------------
# Step 3: Scrape
# -----------------------
for index, row in df.iterrows():

    well_name = str(row.get("Well_Name", "")).strip()
    api_no = str(row.get("API_No", "")).strip()

    # 如果 Well_Name 和 API_No 都是無效值 (N/A 或空)，則直接跳過
    if (not well_name or well_name == "N/A" or well_name.lower() == "nan" or well_name == "ERROR") and \
       (not api_no or api_no == "N/A" or api_no.lower() == "nan" or api_no == "ERROR"):
        continue

    print(f"\nScraping: {well_name} (API: {api_no})")

    # 將 API No 也加入搜尋參數中以提高精準度
    params = {
        "type": "wells",
        "operator_name": ""
    }
    
    if well_name and well_name != "N/A":
        params["well_name"] = well_name
    if api_no and api_no != "N/A":
        params["api_no"] = api_no

    try:
        response = session.get(BASE_URL + "/search", params=params, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table")
        if not table:
            print("  No results table.")
            continue

        rows = table.find_all("tr")
        if len(rows) <= 1:
            print("  No search results.")
            continue

        cols = rows[1].find_all("td")

        scraped_api = cols[0].get_text(strip=True)
        scraped_name = cols[1].get_text(strip=True)

        # 比對邏輯強化：有 API 就優先比對 API，否則比對油井名稱
        if api_no and api_no != "N/A":
            if normalize(api_no) not in normalize(scraped_api):
                print(f"  API mismatch. (Expected: {api_no}, Found: {scraped_api})")
                continue
        else:
            if normalize(well_name) not in normalize(scraped_name):
                print(f"  Name mismatch. (Expected: {well_name}, Found: {scraped_name})")
                continue

        # 寫入爬取到的資料
        df.at[index, "Scraped_API"] = scraped_api
        df.at[index, "Scraped_Location"] = cols[3].get_text(strip=True)
        df.at[index, "Scraped_Operator"] = cols[4].get_text(strip=True)
        df.at[index, "Scraped_Status"] = cols[5].get_text(strip=True)

        detail_link = cols[1].find("a")["href"]

        if not detail_link.startswith("http"):
            detail_url = BASE_URL + detail_link
        else:
            detail_url = detail_link

        detail_response = session.get(detail_url, timeout=10)
        detail_soup = BeautifulSoup(detail_response.text, "html.parser")
        detail_text = detail_soup.get_text()

        # Simple production extraction
        oil_match = re.search(r'Barrels of Oil Produced.*?([\d,]+)', detail_text)
        gas_match = re.search(r'MCF Gas Produced.*?([\d,]+)', detail_text)

        if oil_match:
            df.at[index, "Oil_Produced"] = oil_match.group(1)

        if gas_match:
            df.at[index, "Gas_Produced"] = gas_match.group(1)

        print("  Success.")

    except Exception as e:
        print("  Error:", e)

# -----------------------
# Step 4: Save
# -----------------------
df.to_csv("final_enriched_results.csv", index=False)

print("\nDone. Saved to final_enriched_results.csv")