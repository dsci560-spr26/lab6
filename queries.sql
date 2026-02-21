-- ============================================================
-- Lab 6 – Demo Queries
-- Run these after load_to_db.py to verify data
-- ============================================================
USE oil_wells_db;

-- ---------------------------------------------------------
-- 1. All wells with complete core data (name + API)
-- ---------------------------------------------------------
SELECT id, pdf_file, well_file_no, well_name, api_no, county, state
FROM   wells
WHERE  well_name IS NOT NULL
  AND  api_no    IS NOT NULL;

-- ---------------------------------------------------------
-- 2. Well count by operator (from scraped data)
-- ---------------------------------------------------------
SELECT   s.scraped_operator   AS operator,
         COUNT(*)             AS well_count
FROM     scraped_info s
WHERE    s.scraped_operator IS NOT NULL
GROUP BY s.scraped_operator
ORDER BY well_count DESC;

-- ---------------------------------------------------------
-- 3. Wells grouped by status (Active / Inactive)
-- ---------------------------------------------------------
SELECT   s.well_status  AS status,
         COUNT(*)       AS cnt
FROM     scraped_info s
WHERE    s.well_status IS NOT NULL
GROUP BY s.well_status
ORDER BY cnt DESC;

-- ---------------------------------------------------------
-- 4. Wells grouped by county / state
-- ---------------------------------------------------------
SELECT   w.county, w.state, COUNT(*) AS cnt
FROM     wells w
WHERE    w.county IS NOT NULL
GROUP BY w.county, w.state
ORDER BY cnt DESC;

-- ---------------------------------------------------------
-- 5. Data quality – NULL rate per column in wells
-- ---------------------------------------------------------
SELECT
    COUNT(*)                                            AS total_rows,
    SUM(CASE WHEN well_file_no IS NULL THEN 1 ELSE 0 END) AS null_well_file_no,
    SUM(CASE WHEN well_name    IS NULL THEN 1 ELSE 0 END) AS null_well_name,
    SUM(CASE WHEN api_no       IS NULL THEN 1 ELSE 0 END) AS null_api_no,
    SUM(CASE WHEN county       IS NULL THEN 1 ELSE 0 END) AS null_county,
    SUM(CASE WHEN state        IS NULL THEN 1 ELSE 0 END) AS null_state,
    SUM(CASE WHEN latitude     IS NULL THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN longitude    IS NULL THEN 1 ELSE 0 END) AS null_longitude
FROM wells;

-- ---------------------------------------------------------
-- 6. Data quality – NULL rate per column in scraped_info
-- ---------------------------------------------------------
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN oil_produced  IS NULL THEN 1 ELSE 0 END)     AS null_oil,
    SUM(CASE WHEN gas_produced  IS NULL THEN 1 ELSE 0 END)     AS null_gas,
    SUM(CASE WHEN well_status   IS NULL THEN 1 ELSE 0 END)     AS null_status
FROM scraped_info;

-- ---------------------------------------------------------
-- 7. Map query for Part 2 (wells with coordinates)
--    NOTE: latitude/longitude will be populated later;
--    for now shows wells that have location info via scraping
-- ---------------------------------------------------------
SELECT w.id,
       w.well_name,
       w.api_no,
       w.latitude,
       w.longitude,
       w.county,
       w.state,
       s.well_status,
       s.scraped_operator AS operator,
       s.oil_produced,
       s.gas_produced
FROM   wells w
       LEFT JOIN scraped_info s ON s.well_id = w.id
WHERE  w.county IS NOT NULL;

-- ---------------------------------------------------------
-- 8. Full join view – all wells with scraped data
-- ---------------------------------------------------------
SELECT w.pdf_file,
       w.well_file_no,
       w.well_name,
       w.api_no,
       w.county,
       w.state,
       s.scraped_api,
       s.scraped_operator,
       s.well_status,
       s.oil_produced,
       s.gas_produced
FROM   wells w
       LEFT JOIN scraped_info s ON s.well_id = w.id
ORDER BY w.id;
