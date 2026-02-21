-- ============================================================
-- Lab 6 – Oil Well Database Schema
-- MySQL DDL: creates database and 3 tables
-- ============================================================

CREATE DATABASE IF NOT EXISTS oil_wells_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE oil_wells_db;

-- ----------------------------------------------------------
-- 1. wells – basic well information (from PDF page-1 OCR)
-- ----------------------------------------------------------
DROP TABLE IF EXISTS scraped_info;
DROP TABLE IF EXISTS stimulations;
DROP TABLE IF EXISTS wells;

CREATE TABLE wells (
    id                          INT AUTO_INCREMENT PRIMARY KEY,
    pdf_file                    VARCHAR(100)   NOT NULL UNIQUE,
    well_file_no                VARCHAR(20),
    well_name                   VARCHAR(200),
    api_no                      VARCHAR(20),
    operator                    VARCHAR(200),
    enseco_job_no               VARCHAR(50),
    job_type                    VARCHAR(50),
    county                      VARCHAR(100),
    state                       VARCHAR(50),
    well_surface_hole_location  VARCHAR(300),
    latitude                    DECIMAL(10,6),
    longitude                   DECIMAL(10,6),
    datum                       VARCHAR(20),
    INDEX idx_api_no    (api_no),
    INDEX idx_well_name (well_name)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- 2. stimulations – frac data (from PDF page-2 OCR)
-- ----------------------------------------------------------
CREATE TABLE stimulations (
    id                              INT AUTO_INCREMENT PRIMARY KEY,
    well_id                         INT NOT NULL,
    date_stimulated                 DATE,
    stimulated_formation            VARCHAR(100),
    type_treatment                  VARCHAR(100),
    acid_pct                        DECIMAL(5,2),
    lbs_proppant                    DECIMAL(15,2),
    top_ft                          DECIMAL(10,2),
    bottom_ft                       DECIMAL(10,2),
    stimulation_stages              INT,
    volume                          DECIMAL(15,2),
    volume_units                    VARCHAR(20),
    max_treatment_pressure_psi      DECIMAL(10,2),
    max_treatment_rate_bbls_min     DECIMAL(10,2),
    details                         TEXT,
    INDEX idx_stim_well_id (well_id),
    CONSTRAINT fk_stim_well FOREIGN KEY (well_id) REFERENCES wells(id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- 3. scraped_info – web-scraper supplementary data
-- ----------------------------------------------------------
CREATE TABLE scraped_info (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    well_id           INT NOT NULL UNIQUE,
    scraped_api       VARCHAR(20),
    scraped_operator  VARCHAR(200),
    location          VARCHAR(200),
    well_status       VARCHAR(20),
    well_type         VARCHAR(50),
    closest_city      VARCHAR(100),
    oil_produced      DECIMAL(15,2),
    gas_produced      DECIMAL(15,2),
    INDEX idx_scraped_well_id (well_id),
    CONSTRAINT fk_scraped_well FOREIGN KEY (well_id) REFERENCES wells(id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;
