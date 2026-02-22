SELECT
  -- wells
  w.pdf_file,
  w.well_file_no,
  w.well_name,
  w.api_no,
  w.operator,
  w.enseco_job_no,
  w.job_type,
  w.county,
  w.state,
  w.well_surface_hole_location,
  w.latitude,
  w.longitude,
  w.datum,

  -- stimulations
  s.date_stimulated,
  s.stimulated_formation,
  s.type_treatment,
  s.acid_pct,
  s.lbs_proppant,
  s.top_ft,
  s.bottom_ft,
  s.stimulation_stages,
  s.volume,
  s.volume_units,
  s.max_treatment_pressure_psi,
  s.max_treatment_rate_bbls_min,
  s.details,

  -- scraped_info
  si.scraped_api,
  si.scraped_operator,
  si.location,
  si.well_status,
  si.well_type,
  si.closest_city,
  si.oil_produced,
  si.gas_produced

FROM wells AS w
LEFT JOIN stimulations AS s  ON w.id = s.well_id
LEFT JOIN scraped_info AS si ON w.id = si.well_id
WHERE w.pdf_file IS NOT NULL
  AND w.latitude IS NOT NULL
  AND w.longitude IS NOT NULL