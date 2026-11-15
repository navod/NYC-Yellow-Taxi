INSERT INTO dw.dim_location (location_id, borough, zone, service_zone)
SELECT DISTINCT ON (location_id)
    location_id,
    COALESCE(NULLIF(borough, ''),       'Unknown'),
    COALESCE(NULLIF(zone, ''),          'Unknown'),
    COALESCE(NULLIF(service_zone, ''),  'Unknown')
FROM staging.stg_zones
WHERE location_id IS NOT NULL
ORDER BY location_id
ON CONFLICT (location_id) DO UPDATE
SET borough      = EXCLUDED.borough,
    zone         = EXCLUDED.zone,
    service_zone = EXCLUDED.service_zone;
