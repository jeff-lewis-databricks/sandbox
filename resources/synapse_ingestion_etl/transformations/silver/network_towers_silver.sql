-- Silver Layer: Cleaned and validated network tower data
CREATE OR REFRESH MATERIALIZED VIEW network_towers_silver (
  CONSTRAINT valid_tower_id EXPECT (TowerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_tower_name EXPECT (TowerName IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (Status IN ('Active', 'Maintenance', 'Planned'))
)
COMMENT "Silver layer: Cleaned network tower infrastructure with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  TowerID,
  TowerName,
  CAST(Latitude AS DECIMAL(10,6)) AS Latitude,
  CAST(Longitude AS DECIMAL(10,6)) AS Longitude,
  City,
  State,
  TowerType,
  Status,
  CAST(InstallationDate AS DATE) AS InstallationDate,
  CAST(Coverage5G AS BOOLEAN) AS Coverage5G,
  -- Calculated fields
  CASE 
    WHEN Status = 'Active' THEN TRUE
    ELSE FALSE
  END AS IsOperational,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM network_towers_bronze;

