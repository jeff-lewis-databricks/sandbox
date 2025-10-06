-- Silver Layer: Cleaned and validated device inventory data
CREATE OR REFRESH MATERIALIZED VIEW device_inventory_silver (
  CONSTRAINT valid_device_id EXPECT (DeviceID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (Status IN ('Active', 'Inactive', 'In Stock'))
)
COMMENT "Silver layer: Cleaned device inventory with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  DeviceID,
  CustomerID,
  DeviceType,
  Manufacturer,
  Model,
  IMEI,
  CAST(PurchaseDate AS DATE) AS PurchaseDate,
  CAST(WarrantyExpiration AS DATE) AS WarrantyExpiration,
  Status,
  -- Calculated fields
  CASE 
    WHEN WarrantyExpiration IS NOT NULL AND WarrantyExpiration < CURRENT_DATE() THEN TRUE
    ELSE FALSE
  END AS IsWarrantyExpired,
  CASE 
    WHEN CustomerID IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS IsAssignedToCustomer,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM device_inventory_bronze;

