-- Gold Layer: Device Lifecycle Management
-- Device inventory, warranty status, and upgrade opportunities
CREATE OR REFRESH MATERIALIZED VIEW device_lifecycle_management
COMMENT "Gold layer: Device inventory management and upgrade opportunity analysis"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Device Aggregations
  Manufacturer,
  DeviceType,
  Model,
  
  -- Inventory Counts
  COUNT(*) AS TotalDevices,
  COUNT(CASE WHEN IsAssignedToCustomer = TRUE THEN 1 END) AS AssignedDevices,
  COUNT(CASE WHEN Status = 'In Stock' THEN 1 END) AS AvailableStock,
  COUNT(CASE WHEN Status = 'Active' THEN 1 END) AS ActiveDevices,
  COUNT(CASE WHEN Status = 'Inactive' THEN 1 END) AS InactiveDevices,
  
  -- Warranty Status
  COUNT(CASE WHEN IsWarrantyExpired = FALSE AND IsAssignedToCustomer = TRUE THEN 1 END) AS DevicesUnderWarranty,
  COUNT(CASE WHEN IsWarrantyExpired = TRUE AND IsAssignedToCustomer = TRUE THEN 1 END) AS DevicesOutOfWarranty,
  ROUND(COUNT(CASE WHEN IsWarrantyExpired = TRUE AND IsAssignedToCustomer = TRUE THEN 1 END) * 100.0 / 
    NULLIF(COUNT(CASE WHEN IsAssignedToCustomer = TRUE THEN 1 END), 0), 2) AS PercentOutOfWarranty,
  
  -- Device Age Metrics
  AVG(DATEDIFF(CURRENT_DATE(), PurchaseDate)) AS AvgDeviceAgeDays,
  MIN(PurchaseDate) AS OldestDevicePurchaseDate,
  MAX(PurchaseDate) AS NewestDevicePurchaseDate,
  
  -- Upgrade Opportunities
  COUNT(CASE 
    WHEN IsAssignedToCustomer = TRUE 
    AND DATEDIFF(CURRENT_DATE(), PurchaseDate) > 730  -- Older than 2 years
    THEN 1 
  END) AS DevicesDueForUpgrade,
  
  COUNT(CASE 
    WHEN IsAssignedToCustomer = TRUE 
    AND IsWarrantyExpired = TRUE
    AND DATEDIFF(CURRENT_DATE(), WarrantyExpiration) > 180  -- Warranty expired > 6 months ago
    THEN 1 
  END) AS HighPriorityUpgrades,
  
  -- Stock Management
  CASE
    WHEN COUNT(CASE WHEN Status = 'In Stock' THEN 1 END) < 5 THEN 'Low Stock - Reorder'
    WHEN COUNT(CASE WHEN Status = 'In Stock' THEN 1 END) < 10 THEN 'Medium Stock'
    ELSE 'Adequate Stock'
  END AS StockStatus,
  
  -- Device Popularity (based on active assignments)
  ROUND(COUNT(CASE WHEN Status = 'Active' THEN 1 END) * 100.0 / 
    NULLIF(COUNT(CASE WHEN IsAssignedToCustomer = TRUE THEN 1 END), 0), 2) AS DevicePopularityScore,
  
  -- Revenue Opportunity (customers with old devices * avg plan price)
  COUNT(CASE 
    WHEN IsAssignedToCustomer = TRUE 
    AND DATEDIFF(CURRENT_DATE(), PurchaseDate) > 730
    THEN 1 
  END) AS UpgradeOpportunityCount,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM device_inventory_silver
GROUP BY Manufacturer, DeviceType, Model;

