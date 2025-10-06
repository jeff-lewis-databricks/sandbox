-- Gold Layer: Network Coverage Analysis
-- Network infrastructure metrics and coverage analysis by region
CREATE OR REFRESH MATERIALIZED VIEW network_coverage_analysis
COMMENT "Gold layer: Network tower performance and coverage metrics"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Geographic Grouping
  nt.State,
  nt.City,
  
  -- Tower Counts
  COUNT(*) AS TotalTowers,
  COUNT(CASE WHEN nt.IsOperational = TRUE THEN 1 END) AS OperationalTowers,
  COUNT(CASE WHEN nt.Status = 'Maintenance' THEN 1 END) AS TowersInMaintenance,
  COUNT(CASE WHEN nt.Status = 'Planned' THEN 1 END) AS PlannedTowers,
  
  -- Tower Type Distribution
  COUNT(CASE WHEN nt.TowerType = 'Macro' THEN 1 END) AS MacroTowers,
  COUNT(CASE WHEN nt.TowerType = 'Microcell' THEN 1 END) AS MicrocellTowers,
  COUNT(CASE WHEN nt.TowerType = 'Small Cell' THEN 1 END) AS SmallCellTowers,
  
  -- 5G Coverage
  COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) AS Towers5GEnabled,
  COUNT(CASE WHEN nt.Coverage5G = TRUE AND nt.IsOperational = TRUE THEN 1 END) AS Operational5GTowers,
  ROUND(COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS Percent5GCoverage,
  
  -- Customer Coverage (customers in areas with towers)
  COUNT(DISTINCT c.CustomerID) AS CustomersInArea,
  COUNT(DISTINCT CASE WHEN c.AccountStatus = 'Active' THEN c.CustomerID END) AS ActiveCustomersInArea,
  
  -- Network Capacity Metrics
  ROUND(COUNT(CASE WHEN nt.IsOperational = TRUE THEN 1 END) * 1.0 / NULLIF(COUNT(DISTINCT c.CustomerID), 0), 4) AS TowersPerCustomer,
  
  -- Infrastructure Age
  AVG(DATEDIFF(CURRENT_DATE(), nt.InstallationDate)) AS AvgTowerAgeDays,
  MIN(nt.InstallationDate) AS OldestTowerInstallDate,
  MAX(nt.InstallationDate) AS NewestTowerInstallDate,
  
  -- Network Quality Indicators
  CASE
    WHEN ROUND(COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) >= 80 THEN 'Excellent'
    WHEN ROUND(COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) >= 60 THEN 'Good'
    WHEN ROUND(COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) >= 40 THEN 'Fair'
    ELSE 'Poor'
  END AS NetworkQualityRating,
  
  -- Expansion Opportunity
  CASE
    WHEN COUNT(*) < 2 THEN 'High Priority - Underserved'
    WHEN ROUND(COUNT(CASE WHEN nt.Coverage5G = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) < 50 THEN 'Medium Priority - 5G Expansion'
    WHEN COUNT(CASE WHEN nt.Status = 'Maintenance' THEN 1 END) > 1 THEN 'Medium Priority - Maintenance Issues'
    ELSE 'Low Priority - Well Covered'
  END AS ExpansionPriority,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM network_towers_silver nt
LEFT JOIN customers_silver c ON nt.City = c.City AND nt.State = c.State
GROUP BY nt.State, nt.City;

