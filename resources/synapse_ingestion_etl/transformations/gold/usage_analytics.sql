-- Gold Layer: Usage Analytics
-- Customer usage patterns and trends for data, voice, and text services
CREATE OR REFRESH MATERIALIZED VIEW usage_analytics
COMMENT "Gold layer: Customer usage patterns and consumption metrics"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Customer and Plan Info
  u.CustomerID,
  c.FirstName,
  c.LastName,
  s.PlanID,
  p.PlanName,
  p.PlanType,
  p.IsUnlimited,
  
  -- Data Usage Metrics
  SUM(u.DataUsedGB) AS TotalDataUsedGB,
  AVG(u.DataUsedGB) AS AvgDailyDataUsedGB,
  MAX(u.DataUsedGB) AS PeakDailyDataUsedGB,
  p.DataAllowanceGB,
  CASE
    WHEN p.IsUnlimited = TRUE THEN 0
    WHEN p.DataAllowanceGB IS NULL THEN 0
    ELSE ROUND((SUM(u.DataUsedGB) - p.DataAllowanceGB) / NULLIF(p.DataAllowanceGB, 0) * 100, 2)
  END AS DataOveragePercent,
  
  -- Voice Usage Metrics
  SUM(u.VoiceMinutesUsed) AS TotalVoiceMinutes,
  AVG(u.VoiceMinutesUsed) AS AvgDailyVoiceMinutes,
  MAX(u.VoiceMinutesUsed) AS PeakDailyVoiceMinutes,
  p.VoiceMinutes AS VoiceAllowance,
  CASE
    WHEN p.IsUnlimited = TRUE THEN 0
    WHEN p.VoiceMinutes IS NULL THEN 0
    ELSE ROUND((SUM(u.VoiceMinutesUsed) - p.VoiceMinutes) / NULLIF(p.VoiceMinutes, 0) * 100, 2)
  END AS VoiceOveragePercent,
  
  -- Text Usage Metrics
  SUM(u.TextsSent) AS TotalTextsSent,
  AVG(u.TextsSent) AS AvgDailyTexts,
  MAX(u.TextsSent) AS PeakDailyTexts,
  p.TextMessages AS TextAllowance,
  
  -- Roaming Metrics
  SUM(u.RoamingCharges) AS TotalRoamingCharges,
  COUNT(CASE WHEN u.RoamingCharges > 0 THEN 1 END) AS DaysWithRoaming,
  
  -- Usage Days
  COUNT(DISTINCT u.UsageDate) AS TotalUsageDays,
  MIN(u.UsageDate) AS FirstUsageDate,
  MAX(u.UsageDate) AS LastUsageDate,
  
  -- Usage Profile
  CASE
    WHEN SUM(u.DataUsedGB) > 50 THEN 'Heavy Data User'
    WHEN SUM(u.DataUsedGB) > 20 THEN 'Moderate Data User'
    WHEN SUM(u.DataUsedGB) > 5 THEN 'Light Data User'
    ELSE 'Minimal Data User'
  END AS DataUsageProfile,
  
  CASE
    WHEN SUM(u.VoiceMinutesUsed) > 2000 THEN 'Heavy Voice User'
    WHEN SUM(u.VoiceMinutesUsed) > 1000 THEN 'Moderate Voice User'
    WHEN SUM(u.VoiceMinutesUsed) > 500 THEN 'Light Voice User'
    ELSE 'Minimal Voice User'
  END AS VoiceUsageProfile,
  
  -- Upgrade Opportunity Flag
  CASE
    WHEN p.IsUnlimited = FALSE AND (
      (p.DataAllowanceGB IS NOT NULL AND SUM(u.DataUsedGB) > p.DataAllowanceGB * 1.2) OR
      (p.VoiceMinutes IS NOT NULL AND SUM(u.VoiceMinutesUsed) > p.VoiceMinutes * 1.2)
    ) THEN TRUE
    ELSE FALSE
  END AS ShouldUpgradePlan,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM usage_data_silver u
INNER JOIN customers_silver c ON u.CustomerID = c.CustomerID
INNER JOIN subscriptions_silver s ON u.SubscriptionID = s.SubscriptionID
INNER JOIN plans_silver p ON s.PlanID = p.PlanID
WHERE u.UsageDate >= DATE_SUB(CURRENT_DATE(), 30)  -- Last 30 days
GROUP BY
  u.CustomerID,
  c.FirstName,
  c.LastName,
  s.PlanID,
  p.PlanName,
  p.PlanType,
  p.IsUnlimited,
  p.DataAllowanceGB,
  p.VoiceMinutes,
  p.TextMessages;

