-- Silver Layer: Cleaned and validated usage data
CREATE OR REFRESH MATERIALIZED VIEW usage_data_silver (
  CONSTRAINT valid_usage_id EXPECT (UsageID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_subscription_id EXPECT (SubscriptionID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_data_used EXPECT (DataUsedGB >= 0),
  CONSTRAINT valid_voice_minutes EXPECT (VoiceMinutesUsed >= 0),
  CONSTRAINT valid_texts EXPECT (TextsSent >= 0)
)
COMMENT "Silver layer: Cleaned usage data with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  UsageID,
  CustomerID,
  SubscriptionID,
  CAST(UsageDate AS DATE) AS UsageDate,
  -- Convert MB to GB for consistency
  CAST(DataUsedMB / 1024.0 AS DECIMAL(10,2)) AS DataUsedGB,
  CAST(VoiceMinutesUsed AS DECIMAL(10,2)) AS VoiceMinutesUsed,
  CAST(TextsSent AS INT) AS TextsSent,
  CAST(RoamingCharges AS DECIMAL(10,2)) AS RoamingCharges,
  -- Calculated fields
  CASE 
    WHEN RoamingCharges > 0 THEN TRUE
    ELSE FALSE
  END AS HasRoamingActivity,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM usage_data_bronze;
