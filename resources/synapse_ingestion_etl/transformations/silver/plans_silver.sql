-- Silver Layer: Cleaned and validated plan data
CREATE OR REFRESH MATERIALIZED VIEW plans_silver (
  CONSTRAINT valid_plan_id EXPECT (PlanID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_plan_name EXPECT (PlanName IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_monthly_price EXPECT (MonthlyPrice >= 0)
)
COMMENT "Silver layer: Cleaned mobile service plans with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  PlanID,
  PlanName,
  PlanType,
  CAST(MonthlyPrice AS DECIMAL(10,2)) AS MonthlyPrice,
  CAST(DataAllowanceGB AS DECIMAL(10,2)) AS DataAllowanceGB,
  CAST(VoiceMinutes AS DECIMAL(10,2)) AS VoiceMinutes,
  CAST(TextMessages AS DECIMAL(10,2)) AS TextMessages,
  CAST(IsUnlimited AS BOOLEAN) AS IsUnlimited,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM plans_bronze;

