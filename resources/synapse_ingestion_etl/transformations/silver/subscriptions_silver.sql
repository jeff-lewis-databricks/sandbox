-- Silver Layer: Cleaned and validated subscription data
CREATE OR REFRESH MATERIALIZED VIEW subscriptions_silver (
  CONSTRAINT valid_subscription_id EXPECT (SubscriptionID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_plan_id EXPECT (PlanID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (Status IN ('Active', 'Inactive', 'Suspended'))
)
COMMENT "Silver layer: Cleaned subscription records with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  SubscriptionID,
  CustomerID,
  PlanID,
  CAST(StartDate AS DATE) AS StartDate,
  CAST(EndDate AS DATE) AS EndDate,
  Status,
  CAST(AutoRenew AS BOOLEAN) AS AutoRenew,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM subscriptions_bronze;

