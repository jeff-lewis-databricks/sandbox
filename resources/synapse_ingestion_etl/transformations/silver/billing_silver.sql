-- Silver Layer: Cleaned and validated billing data
CREATE OR REFRESH MATERIALIZED VIEW billing_silver (
  CONSTRAINT valid_billing_id EXPECT (BillingID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_subscription_id EXPECT (SubscriptionID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_total_amount EXPECT (TotalAmount >= 0),
  CONSTRAINT valid_paid_amount EXPECT (PaidAmount >= 0),
  CONSTRAINT valid_payment_status EXPECT (PaymentStatus IN ('Paid', 'Pending', 'Overdue', 'Failed'))
)
COMMENT "Silver layer: Cleaned billing transactions with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  BillingID,
  CustomerID,
  SubscriptionID,
  CAST(BillingDate AS DATE) AS BillingDate,
  CAST(DueDate AS DATE) AS DueDate,
  CAST(TotalAmount AS DECIMAL(10,2)) AS TotalAmount,
  CAST(PaidAmount AS DECIMAL(10,2)) AS PaidAmount,
  PaymentStatus,
  PaymentMethod,
  -- Calculated fields
  CAST(TotalAmount - PaidAmount AS DECIMAL(10,2)) AS OutstandingAmount,
  CASE 
    WHEN PaymentStatus = 'Paid' THEN TRUE
    ELSE FALSE
  END AS IsFullyPaid,
  CASE 
    WHEN PaymentStatus = 'Overdue' OR (DueDate < CURRENT_DATE() AND PaymentStatus != 'Paid') THEN TRUE
    ELSE FALSE
  END AS IsOverdue,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM billing_bronze;
