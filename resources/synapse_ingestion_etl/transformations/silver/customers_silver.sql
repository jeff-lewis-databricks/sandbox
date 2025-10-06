-- Silver Layer: Cleaned and validated customer data
CREATE OR REFRESH MATERIALIZED VIEW customers_silver (
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (Email IS NOT NULL AND Email LIKE '%@%') ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_status EXPECT (AccountStatus IN ('Active', 'Inactive', 'Suspended'))
)
COMMENT "Silver layer: Cleaned customer master data with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  CustomerID,
  FirstName,
  LastName,
  Email,
  PhoneNumber,
  CAST(DateOfBirth AS DATE) AS DateOfBirth,
  AccountStatus,
  CAST(CustomerSince AS DATE) AS CustomerSince,
  Address,
  City,
  State,
  ZipCode,
  CAST(CreditScore AS INT) AS CreditScore,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM customers_bronze;

