-- Bronze Layer: Ingest raw data from Synapse
CREATE MATERIALIZED VIEW customer_orders_bronze
COMMENT "Raw customer orders data ingested from Synapse dedicated SQL pool"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT 
  *,
  current_timestamp() AS ingestion_timestamp
FROM synapse_source_data;



-- Silver Layer: Cleaned and validated data with quality checks
CREATE MATERIALIZED VIEW customer_orders_silver (
  CONSTRAINT valid_order_id EXPECT (OrderID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated customer orders"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT 
  *
FROM customer_orders_bronze;



-- Gold Layer: Aggregated customer metrics
CREATE MATERIALIZED VIEW customer_orders_summary
COMMENT "Aggregated customer order metrics"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT 
  CustomerID,
  COUNT(OrderID) AS total_orders
FROM customer_orders_silver
GROUP BY CustomerID