-- Silver Layer: Cleaned and validated customer service ticket data
CREATE OR REFRESH MATERIALIZED VIEW customer_service_tickets_silver (
  CONSTRAINT valid_ticket_id EXPECT (TicketID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (Status IN ('Open', 'Pending', 'In Progress', 'Closed')),
  CONSTRAINT valid_priority EXPECT (Priority IN ('Low', 'Medium', 'High', 'Critical'))
)
COMMENT "Silver layer: Cleaned customer service tickets with quality checks"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  TicketID,
  CustomerID,
  CAST(OpenedDate AS TIMESTAMP) AS OpenedDate,
  CAST(ClosedDate AS TIMESTAMP) AS ClosedDate,
  IssueType,
  Priority,
  Status,
  Resolution,
  AgentID,
  -- Calculated fields
  CASE 
    WHEN ClosedDate IS NOT NULL THEN 
      CAST((UNIX_TIMESTAMP(ClosedDate) - UNIX_TIMESTAMP(OpenedDate)) / 3600.0 AS DECIMAL(10,2))
    ELSE NULL
  END AS ResolutionTimeHours,
  CASE 
    WHEN Status = 'Closed' THEN TRUE
    ELSE FALSE
  END AS IsClosed,
  _ingestion_timestamp,
  _source_system,
  _source_table
FROM customer_service_tickets_bronze;

