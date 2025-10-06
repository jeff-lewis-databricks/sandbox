-- Gold Layer: Support Metrics
-- Customer service performance and issue analysis
CREATE OR REFRESH MATERIALIZED VIEW support_metrics
COMMENT "Gold layer: Customer support performance metrics and issue tracking"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Issue Categorization
  IssueType,
  Priority,
  
  -- Ticket Volume Metrics
  COUNT(*) AS TotalTickets,
  COUNT(CASE WHEN IsClosed = TRUE THEN 1 END) AS ClosedTickets,
  COUNT(CASE WHEN IsClosed = FALSE THEN 1 END) AS OpenTickets,
  COUNT(CASE WHEN Status = 'Pending' THEN 1 END) AS PendingTickets,
  COUNT(CASE WHEN Status = 'In Progress' THEN 1 END) AS InProgressTickets,
  
  -- Resolution Metrics
  AVG(CASE WHEN IsClosed = TRUE THEN ResolutionTimeHours END) AS AvgResolutionTimeHours,
  MIN(CASE WHEN IsClosed = TRUE THEN ResolutionTimeHours END) AS MinResolutionTimeHours,
  MAX(CASE WHEN IsClosed = TRUE THEN ResolutionTimeHours END) AS MaxResolutionTimeHours,
  PERCENTILE(CASE WHEN IsClosed = TRUE THEN ResolutionTimeHours END, 0.5) AS MedianResolutionTimeHours,
  
  -- Resolution Rate
  ROUND(COUNT(CASE WHEN IsClosed = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ResolutionRate,
  
  -- Resolution Type Distribution
  COUNT(CASE WHEN Resolution = 'Issue resolved' THEN 1 END) AS IssuesResolved,
  COUNT(CASE WHEN Resolution = 'Provided workaround' THEN 1 END) AS WorkaroundsProvided,
  COUNT(CASE WHEN Resolution = 'Escalated to tech' THEN 1 END) AS EscalatedToTech,
  COUNT(CASE WHEN Resolution = 'Customer satisfied' THEN 1 END) AS CustomerSatisfied,
  
  -- Agent Performance
  COUNT(DISTINCT AgentID) AS UniqueAgentsHandling,
  ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT AgentID), 0), 2) AS AvgTicketsPerAgent,
  
  -- Time-based Metrics
  MIN(OpenedDate) AS FirstTicketDate,
  MAX(OpenedDate) AS LastTicketDate,
  COUNT(CASE WHEN OpenedDate >= DATE_SUB(CURRENT_DATE(), 7) THEN 1 END) AS TicketsLast7Days,
  COUNT(CASE WHEN OpenedDate >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS TicketsLast30Days,
  
  -- Priority Distribution
  ROUND(COUNT(CASE WHEN Priority = 'Critical' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS PercentCritical,
  ROUND(COUNT(CASE WHEN Priority = 'High' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS PercentHigh,
  ROUND(COUNT(CASE WHEN Priority = 'Medium' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS PercentMedium,
  ROUND(COUNT(CASE WHEN Priority = 'Low' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS PercentLow,
  
  -- SLA Compliance (assuming 24 hours for Critical, 48 for High, 72 for Medium, 168 for Low)
  ROUND(COUNT(CASE 
    WHEN IsClosed = TRUE AND (
      (Priority = 'Critical' AND ResolutionTimeHours <= 24) OR
      (Priority = 'High' AND ResolutionTimeHours <= 48) OR
      (Priority = 'Medium' AND ResolutionTimeHours <= 72) OR
      (Priority = 'Low' AND ResolutionTimeHours <= 168)
    ) THEN 1 
  END) * 100.0 / NULLIF(COUNT(CASE WHEN IsClosed = TRUE THEN 1 END), 0), 2) AS SLAComplianceRate,
  
  -- Issue Severity Score (weighted by priority)
  ROUND(
    (COUNT(CASE WHEN Priority = 'Critical' THEN 1 END) * 4 +
     COUNT(CASE WHEN Priority = 'High' THEN 1 END) * 3 +
     COUNT(CASE WHEN Priority = 'Medium' THEN 1 END) * 2 +
     COUNT(CASE WHEN Priority = 'Low' THEN 1 END) * 1) * 1.0 / NULLIF(COUNT(*), 0), 2
  ) AS AvgIssueSeverityScore,
  
  -- Action Recommendations
  CASE
    WHEN COUNT(CASE WHEN IsClosed = FALSE THEN 1 END) > 10 THEN 'High backlog - increase staffing'
    WHEN AVG(CASE WHEN IsClosed = TRUE THEN ResolutionTimeHours END) > 48 THEN 'Slow resolution - process improvement needed'
    WHEN COUNT(CASE WHEN Priority = 'Critical' AND IsClosed = FALSE THEN 1 END) > 0 THEN 'Critical tickets pending - immediate attention required'
    ELSE 'Performance within acceptable range'
  END AS RecommendedAction,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM customer_service_tickets_silver
GROUP BY IssueType, Priority;

