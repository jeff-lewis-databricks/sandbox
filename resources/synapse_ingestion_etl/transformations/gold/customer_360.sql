-- Gold Layer: Customer 360 View
-- Comprehensive customer profile combining all customer touchpoints
CREATE OR REFRESH MATERIALIZED VIEW customer_360
COMMENT "Gold layer: Complete customer profile with subscription, billing, usage, and support metrics"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Customer Demographics
  c.CustomerID,
  c.FirstName,
  c.LastName,
  c.Email,
  c.PhoneNumber,
  c.DateOfBirth,
  FLOOR(DATEDIFF(CURRENT_DATE(), c.DateOfBirth) / 365.25) AS Age,
  c.AccountStatus,
  c.CustomerSince,
  DATEDIFF(CURRENT_DATE(), c.CustomerSince) AS DaysSinceJoined,
  c.City,
  c.State,
  c.CreditScore,
  
  -- Current Subscription
  s.SubscriptionID,
  s.PlanID,
  p.PlanName,
  p.PlanType,
  p.MonthlyPrice,
  s.Status AS SubscriptionStatus,
  s.AutoRenew,
  
  -- Billing Metrics
  b.TotalAmount AS CurrentBillAmount,
  b.PaidAmount AS CurrentPaidAmount,
  b.OutstandingAmount AS CurrentAmountDue,
  b.PaymentStatus AS CurrentPaymentStatus,
  b.PaymentMethod,
  
  -- Usage Metrics (Last 30 days)
  COALESCE(u.TotalDataUsedGB, 0) AS DataUsedGB_Last30Days,
  COALESCE(u.TotalVoiceMinutes, 0) AS VoiceMinutes_Last30Days,
  COALESCE(u.TotalTexts, 0) AS Texts_Last30Days,
  COALESCE(u.TotalRoamingCharges, 0) AS RoamingCharges_Last30Days,
  
  -- Support Metrics
  COALESCE(t.TotalTickets, 0) AS TotalSupportTickets,
  COALESCE(t.OpenTickets, 0) AS OpenSupportTickets,
  COALESCE(t.AvgResolutionTimeHours, 0) AS AvgTicketResolutionHours,
  
  -- Device Information
  d.DeviceType,
  d.Manufacturer AS DeviceManufacturer,
  d.Model AS DeviceModel,
  d.IsWarrantyExpired,
  
  -- Customer Segment
  CASE
    WHEN p.MonthlyPrice >= 90 THEN 'Premium'
    WHEN p.MonthlyPrice >= 70 THEN 'Standard'
    WHEN p.MonthlyPrice >= 40 THEN 'Basic'
    ELSE 'Economy'
  END AS CustomerSegment,
  
  -- Churn Risk Indicators
  CASE
    WHEN c.AccountStatus = 'Suspended' THEN 'High'
    WHEN b.PaymentStatus = 'Overdue' THEN 'High'
    WHEN COALESCE(t.OpenTickets, 0) > 2 THEN 'Medium'
    WHEN s.AutoRenew = FALSE THEN 'Medium'
    ELSE 'Low'
  END AS ChurnRisk,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM customers_silver c
LEFT JOIN subscriptions_silver s ON c.CustomerID = s.CustomerID AND s.Status = 'Active'
LEFT JOIN plans_silver p ON s.PlanID = p.PlanID
LEFT JOIN billing_silver b ON c.CustomerID = b.CustomerID
LEFT JOIN device_inventory_silver d ON c.CustomerID = d.CustomerID AND d.Status = 'Active'
LEFT JOIN (
  SELECT
    CustomerID,
    SUM(DataUsedGB) AS TotalDataUsedGB,
    SUM(VoiceMinutesUsed) AS TotalVoiceMinutes,
    SUM(TextsSent) AS TotalTexts,
    SUM(RoamingCharges) AS TotalRoamingCharges
  FROM usage_data_silver
  WHERE UsageDate >= DATE_SUB(CURRENT_DATE(), 30)
  GROUP BY CustomerID
) u ON c.CustomerID = u.CustomerID
LEFT JOIN (
  SELECT
    CustomerID,
    COUNT(*) AS TotalTickets,
    SUM(CASE WHEN IsClosed = FALSE THEN 1 ELSE 0 END) AS OpenTickets,
    AVG(ResolutionTimeHours) AS AvgResolutionTimeHours
  FROM customer_service_tickets_silver
  GROUP BY CustomerID
) t ON c.CustomerID = t.CustomerID;

