-- Gold Layer: Churn Risk Analysis
-- Identify customers at risk of churning with actionable insights
CREATE OR REFRESH MATERIALIZED VIEW churn_risk_analysis
COMMENT "Gold layer: Customer churn risk scoring and retention insights"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Customer Info
  c.CustomerID,
  c.FirstName,
  c.LastName,
  c.Email,
  c.PhoneNumber,
  c.AccountStatus,
  c.CustomerSince,
  DATEDIFF(CURRENT_DATE(), c.CustomerSince) AS CustomerTenureDays,
  
  -- Subscription Info
  s.Status AS SubscriptionStatus,
  s.AutoRenew,
  p.PlanName,
  p.MonthlyPrice,
  
  -- Churn Risk Factors
  CASE WHEN c.AccountStatus = 'Suspended' THEN 1 ELSE 0 END AS IsSuspended,
  CASE WHEN c.AccountStatus = 'Inactive' THEN 1 ELSE 0 END AS IsInactive,
  CASE WHEN s.AutoRenew = FALSE THEN 1 ELSE 0 END AS NoAutoRenew,
  CASE WHEN b.PaymentStatus = 'Overdue' THEN 1 ELSE 0 END AS HasOverduePayment,
  CASE WHEN b.PaymentStatus = 'Pending' THEN 1 ELSE 0 END AS HasPendingPayment,
  COALESCE(t.OpenTickets, 0) AS OpenSupportTickets,
  COALESCE(t.TotalTickets, 0) AS TotalSupportTickets,
  COALESCE(t.HighPriorityTickets, 0) AS HighPriorityTickets,
  CASE WHEN d.IsWarrantyExpired = TRUE THEN 1 ELSE 0 END AS DeviceWarrantyExpired,
  
  -- Usage Indicators
  COALESCE(u.DaysSinceLastUsage, 999) AS DaysSinceLastUsage,
  CASE WHEN COALESCE(u.DaysSinceLastUsage, 999) > 7 THEN 1 ELSE 0 END AS NoRecentUsage,
  
  -- Billing Metrics
  b.OutstandingAmount AS AmountDue,
  b.TotalAmount AS CurrentBillAmount,
  
  -- Calculate Churn Risk Score (0-100)
  (
    (CASE WHEN c.AccountStatus = 'Suspended' THEN 30 ELSE 0 END) +
    (CASE WHEN c.AccountStatus = 'Inactive' THEN 40 ELSE 0 END) +
    (CASE WHEN s.AutoRenew = FALSE THEN 15 ELSE 0 END) +
    (CASE WHEN b.PaymentStatus = 'Overdue' THEN 25 ELSE 0 END) +
    (CASE WHEN b.PaymentStatus = 'Pending' THEN 10 ELSE 0 END) +
    (CASE WHEN COALESCE(t.OpenTickets, 0) > 2 THEN 15 ELSE 0 END) +
    (CASE WHEN COALESCE(t.HighPriorityTickets, 0) > 0 THEN 10 ELSE 0 END) +
    (CASE WHEN COALESCE(u.DaysSinceLastUsage, 999) > 7 THEN 20 ELSE 0 END) +
    (CASE WHEN d.IsWarrantyExpired = TRUE THEN 5 ELSE 0 END)
  ) AS ChurnRiskScore,
  
  -- Churn Risk Category
  CASE
    WHEN c.AccountStatus = 'Inactive' THEN 'Critical'
    WHEN c.AccountStatus = 'Suspended' OR b.PaymentStatus = 'Overdue' THEN 'High'
    WHEN (
      (s.AutoRenew = FALSE) OR
      (COALESCE(t.OpenTickets, 0) > 2) OR
      (COALESCE(u.DaysSinceLastUsage, 999) > 7)
    ) THEN 'Medium'
    ELSE 'Low'
  END AS ChurnRiskCategory,
  
  -- Recommended Actions
  CONCAT_WS('; ',
    CASE WHEN b.PaymentStatus = 'Overdue' THEN 'Contact for payment' END,
    CASE WHEN s.AutoRenew = FALSE THEN 'Enable auto-renew' END,
    CASE WHEN COALESCE(t.OpenTickets, 0) > 0 THEN 'Resolve open tickets' END,
    CASE WHEN COALESCE(u.DaysSinceLastUsage, 999) > 7 THEN 'Re-engagement campaign' END,
    CASE WHEN d.IsWarrantyExpired = TRUE THEN 'Offer device upgrade' END,
    CASE WHEN c.AccountStatus = 'Suspended' THEN 'Account recovery outreach' END
  ) AS RecommendedActions,
  
  -- Lifetime Value Estimate
  ROUND(p.MonthlyPrice * (DATEDIFF(CURRENT_DATE(), c.CustomerSince) / 30.0), 2) AS EstimatedLifetimeValue,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM customers_silver c
LEFT JOIN subscriptions_silver s ON c.CustomerID = s.CustomerID
LEFT JOIN plans_silver p ON s.PlanID = p.PlanID
LEFT JOIN billing_silver b ON c.CustomerID = b.CustomerID
LEFT JOIN device_inventory_silver d ON c.CustomerID = d.CustomerID AND d.Status = 'Active'
LEFT JOIN (
  SELECT
    CustomerID,
    COUNT(*) AS TotalTickets,
    SUM(CASE WHEN IsClosed = FALSE THEN 1 ELSE 0 END) AS OpenTickets,
    SUM(CASE WHEN Priority IN ('High', 'Critical') THEN 1 ELSE 0 END) AS HighPriorityTickets
  FROM customer_service_tickets_silver
  GROUP BY CustomerID
) t ON c.CustomerID = t.CustomerID
LEFT JOIN (
  SELECT
    CustomerID,
    DATEDIFF(CURRENT_DATE(), MAX(UsageDate)) AS DaysSinceLastUsage
  FROM usage_data_silver
  GROUP BY CustomerID
) u ON c.CustomerID = u.CustomerID;

