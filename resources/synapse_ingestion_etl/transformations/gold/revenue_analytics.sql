-- Gold Layer: Revenue Analytics
-- Comprehensive revenue metrics by plan, customer segment, and payment status
CREATE OR REFRESH MATERIALIZED VIEW revenue_analytics
COMMENT "Gold layer: Revenue metrics and payment analytics"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  -- Plan Information
  p.PlanID,
  p.PlanName,
  p.PlanType,
  p.MonthlyPrice,
  
  -- Customer Counts
  COUNT(DISTINCT s.CustomerID) AS TotalCustomers,
  COUNT(DISTINCT CASE WHEN c.AccountStatus = 'Active' THEN s.CustomerID END) AS ActiveCustomers,
  COUNT(DISTINCT CASE WHEN c.AccountStatus = 'Suspended' THEN s.CustomerID END) AS SuspendedCustomers,
  COUNT(DISTINCT CASE WHEN c.AccountStatus = 'Inactive' THEN s.CustomerID END) AS InactiveCustomers,
  
  -- Revenue Metrics
  SUM(b.TotalAmount) AS TotalRevenue,
  SUM(b.PaidAmount) AS CollectedRevenue,
  SUM(b.OutstandingAmount) AS OutstandingRevenue,
  AVG(b.TotalAmount) AS AvgBillAmount,
  
  -- Payment Status Breakdown
  COUNT(DISTINCT CASE WHEN b.PaymentStatus = 'Paid' THEN b.BillingID END) AS PaidBills,
  COUNT(DISTINCT CASE WHEN b.PaymentStatus = 'Pending' THEN b.BillingID END) AS PendingBills,
  COUNT(DISTINCT CASE WHEN b.PaymentStatus = 'Overdue' THEN b.BillingID END) AS OverdueBills,
  
  -- Payment Method Distribution
  COUNT(DISTINCT CASE WHEN b.PaymentMethod = 'Credit Card' THEN b.BillingID END) AS CreditCardPayments,
  COUNT(DISTINCT CASE WHEN b.PaymentMethod = 'Debit Card' THEN b.BillingID END) AS DebitCardPayments,
  COUNT(DISTINCT CASE WHEN b.PaymentMethod = 'Auto-Pay' THEN b.BillingID END) AS AutoPayPayments,
  COUNT(DISTINCT CASE WHEN b.PaymentMethod = 'Bank Transfer' THEN b.BillingID END) AS BankTransferPayments,
  COUNT(DISTINCT CASE WHEN b.PaymentMethod = 'Cash' THEN b.BillingID END) AS CashPayments,
  
  -- Collection Metrics
  ROUND(SUM(b.PaidAmount) / NULLIF(SUM(b.TotalAmount), 0) * 100, 2) AS CollectionRate,
  
  -- Revenue Per Customer
  ROUND(SUM(b.TotalAmount) / NULLIF(COUNT(DISTINCT s.CustomerID), 0), 2) AS RevenuePerCustomer,
  
  -- Monthly Recurring Revenue (MRR)
  SUM(p.MonthlyPrice * CASE WHEN s.Status = 'Active' THEN 1 ELSE 0 END) AS MonthlyRecurringRevenue,
  
  CURRENT_TIMESTAMP() AS LastUpdated

FROM plans_silver p
LEFT JOIN subscriptions_silver s ON p.PlanID = s.PlanID
LEFT JOIN customers_silver c ON s.CustomerID = c.CustomerID
LEFT JOIN billing_silver b ON s.SubscriptionID = b.SubscriptionID
GROUP BY 
  p.PlanID,
  p.PlanName,
  p.PlanType,
  p.MonthlyPrice;

