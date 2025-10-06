# 🚀 Quick Start Guide

## Prerequisites Checklist

- [ ] Azure Synapse workspace accessible
- [ ] Databricks workspace with Unity Catalog
- [ ] Azure Access Connector configured (credential: `azure-synapse`)
- [ ] Catalog and schema created in Unity Catalog
- [ ] Network connectivity: Databricks ↔ Synapse

## 🎯 Deploy & Run (3 Steps)

### Step 1: Validate Configuration

```bash
cd synapse_ingestion
databricks bundle validate
```

**Expected output:** ✅ Configuration is valid

### Step 2: Deploy Pipeline

```bash
# Deploy to dev environment
databricks bundle deploy -t dev
```

**What this does:**
- Creates DLT pipeline in Databricks
- Uploads all transformation code
- Configures pipeline with your Synapse connection

### Step 3: Run Pipeline

**Option A: Command Line**
```bash
databricks bundle run synapse_ingestion_etl -t dev
```

**Option B: Databricks UI**
1. Go to **Workflows** → **Delta Live Tables**
2. Find pipeline: `[dev your_name] synapse_ingestion_etl`
3. Click **Start**
4. Monitor progress in the pipeline graph

## 📊 What Gets Created

### Bronze Layer (8 tables)
- `customers_bronze`
- `plans_bronze`
- `subscriptions_bronze`
- `billing_bronze`
- `usage_data_bronze`
- `customer_service_tickets_bronze`
- `device_inventory_bronze`
- `network_towers_bronze`

### Silver Layer (8 tables)
- `customers_silver`
- `plans_silver`
- `subscriptions_silver`
- `billing_silver`
- `usage_data_silver`
- `customer_service_tickets_silver`
- `device_inventory_silver`
- `network_towers_silver`

### Gold Layer (7 analytics tables)
- `customer_360` - Complete customer profiles
- `revenue_analytics` - Revenue metrics by plan
- `usage_analytics` - Usage patterns and trends
- `churn_risk_analysis` - Churn prediction scores
- `network_coverage_analysis` - Network performance
- `device_lifecycle_management` - Device inventory
- `support_metrics` - Customer service analytics

## 🔍 Verify Success

### Check Pipeline Status
```sql
-- In Databricks SQL Editor
SELECT * FROM customer_360 LIMIT 10;
```

### Expected Results
- ✅ All 23 tables created (8 bronze + 8 silver + 7 gold)
- ✅ Data loaded from Synapse
- ✅ Quality checks passed
- ✅ No pipeline errors

## 📈 Explore the Data

### Customer 360 View
```sql
SELECT 
  CustomerID,
  FirstName,
  LastName,
  CustomerSegment,
  ChurnRisk,
  CurrentBillAmount,
  DataUsedGB_Last30Days
FROM customer_360
WHERE ChurnRisk = 'High'
ORDER BY CurrentBillAmount DESC;
```

### Revenue by Plan
```sql
SELECT 
  PlanName,
  TotalCustomers,
  TotalRevenue,
  CollectionRate,
  MonthlyRecurringRevenue
FROM revenue_analytics
ORDER BY TotalRevenue DESC;
```

### Churn Risk Analysis
```sql
SELECT 
  ChurnRiskCategory,
  COUNT(*) as CustomerCount,
  AVG(ChurnRiskScore) as AvgRiskScore,
  SUM(EstimatedLifetimeValue) as TotalLTV
FROM churn_risk_analysis
GROUP BY ChurnRiskCategory
ORDER BY AvgRiskScore DESC;
```

### Usage Patterns
```sql
SELECT 
  DataUsageProfile,
  VoiceUsageProfile,
  COUNT(*) as CustomerCount,
  AVG(TotalDataUsedGB) as AvgDataGB,
  COUNT(CASE WHEN ShouldUpgradePlan THEN 1 END) as UpgradeOpportunities
FROM usage_analytics
GROUP BY DataUsageProfile, VoiceUsageProfile;
```

## 🛠️ Troubleshooting

### Issue: "Credential not found"
**Solution:** Verify Access Connector is configured:
```python
# Run in Databricks notebook
credential = dbutils.credentials.getServiceCredentialsProvider('azure-synapse')
token = credential.get_token('https://database.windows.net/.default').token
print("✅ Credential configured correctly")
```

### Issue: "Cannot connect to Synapse"
**Solution:** Check network connectivity and firewall rules:
- Databricks workspace IP must be allowed in Synapse firewall
- Verify server name: `jlewis-synapse-workspace.sql.azuresynapse.net`
- Verify database name: `jlewisdedicatedsqlpool`

### Issue: "Table not found in Synapse"
**Solution:** Verify tables exist in Synapse:
```sql
-- Run in Synapse
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'dbo';
```

Expected tables:
- dbo.Customers
- dbo.Plans
- dbo.Subscriptions
- dbo.Billing
- dbo.UsageData
- dbo.CustomerServiceTickets
- dbo.DeviceInventory
- dbo.NetworkTowers

### Issue: "Pipeline fails with quality violations"
**Solution:** Check DLT expectations in pipeline UI:
1. Go to pipeline run details
2. Click on failed table
3. Review "Expectations" tab
4. Fix data quality issues in source

## 🎓 Next Steps

1. **Explore Gold Tables:** Query the 7 analytics tables
2. **Create Dashboards:** Build visualizations in Databricks SQL
3. **Add More Analytics:** Create custom Gold layer tables
4. **Schedule Pipeline:** Add trigger for automated runs (if needed)
5. **Monitor Quality:** Review DLT expectations and metrics

## 📚 Key Concepts

- **Bronze Layer:** Raw data from Synapse (minimal transformation)
- **Silver Layer:** Cleaned, validated, typed data
- **Gold Layer:** Business-ready analytics and aggregations
- **DLT Expectations:** Data quality constraints
- **Streaming Tables:** Incremental processing
- **Medallion Architecture:** Bronze → Silver → Gold pattern

## 💡 Pro Tips

1. **View Lineage:** Click on any table in DLT UI to see data lineage
2. **Monitor Quality:** Check "Data Quality" tab for expectation metrics
3. **Optimize Performance:** Pipeline uses serverless compute (auto-scaling)
4. **Debug Issues:** Use "Event Log" in pipeline UI for detailed errors
5. **Incremental Updates:** Silver/Gold use streaming for efficiency

---

**Ready to go!** 🎉 Run the pipeline and explore your telecom analytics!

