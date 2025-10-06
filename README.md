# Synapse Ingestion ETL Pipeline

A comprehensive Databricks Delta Live Tables (DLT) pipeline that ingests telecommunications data from Azure Synapse SQL Pool and transforms it into analytics-ready datasets using a medallion architecture.

## 📊 Overview

This pipeline demonstrates enterprise-grade data engineering practices for ingesting and transforming telecom customer data from Azure Synapse into Databricks Lakehouse.

### Data Sources (Azure Synapse)
- **Customers** - Customer master data (150 records)
- **Plans** - Mobile service plans (10 plans)
- **Subscriptions** - Customer subscriptions (150 records)
- **Billing** - Billing transactions (150 records)
- **UsageData** - Usage metrics (208 records)
- **CustomerServiceTickets** - Support tickets (200 records)
- **DeviceInventory** - Device inventory (180 devices)
- **NetworkTowers** - Network infrastructure (50 towers)

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│  Raw data ingestion from Synapse (8 tables)            │
│  • Minimal transformation                               │
│  • Metadata tracking (_ingestion_timestamp, etc.)      │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER                         │
│  Cleaned and validated data (8 tables)                 │
│  • Data quality constraints                             │
│  • Type casting and standardization                     │
│  • Calculated fields                                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│                     GOLD LAYER                          │
│  Business-ready analytics tables (7 tables)            │
│  • customer_360 - Complete customer profiles           │
│  • revenue_analytics - Revenue and payment metrics     │
│  • usage_analytics - Usage patterns and trends         │
│  • churn_risk_analysis - Churn prediction and scoring  │
│  • network_coverage_analysis - Network performance     │
│  • device_lifecycle_management - Device inventory      │
│  • support_metrics - Customer service analytics        │
└─────────────────────────────────────────────────────────┘
```

## 🔐 Authentication

Uses **Azure Access Connector for Databricks** for secure, token-based authentication to Azure Synapse:
- No passwords stored in code
- OAuth 2.0 token-based authentication
- Managed identity integration

## 📁 Project Structure

```
synapse_ingestion/
├── databricks.yml                              # Databricks Asset Bundle configuration
├── README.md                                   # This file
└── resources/
    └── synapse_ingestion_etl/
        ├── synapse_ingestion_etl.pipeline.yml # DLT Pipeline configuration
        ├── utilities/
        │   ├── __init__.py
        │   ├── synapse_connector.py           # Synapse connection utilities
        │   └── table_config.py                # Table metadata configuration
        └── transformations/
            ├── bronze/
            │   └── ingest_synapse_tables.py   # Dynamic bronze layer ingestion
            ├── silver/
            │   ├── customers_silver.sql
            │   ├── plans_silver.sql
            │   ├── subscriptions_silver.sql
            │   ├── billing_silver.sql
            │   ├── usage_data_silver.sql
            │   ├── customer_service_tickets_silver.sql
            │   ├── device_inventory_silver.sql
            │   └── network_towers_silver.sql
            └── gold/
                ├── customer_360.sql
                ├── revenue_analytics.sql
                ├── usage_analytics.sql
                ├── churn_risk_analysis.sql
                ├── network_coverage_analysis.sql
                ├── device_lifecycle_management.sql
                └── support_metrics.sql
```

## 🚀 Getting Started

### Prerequisites

1. **Azure Synapse Setup:**
   - Synapse workspace with dedicated SQL pool
   - Tables loaded with telecom data
   - Network connectivity from Databricks to Synapse

2. **Databricks Setup:**
   - Databricks workspace
   - Azure Access Connector configured with credential name: `azure-synapse`
   - Unity Catalog enabled
   - Catalog and schema created

3. **Configuration:**
   Update `databricks.yml` with your values:
   ```yaml
   variables:
     synapse_server: "your-workspace.sql.azuresynapse.net"
     synapse_database: "your-sql-pool-name"
     synapse_credential: "azure-synapse"
   ```

### Deployment

1. **Validate the bundle:**
   ```bash
   databricks bundle validate
   ```

2. **Deploy to development:**
   ```bash
   databricks bundle deploy -t dev
   ```

3. **Run the pipeline:**
   ```bash
   databricks bundle run synapse_ingestion_etl -t dev
   ```

   Or run from the Databricks UI:
   - Navigate to **Workflows** → **Delta Live Tables**
   - Find `synapse_ingestion_etl` pipeline
   - Click **Start**

## 📊 Gold Layer Analytics

### 1. Customer 360 View
Complete customer profile combining:
- Demographics and account status
- Current subscription and plan details
- Billing and payment information
- Usage metrics (last 30 days)
- Support ticket history
- Device information
- Customer segmentation
- Churn risk scoring

**Key Metrics:**
- Customer lifetime value
- Days since joined
- Current amount due
- Data/voice/text usage
- Support ticket count

### 2. Revenue Analytics
Revenue and payment metrics by plan:
- Total and collected revenue
- Outstanding balances
- Payment status breakdown
- Payment method distribution
- Collection rates
- Monthly Recurring Revenue (MRR)
- Revenue per customer

### 3. Usage Analytics
Customer usage patterns and trends:
- Data, voice, and text consumption
- Usage vs. plan allowances
- Overage analysis
- Usage profiles (Heavy/Moderate/Light)
- Roaming charges
- Plan upgrade recommendations

### 4. Churn Risk Analysis
Predictive churn scoring with:
- Risk factors (suspended accounts, overdue payments, etc.)
- Churn risk score (0-100)
- Risk categories (Critical/High/Medium/Low)
- Recommended retention actions
- Lifetime value estimates

**Churn Risk Factors:**
- Account suspension/inactivity
- Overdue/pending payments
- High support ticket volume
- No auto-renew enabled
- Inactive usage patterns
- Expired device warranties

### 5. Network Coverage Analysis
Network infrastructure metrics:
- Tower counts by region
- 5G coverage percentage
- Operational status
- Customer coverage density
- Infrastructure age
- Expansion priorities

### 6. Device Lifecycle Management
Device inventory and upgrade opportunities:
- Device counts by manufacturer/model
- Warranty status tracking
- Device age analysis
- Upgrade opportunity identification
- Stock management alerts

### 7. Support Metrics
Customer service performance:
- Ticket volume by issue type and priority
- Resolution time metrics
- SLA compliance rates
- Agent performance
- Issue severity scoring
- Recommended actions

## 🎯 Key Features

### Data Quality
- **Constraint Enforcement:** NOT NULL, valid enums, range checks
- **Data Validation:** Email format, date consistency, numeric ranges
- **Quality Metrics:** Tracked via DLT expectations

### Performance Optimization
- **Auto-optimize:** Enabled on all tables
- **Streaming Tables:** Incremental processing in Silver/Gold layers
- **Serverless Compute:** Automatic scaling

### Observability
- **Lineage Tracking:** Full data lineage via DLT
- **Metadata:** Source system and ingestion timestamps
- **Quality Metrics:** Expectation violations tracked

## 🔧 Configuration

### Pipeline Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Unity Catalog name | Set per environment |
| `schema` | Schema name | Set per environment |
| `synapse_server` | Synapse workspace server | `jlewis-synapse-workspace.sql.azuresynapse.net` |
| `synapse_database` | SQL pool database name | `jlewisdedicatedsqlpool` |
| `synapse_credential` | Access Connector credential | `azure-synapse` |

### Environment Targets

- **sandbox:** Development environment with user-specific schema
- **dev:** Shared development environment
- **prod:** Production environment with strict permissions

## 📈 Demo Use Cases

This pipeline demonstrates:

1. **Enterprise Data Integration:** Synapse → Databricks migration pattern
2. **Medallion Architecture:** Bronze → Silver → Gold best practices
3. **Customer Analytics:** 360-degree customer view
4. **Predictive Analytics:** Churn risk scoring
5. **Operational Analytics:** Network and device management
6. **Business Intelligence:** Revenue and support metrics

## 🛠️ Extending the Pipeline

### Adding New Tables

1. Add table configuration to `utilities/table_config.py`:
   ```python
   {
       "synapse_table": "dbo.NewTable",
       "bronze_table": "new_table_bronze",
       "primary_key": "ID",
       "description": "Description"
   }
   ```

2. Create Silver transformation: `transformations/silver/new_table_silver.sql`

3. Bronze layer will automatically ingest the new table!

### Adding Gold Analytics

Create new SQL file in `transformations/gold/` with your analytics logic.

## 📝 Notes

- **Full Refresh:** Pipeline uses full refresh (suitable for demo/small datasets)
- **Data Volume:** Optimized for hundreds of records
- **Execution:** Run on-demand (no scheduling configured)
- **Security:** Token-based authentication (no passwords in code)

## 🎓 Learning Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Azure Access Connector](https://learn.microsoft.com/en-us/azure/databricks/security/access-connector)

## 📧 Support

For questions or issues with this pipeline, please refer to the Databricks documentation or contact your Databricks representative.

---

**Built with ❤️ using Databricks Delta Live Tables**