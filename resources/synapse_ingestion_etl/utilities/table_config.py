"""
Configuration metadata for Synapse tables to be ingested.

This module defines the structure and metadata for all tables
being ingested from Azure Synapse into the Databricks lakehouse.
"""

# Table configuration for Synapse ingestion
SYNAPSE_TABLES = [
    {
        "synapse_table": "dbo.Customers",
        "bronze_table": "customers_bronze",
        "primary_key": "CustomerID",
        "description": "Customer master data including demographics and account status"
    },
    {
        "synapse_table": "dbo.Plans",
        "bronze_table": "plans_bronze",
        "primary_key": "PlanID",
        "description": "Mobile service plans with pricing and allowances"
    },
    {
        "synapse_table": "dbo.Subscriptions",
        "bronze_table": "subscriptions_bronze",
        "primary_key": "SubscriptionID",
        "description": "Customer subscription records linking customers to plans"
    },
    {
        "synapse_table": "dbo.Billing",
        "bronze_table": "billing_bronze",
        "primary_key": "BillingID",
        "description": "Billing transactions and payment records"
    },
    {
        "synapse_table": "dbo.UsageData",
        "bronze_table": "usage_data_bronze",
        "primary_key": "UsageID",
        "description": "Customer usage data for data, voice, and text services"
    },
    {
        "synapse_table": "dbo.CustomerServiceTickets",
        "bronze_table": "customer_service_tickets_bronze",
        "primary_key": "TicketID",
        "description": "Customer support tickets and issue tracking"
    },
    {
        "synapse_table": "dbo.DeviceInventory",
        "bronze_table": "device_inventory_bronze",
        "primary_key": "DeviceID",
        "description": "Device inventory including customer devices and stock"
    },
    {
        "synapse_table": "dbo.NetworkTowers",
        "bronze_table": "network_towers_bronze",
        "primary_key": "TowerID",
        "description": "Network tower infrastructure and coverage information"
    }
]


def get_table_config(synapse_table: str = None, bronze_table: str = None):
    """
    Get configuration for a specific table.
    
    Args:
        synapse_table: Synapse table name (e.g., "dbo.Customers")
        bronze_table: Bronze table name (e.g., "customers_bronze")
        
    Returns:
        Table configuration dictionary or None if not found
    """
    if synapse_table:
        return next((t for t in SYNAPSE_TABLES if t["synapse_table"] == synapse_table), None)
    elif bronze_table:
        return next((t for t in SYNAPSE_TABLES if t["bronze_table"] == bronze_table), None)
    return None


def get_all_synapse_tables():
    """Get list of all Synapse table names."""
    return [t["synapse_table"] for t in SYNAPSE_TABLES]


def get_all_bronze_tables():
    """Get list of all Bronze table names."""
    return [t["bronze_table"] for t in SYNAPSE_TABLES]

