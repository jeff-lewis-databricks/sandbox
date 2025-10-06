"""
Synapse connector utility for reading data from Azure Synapse.

This module provides reusable functions for connecting to and reading
data from Azure Synapse SQL pools via JDBC using Azure Access Connector
for token-based authentication.
"""
from pyspark.sql import DataFrame
from typing import Dict, Optional


def get_synapse_access_token(credential_name: str = 'azure-synapse') -> str:
    """
    Gets Azure Synapse access token using Databricks Access Connector.
    
    Args:
        credential_name: Name of the configured credential in Databricks
        
    Returns:
        Access token string for Azure SQL authentication
        
    Example:
        token = get_synapse_access_token('azure-synapse')
    """
    # Import dbutils from global scope - available in Databricks runtime
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        dbutils = DBUtils(spark)
    except:
        # Fallback for notebook environment
        import IPython
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
    
    credential = dbutils.credentials.getServiceCredentialsProvider(credential_name)
    return credential.get_token('https://database.windows.net/.default').token


def get_synapse_connection_properties(access_token: str) -> Dict[str, str]:
    """
    Constructs JDBC connection properties for Synapse with token authentication.
    
    Args:
        access_token: Azure SQL access token
        
    Returns:
        Dictionary of JDBC connection properties
    """
    return {
        "accessToken": access_token,
        "hostNameInCertificate": "*.sql.azuresynapse.net",
        "encrypt": "true",
        "trustServerCertificate": "false",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }


def get_synapse_jdbc_url(server: str, database: str, port: int = 1433) -> str:
    """
    Constructs JDBC URL for Azure Synapse connection.
    
    Args:
        server: Synapse workspace server name (e.g., "myworkspace.sql.azuresynapse.net")
        database: Database/SQL pool name
        port: Port number (default: 1433)
        
    Returns:
        JDBC connection URL string
    """
    return f"jdbc:sqlserver://{server}:{port};database={database}"


def read_synapse_table_with_token(
    spark,
    server: str,
    database: str,
    table: str,
    credential_name: str = 'azure-synapse',
    port: int = 1433
) -> DataFrame:
    """
    Reads a table from Azure Synapse using token-based authentication.
    
    This function uses Azure Access Connector for Databricks to retrieve
    an OAuth token for secure, password-less authentication to Synapse.
    
    Args:
        spark: SparkSession object
        server: Synapse workspace server (e.g., "myworkspace.sql.azuresynapse.net")
        database: Database/SQL pool name
        table: Table name (e.g., "dbo.TableName")
        credential_name: Name of configured credential in Databricks (default: 'azure-synapse')
        port: Port number (default: 1433)
        
    Returns:
        DataFrame containing the table data
        
    Example usage in a DLT pipeline:
        from pyspark import pipelines as dp
        from utilities import synapse_connector
        
        @dp.table()
        def customers_bronze():
            return synapse_connector.read_synapse_table_with_token(
                spark=spark,
                server="myworkspace.sql.azuresynapse.net",
                database="mydatabase",
                table="dbo.Customers"
            )
    """
    # Get access token
    access_token = get_synapse_access_token(credential_name)
    
    # Build connection properties
    connection_properties = get_synapse_connection_properties(access_token)
    
    # Build JDBC URL
    jdbc_url = get_synapse_jdbc_url(server, database, port)
    
    # Read table
    return spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties=connection_properties
    )


def read_synapse_query_with_token(
    spark,
    server: str,
    database: str,
    query: str,
    credential_name: str = 'azure-synapse',
    port: int = 1433
) -> DataFrame:
    """
    Executes a custom SQL query on Azure Synapse using token-based authentication.
    
    Args:
        spark: SparkSession object
        server: Synapse workspace server
        database: Database/SQL pool name
        query: SQL query to execute
        credential_name: Name of configured credential (default: 'azure-synapse')
        port: Port number (default: 1433)
        
    Returns:
        DataFrame containing the query results
        
    Example:
        query = '''
            SELECT CustomerID, COUNT(*) as order_count
            FROM dbo.Orders
            WHERE OrderDate >= '2024-01-01'
            GROUP BY CustomerID
        '''
        df = read_synapse_query_with_token(
            spark=spark,
            server="myworkspace.sql.azuresynapse.net",
            database="mydatabase",
            query=query
        )
    """
    # Wrap query for JDBC
    query_wrapped = f"({query}) AS query"
    
    return read_synapse_table_with_token(
        spark=spark,
        server=server,
        database=database,
        table=query_wrapped,
        credential_name=credential_name,
        port=port
    )