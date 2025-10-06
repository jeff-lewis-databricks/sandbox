"""
Bronze Layer: Ingest all tables from Azure Synapse SQL Pool.

This module dynamically creates DLT tables for all configured Synapse tables.
Each table is ingested as-is with minimal transformation, adding only metadata
for tracking and lineage.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import synapse_connector
from utilities.table_config import SYNAPSE_TABLES


# Get Synapse connection parameters from pipeline configuration
synapse_server = spark.conf.get("synapse_server")
synapse_database = spark.conf.get("synapse_database")
synapse_credential = spark.conf.get("synapse_credential", "azure-synapse")


# Dynamically create bronze tables for each Synapse table
for table_config in SYNAPSE_TABLES:
    synapse_table = table_config["synapse_table"]
    bronze_table = table_config["bronze_table"]
    description = table_config["description"]
    
    # Create a function for this specific table
    # We use exec to dynamically create the function with the correct name
    exec(f"""
@dp.table(
    name="{bronze_table}",
    comment="Bronze layer: {description}",
    table_properties={{
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "source_system": "azure_synapse",
        "source_table": "{synapse_table}"
    }}
)
def {bronze_table}():
    '''
    Ingests {synapse_table} from Azure Synapse.
    
    Bronze layer tables contain raw data with minimal transformation:
    - All source columns preserved
    - Ingestion timestamp added for tracking
    - Source system metadata added
    '''
    return (
        synapse_connector.read_synapse_table_with_token(
            spark=spark,
            server=synapse_server,
            database=synapse_database,
            table="{synapse_table}",
            credential_name=synapse_credential
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("azure_synapse"))
        .withColumn("_source_table", F.lit("{synapse_table}"))
    )
""")

print(f"✓ Created {len(SYNAPSE_TABLES)} bronze layer tables dynamically")

