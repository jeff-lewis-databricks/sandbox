from pyspark import pipelines as dp

# Get credentials from secrets
user = dbutils.secrets.get("jlewis-synapse", "synapseuser")
password = dbutils.secrets.get("jlewis-synapse", "synapsepass")

synapse_server = "jlewis-synapse.sql.azuresynapse.net"
database_name = "jlewispool"
table_name = "dbo.CustomerOrders"

jdbc_url = f"jdbc:sqlserver://{synapse_server}:1433;database={database_name};user={user};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

# Create materialized view from Synapse data
@dp.table()
def synapse_source_data():
        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .load()
    )