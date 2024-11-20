from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("TigerGraph Data Write") \
    .getOrCreate()

# Step 2: Define TigerGraph connection parameters
tg_options = {
    "tg.url": "http://<YOUR_TIGERGRAPH_SERVER>:9000",  # Replace with your TigerGraph server URL
    "tg.token": "<YOUR_ACCESS_TOKEN>",                # Replace with your TigerGraph access token
    "tg.graph": "<GRAPH_NAME>",                       # Replace with your TigerGraph graph name
    "tg.parallelism": "4",                            # Adjust parallelism for your cluster
}

# Step 3: Create a sample DataFrame to write into TigerGraph
data = [
    ("C1", "Customer One", "customer1@example.com"),
    ("C2", "Customer Two", "customer2@example.com"),
    ("C3", "Customer Three", "customer3@example.com"),
    ("C4", "Customer Four", "customer4@example.com")
]

# Define column names
columns = ["customer_id", "customer_name", "email"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame content
print("Data to be written to TigerGraph:")
df.show()

# Step 4: Write DataFrame into TigerGraph
# Specify the vertex type and the mapping between DataFrame columns and TigerGraph vertex attributes
df.write \
    .format("com.tigergraph.spark.connector") \
    .options(**tg_options) \
    .option("tg.vertexType", "Customer")  # Replace "Customer" with your TigerGraph vertex type
    .mode("append") \
    .save()

print("Data successfully written to TigerGraph.")

# Stop the Spark session
spark.stop()
