# =============================================================================
# NOTEBOOK 01 — BRONZE LAYER: Raw CSV Ingestion
# Project: Medallion Lakehouse — Indian E-Commerce Sales
# Layer: Bronze (raw, untransformed)
# Author: Sayanti Chowdhury
# =============================================================================
# PURPOSE:
#   Read raw CSV files from ADLS and write them as Delta tables in the Bronze
#   layer. No business transformations are applied here — Bronze is the
#   immutable raw source of truth.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime

spark = SparkSession.builder.appName("Bronze_Ingestion_ECommerce").getOrCreate()

# =============================================================================
# CONFIGURATION
# =============================================================================

STORAGE_ACCOUNT = "yourstorageaccount"   # Replace with your ADLS account name
CONTAINER       = "ecommerce"
RAW_PATH        = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw"
BRONZE_PATH     = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/delta"

BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# =============================================================================
# HELPER: Add standard Bronze metadata columns
# =============================================================================

def add_bronze_metadata(df, source_name):
    return (df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file",         input_file_name())
        .withColumn("_batch_id",            lit(BATCH_ID))
        .withColumn("_source_name",         lit(source_name))
    )

# =============================================================================
# INGEST: List of Orders (order headers)
# Columns: Order ID, Order Date, CustomerName, State, City
# =============================================================================

print("Ingesting List of Orders...")

orders_raw = spark.read.csv(
    f"{RAW_PATH}/List of Orders.csv",
    header=True,
    inferSchema=True
)

orders_bronze = add_bronze_metadata(orders_raw, "list_of_orders")

(orders_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_PATH}/orders")
)

print(f"  Orders ingested: {orders_bronze.count()} rows")

# =============================================================================
# INGEST: Order Details (line items)
# Columns: Order ID, Amount, Profit, Quantity, Category, Sub-Category
# =============================================================================

print("Ingesting Order Details...")

order_details_raw = spark.read.csv(
    f"{RAW_PATH}/Order Details.csv",
    header=True,
    inferSchema=True
)

order_details_bronze = add_bronze_metadata(order_details_raw, "order_details")

(order_details_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_PATH}/order_details")
)

print(f"  Order Details ingested: {order_details_bronze.count()} rows")

# =============================================================================
# INGEST: Sales Targets (monthly category targets)
# Columns: Month of Order Date, Category, Target
# =============================================================================

print("Ingesting Sales Targets...")

targets_raw = spark.read.csv(
    f"{RAW_PATH}/Sales Target.csv",
    header=True,
    inferSchema=True
)

targets_bronze = add_bronze_metadata(targets_raw, "sales_target")

(targets_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_PATH}/sales_targets")
)

print(f"  Sales Targets ingested: {targets_bronze.count()} rows")

# =============================================================================
# VALIDATION: Basic row count check
# =============================================================================

print("\n--- Bronze Layer Summary ---")
for table, path in [("orders", "orders"), ("order_details", "order_details"), ("targets", "sales_targets")]:
    count = spark.read.format("delta").load(f"{BRONZE_PATH}/{path}").count()
    print(f"  {table}: {count} rows")

print(f"\nBronze ingestion complete. Batch ID: {BATCH_ID}")
