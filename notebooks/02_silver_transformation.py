# =============================================================================
# NOTEBOOK 02 — SILVER LAYER: Cleaning, Validation & Enrichment
# Project: Medallion Lakehouse — Indian E-Commerce Sales
# Layer: Silver (cleaned, validated, business-rule-applied)
# Author: Sayanti Chowdhury
# =============================================================================
# PURPOSE:
#   Apply data quality rules, standardisations, and business logic to Bronze
#   data. Silver is the single source of truth for analytics. Every row has a
#   dq_passed flag — downstream Gold only uses dq_passed = True rows.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, upper, trim, lit, coalesce,
    current_timestamp, round as spark_round, monotonically_increasing_id
)
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.appName("Silver_Transformation_ECommerce").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
CONTAINER       = "ecommerce"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/delta"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/delta"

# =============================================================================
# REFERENCE DATA: Valid Indian state names (standardisation lookup)
# =============================================================================

VALID_STATES = {
    "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR", "CHHATTISGARH",
    "GOA", "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JHARKHAND", "KARNATAKA",
    "KERALA", "MADHYA PRADESH", "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM",
    "NAGALAND", "ODISHA", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU",
    "TELANGANA", "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL",
    "DELHI", "JAMMU AND KASHMIR", "LADAKH"
}

valid_states_list = list(VALID_STATES)

# =============================================================================
# TRANSFORM: Orders (Silver)
# =============================================================================

print("Processing Orders → Silver...")

orders_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")

orders_silver = (orders_bronze
    # Clean whitespace
    .withColumn("Order ID",       trim(col("Order ID")))
    .withColumn("CustomerName",   trim(col("CustomerName")))
    .withColumn("State",          trim(upper(col("State"))))
    .withColumn("City",           trim(col("City")))

    # Parse date string → DateType
    .withColumn("Order Date",     to_date(col("Order Date"), "dd-MM-yyyy"))

    # Data Quality flags
    .withColumn("dq_order_id_null",      col("Order ID").isNull())
    .withColumn("dq_order_date_null",    col("Order Date").isNull())
    .withColumn("dq_customer_null",      col("CustomerName").isNull())
    .withColumn("dq_state_valid",        upper(trim(col("State"))).isin(valid_states_list))

    # Master DQ flag — True only if ALL critical rules pass
    .withColumn("dq_passed", (
        col("Order ID").isNotNull() &
        col("Order Date").isNotNull() &
        col("CustomerName").isNotNull()
    ))

    # Silver metadata
    .withColumn("_silver_processed_at", current_timestamp())
)

(orders_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/orders")
)

total = orders_silver.count()
passed = orders_silver.filter(col("dq_passed") == True).count()
print(f"  Orders: {total} rows | DQ passed: {passed} ({round(passed/total*100,1)}%)")

# =============================================================================
# TRANSFORM: Order Details (Silver)
# =============================================================================

print("Processing Order Details → Silver...")

details_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/order_details")

details_silver = (details_bronze
    .withColumn("Order ID",     trim(col("Order ID")))
    .withColumn("Category",     trim(col("Category")))
    .withColumn("Sub-Category", when(col("Sub-Category").isNull(), lit("Unknown"))
                                .otherwise(trim(col("Sub-Category"))))

    # Cast numeric fields safely
    .withColumn("Amount",       col("Amount").cast(DoubleType()))
    .withColumn("Profit",       coalesce(col("Profit").cast(DoubleType()), lit(0.0)))
    .withColumn("Quantity",     col("Quantity").cast(IntegerType()))

    # Derived: profit margin percentage
    .withColumn("profit_margin_pct",
        when(col("Amount") > 0,
             spark_round((col("Profit") / col("Amount")) * 100, 2))
        .otherwise(lit(0.0))
    )

    # DQ flags
    .withColumn("dq_amount_positive", col("Amount") > 0)
    .withColumn("dq_order_id_null",   col("Order ID").isNull())

    .withColumn("dq_passed", (
        col("Order ID").isNotNull() &
        (col("Amount") > 0) &
        col("Quantity").isNotNull()
    ))

    .withColumn("_silver_processed_at", current_timestamp())
)

# Remove exact duplicates
details_silver = details_silver.dropDuplicates(["Order ID", "Category", "Sub-Category"])

(details_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/order_details")
)

total = details_silver.count()
passed = details_silver.filter(col("dq_passed") == True).count()
print(f"  Order Details: {total} rows | DQ passed: {passed} ({round(passed/total*100,1)}%)")

# =============================================================================
# TRANSFORM: Sales Targets (Silver)
# =============================================================================

print("Processing Sales Targets → Silver...")

targets_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/sales_targets")

targets_silver = (targets_bronze
    .withColumn("Category",  trim(col("Category")))
    .withColumn("Target",    col("Target").cast(DoubleType()))
    .withColumn("dq_passed", col("Target").isNotNull() & (col("Target") > 0))
    .withColumn("_silver_processed_at", current_timestamp())
)

(targets_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/sales_targets")
)

print(f"  Sales Targets: {targets_silver.count()} rows")
print("\nSilver transformation complete.")
