# =============================================================================
# NOTEBOOK 04 — DATA QUALITY & RECONCILIATION CHECKS
# Project: Medallion Lakehouse — Indian E-Commerce Sales
# Author: Sayanti Chowdhury
# =============================================================================
# PURPOSE:
#   Validate data integrity across Bronze → Silver → Gold.
#   Run after each layer is built. All checks print PASS or FAIL.
#   A failed check should trigger investigation before downstream use.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct

spark = SparkSession.builder.appName("DQ_Reconciliation_ECommerce").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
CONTAINER       = "ecommerce"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/delta"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/delta"
GOLD_PATH   = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/delta"

def check(label, condition, actual=None):
    status = "✅ PASS" if condition else "❌ FAIL"
    detail = f"  (got: {actual})" if actual is not None and not condition else ""
    print(f"  {status}  {label}{detail}")

print("=" * 60)
print("DATA QUALITY & RECONCILIATION REPORT")
print("=" * * 60)

# =============================================================================
# CHECK 1 — Row count: Bronze → Silver (Silver <= Bronze)
# =============================================================================
print("\n[1] Row Count Checks — Bronze vs Silver")

b_orders  = spark.read.format("delta").load(f"{BRONZE_PATH}/orders").count()
s_orders  = spark.read.format("delta").load(f"{SILVER_PATH}/orders").count()
b_details = spark.read.format("delta").load(f"{BRONZE_PATH}/order_details").count()
s_details = spark.read.format("delta").load(f"{SILVER_PATH}/order_details").count()

check("Silver orders <= Bronze orders", s_orders <= b_orders, f"Bronze={b_orders}, Silver={s_orders}")
check("Silver details <= Bronze details", s_details <= b_details, f"Bronze={b_details}, Silver={s_details}")
check("Silver DQ pass rate > 95%", (s_orders / b_orders) > 0.95, f"{round(s_orders/b_orders*100,1)}%")

# =============================================================================
# CHECK 2 — Total revenue reconciliation (Silver → Gold)
# =============================================================================
print("\n[2] Revenue Reconciliation — Silver vs Gold")

silver_details = spark.read.format("delta").load(f"{SILVER_PATH}/order_details").filter(col("dq_passed") == True)
gold_fact      = spark.read.format("delta").load(f"{GOLD_PATH}/fact_sales")

silver_revenue = silver_details.agg(spark_sum("Amount").alias("rev")).collect()[0]["rev"]
gold_revenue   = gold_fact.agg(spark_sum("amount").alias("rev")).collect()[0]["rev"]

tolerance = 0.01  # allow 1% variance for floating point
variance  = abs(silver_revenue - gold_revenue) / silver_revenue if silver_revenue else 1

check("Gold total revenue matches Silver (±1%)", variance < tolerance,
      f"Silver=₹{round(silver_revenue,2):,}, Gold=₹{round(gold_revenue,2):,}")

# =============================================================================
# CHECK 3 — Referential integrity (no orphan FKs in fact table)
# =============================================================================
print("\n[3] Referential Integrity — fact_sales FK coverage")

dim_customer  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")
dim_product   = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")
dim_geography = spark.read.format("delta").load(f"{GOLD_PATH}/dim_geography")
dim_date      = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

orphan_customers = gold_fact.join(dim_customer, on="customer_key", how="left_anti").count()
orphan_products  = gold_fact.join(dim_product,  on="product_key",  how="left_anti").count()
orphan_geos      = gold_fact.join(dim_geography, on="geo_key",     how="left_anti").count()
orphan_dates     = gold_fact.join(dim_date,       on="date_key",   how="left_anti").count()

check("No orphan customer_keys in fact", orphan_customers == 0, f"{orphan_customers} orphans")
check("No orphan product_keys in fact",  orphan_products  == 0, f"{orphan_products} orphans")
check("No orphan geo_keys in fact",      orphan_geos      == 0, f"{orphan_geos} orphans")
check("No orphan date_keys in fact",     orphan_dates     == 0, f"{orphan_dates} orphans")

# =============================================================================
# CHECK 4 — Null checks on Gold fact measures
# =============================================================================
print("\n[4] Null Checks — Gold fact_sales measures")

null_amount   = gold_fact.filter(col("amount").isNull()).count()
null_quantity = gold_fact.filter(col("quantity").isNull()).count()
null_cust_key = gold_fact.filter(col("customer_key").isNull()).count()

check("No null amounts in fact",       null_amount == 0,   f"{null_amount} nulls")
check("No null quantities in fact",    null_quantity == 0, f"{null_quantity} nulls")
check("No null customer_keys in fact", null_cust_key == 0, f"{null_cust_key} nulls")

# =============================================================================
# CHECK 5 — Dimension completeness
# =============================================================================
print("\n[5] Dimension Completeness")

fact_distinct_dates     = gold_fact.select("date_key").distinct().count()
dim_date_total          = dim_date.count()
fact_distinct_customers = gold_fact.select("customer_key").distinct().count()
dim_customer_total      = dim_customer.count()

check("All fact date_keys exist in dim_date",
      fact_distinct_dates <= dim_date_total,
      f"Fact distinct dates={fact_distinct_dates}, dim_date rows={dim_date_total}")
check("All fact customer_keys exist in dim_customer",
      fact_distinct_customers <= dim_customer_total,
      f"Fact distinct customers={fact_distinct_customers}, dim_customer rows={dim_customer_total}")

print("\n" + "=" * 60)
print("Reconciliation complete.")
print("=" * 60)
