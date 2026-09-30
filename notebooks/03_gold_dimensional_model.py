# =============================================================================
# NOTEBOOK 03 — GOLD LAYER: Star Schema Dimensional Model
# Project: Medallion Lakehouse — Indian E-Commerce Sales
# Layer: Gold (analytics-ready, dimensional model)
# Author: Sayanti Chowdhury
# =============================================================================
# PURPOSE:
#   Build the star schema from Silver-layer data. Gold tables are the ONLY
#   layer Power BI connects to. No additional transformation should be needed
#   in the BI layer — all business logic is encoded here.
#
#   Output tables:
#     dim_customer   — unique customers with surrogate key
#     dim_product    — product categories and sub-categories
#     dim_date       — full date spine with calendar attributes
#     dim_geography  — Indian states with region classification
#     fact_sales     — grain: one row per order line item
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month, quarter,
    dayofweek, dayofmonth, weekofyear, date_format, when, lit,
    current_timestamp, round as spark_round, broadcast
)
from pyspark.sql.types import DateType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Gold_DimensionalModel_ECommerce").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
CONTAINER       = "ecommerce"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/delta"
GOLD_PATH   = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/delta"

# Load Silver tables (DQ-passed rows only)
orders  = spark.read.format("delta").load(f"{SILVER_PATH}/orders").filter(col("dq_passed") == True)
details = spark.read.format("delta").load(f"{SILVER_PATH}/order_details").filter(col("dq_passed") == True)

# =============================================================================
# DIM_CUSTOMER
# =============================================================================

print("Building dim_customer...")

dim_customer = (orders
    .select("CustomerName", "City", "State")
    .distinct()
    .withColumn("customer_key", monotonically_increasing_id() + 1)
    .select(
        col("customer_key"),
        col("CustomerName").alias("customer_name"),
        col("City").alias("city"),
        col("State").alias("state")
    )
    .withColumn("_gold_loaded_at", current_timestamp())
)

(dim_customer.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_customer")
)
print(f"  dim_customer: {dim_customer.count()} rows")

# =============================================================================
# DIM_PRODUCT
# =============================================================================

print("Building dim_product...")

dim_product = (details
    .select("Category", "Sub-Category")
    .distinct()
    .withColumn("product_key", monotonically_increasing_id() + 1)
    .select(
        col("product_key"),
        col("Category").alias("category"),
        col("Sub-Category").alias("sub_category")
    )
    .withColumn("_gold_loaded_at", current_timestamp())
)

(dim_product.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_product")
)
print(f"  dim_product: {dim_product.count()} rows")

# =============================================================================
# DIM_GEOGRAPHY
# =============================================================================

print("Building dim_geography...")

# Region lookup for Indian states
region_map = spark.createDataFrame([
    ("MAHARASHTRA", "West"),    ("GUJARAT", "West"),       ("RAJASTHAN", "West"),
    ("GOA", "West"),            ("MADHYA PRADESH", "Central"), ("CHHATTISGARH", "Central"),
    ("UTTAR PRADESH", "North"), ("DELHI", "North"),         ("HARYANA", "North"),
    ("PUNJAB", "North"),        ("HIMACHAL PRADESH", "North"), ("UTTARAKHAND", "North"),
    ("JAMMU AND KASHMIR", "North"), ("WEST BENGAL", "East"), ("ODISHA", "East"),
    ("JHARKHAND", "East"),      ("BIHAR", "East"),          ("ASSAM", "East"),
    ("TAMIL NADU", "South"),    ("KARNATAKA", "South"),     ("KERALA", "South"),
    ("ANDHRA PRADESH", "South"), ("TELANGANA", "South"),
], ["state", "region"])

dim_geography = (orders
    .select(col("State").alias("state"))
    .distinct()
    .join(broadcast(region_map), on="state", how="left")
    .withColumn("region", when(col("region").isNull(), lit("Other")).otherwise(col("region")))
    .withColumn("geo_key", monotonically_increasing_id() + 1)
    .select("geo_key", "state", "region")
    .withColumn("_gold_loaded_at", current_timestamp())
)

(dim_geography.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_geography")
)
print(f"  dim_geography: {dim_geography.count()} rows")

# =============================================================================
# DIM_DATE — date spine from min to max order date
# =============================================================================

print("Building dim_date...")

min_date, max_date = orders.agg(
    F.min("Order Date").alias("min_d"),
    F.max("Order Date").alias("max_d")
).collect()[0]

date_spine = spark.sql(f"""
    SELECT sequence(DATE '{min_date}', DATE '{max_date}', INTERVAL 1 DAY) AS dates
""").select(F.explode(col("dates")).alias("full_date"))

dim_date = (date_spine
    .withColumn("date_key",     F.date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year",         year(col("full_date")))
    .withColumn("quarter",      quarter(col("full_date")))
    .withColumn("month_num",    month(col("full_date")))
    .withColumn("month_name",   date_format(col("full_date"), "MMMM"))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_name",     date_format(col("full_date"), "EEEE"))
    .withColumn("is_weekend",   dayofweek(col("full_date")).isin([1, 7]))
    .withColumn("_gold_loaded_at", current_timestamp())
)

(dim_date.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_date")
)
print(f"  dim_date: {dim_date.count()} rows")

# =============================================================================
# FACT_SALES — join orders + details + all dimension keys
# Grain: one row per order line item (Order ID + Category + Sub-Category)
# =============================================================================

print("Building fact_sales...")

# Reload dims for key lookup
dim_customer_lkp  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")
dim_product_lkp   = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")
dim_geography_lkp = spark.read.format("delta").load(f"{GOLD_PATH}/dim_geography")
dim_date_lkp      = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

# Join orders + details on Order ID
fact_base = orders.join(details, on="Order ID", how="inner")

# Resolve dimension keys
fact_sales = (fact_base
    .join(broadcast(dim_customer_lkp.select("customer_key", "customer_name", "state")),
          (fact_base["CustomerName"] == dim_customer_lkp["customer_name"]) &
          (fact_base["State"] == dim_customer_lkp["state"]), "left")
    .join(broadcast(dim_product_lkp.select("product_key", "category", "sub_category")),
          (fact_base["Category"] == dim_product_lkp["category"]) &
          (fact_base["Sub-Category"] == dim_product_lkp["sub_category"]), "left")
    .join(broadcast(dim_geography_lkp.select("geo_key", "state").withColumnRenamed("state", "geo_state")),
          fact_base["State"] == col("geo_state"), "left")
    .join(broadcast(dim_date_lkp.select("date_key", "full_date")),
          fact_base["Order Date"] == dim_date_lkp["full_date"], "left")
    .select(
        monotonically_increasing_id().alias("sales_key"),
        col("Order ID").alias("order_id"),
        col("date_key"),
        col("customer_key"),
        col("product_key"),
        col("geo_key"),
        col("Amount").alias("amount"),
        col("Quantity").alias("quantity"),
        col("Profit").alias("profit"),
        col("profit_margin_pct"),
        current_timestamp().alias("_gold_loaded_at")
    )
)

(fact_sales.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/fact_sales")
)

print(f"  fact_sales: {fact_sales.count()} rows")
print("\n✅ Gold layer complete. All tables ready for Power BI.")

# Quick summary
print("\n--- Gold Layer Summary ---")
for tbl in ["dim_customer", "dim_product", "dim_date", "dim_geography", "fact_sales"]:
    cnt = spark.read.format("delta").load(f"{GOLD_PATH}/{tbl}").count()
    print(f"  {tbl}: {cnt} rows")
