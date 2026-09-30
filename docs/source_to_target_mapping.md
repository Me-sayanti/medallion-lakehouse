# Source-to-Target Mapping Document
**Project:** Medallion Lakehouse — Indian E-Commerce Sales  
**Author:** Sayanti Chowdhury  
**Version:** 1.0  

---

## Mapping Summary

| Source File | Source Layer | Target Table | Target Layer | Transformation Type |
|---|---|---|---|---|
| List of Orders.csv | Raw / Bronze | silver.orders | Silver | Clean + Validate |
| Order Details.csv | Raw / Bronze | silver.order_details | Silver | Clean + Enrich |
| Sales Target.csv | Raw / Bronze | silver.sales_targets | Silver | Clean |
| silver.orders + silver.order_details | Silver | gold.dim_customer | Gold | Deduplicate + Surrogate key |
| silver.order_details | Silver | gold.dim_product | Gold | Deduplicate + Surrogate key |
| silver.orders | Silver | gold.dim_geography | Gold | Lookup + Surrogate key |
| Derived (date spine) | — | gold.dim_date | Gold | Generate |
| All Silver tables | Silver | gold.fact_sales | Gold | Join + Measure derivation |

---

## Detailed Field Mappings

### 1. Orders: Bronze → Silver

| Source Field | Source Type | Target Field | Target Type | Transformation | Business Rule |
|---|---|---|---|---|---|
| Order ID | String | Order ID | String | TRIM() | Not null — reject row if null |
| Order Date | String (dd-MM-yyyy) | Order Date | DateType | to_date(dd-MM-yyyy) | Must be valid date — reject if null after parse |
| CustomerName | String | CustomerName | String | TRIM() | Not null — reject row if null |
| State | String | State | String | TRIM() + UPPER() | Standardise against Indian state reference list |
| City | String | City | String | TRIM() | No rule |
| — | — | dq_passed | Boolean | Derived | True if Order ID + Order Date + CustomerName all non-null |
| — | — | _ingestion_timestamp | Timestamp | current_timestamp() | System metadata |
| — | — | _silver_processed_at | Timestamp | current_timestamp() | System metadata |

---

### 2. Order Details: Bronze → Silver

| Source Field | Source Type | Target Field | Target Type | Transformation | Business Rule |
|---|---|---|---|---|---|
| Order ID | String | Order ID | String | TRIM() | Not null |
| Amount | String/Double | Amount | Double | cast(DoubleType) | Must be > 0 — flag dq_passed=False if ≤ 0 |
| Profit | String/Double | Profit | Double | COALESCE(cast, 0.0) | Null → 0.0 |
| Quantity | String/Int | Quantity | Integer | cast(IntegerType) | Not null |
| Category | String | Category | String | TRIM() | Not null |
| Sub-Category | String | Sub-Category | String | TRIM(); null → "Unknown" | Default "Unknown" if null |
| — | — | profit_margin_pct | Double | (Profit / Amount) * 100 | 0.0 if Amount = 0 |
| — | — | dq_passed | Boolean | Derived | True if Order ID not null AND Amount > 0 AND Quantity not null |

---

### 3. Silver → Gold: dim_customer

| Source Field | Source Table | Target Field | Target Table | Transformation |
|---|---|---|---|---|
| — | — | customer_key | dim_customer | monotonically_increasing_id() — surrogate PK |
| CustomerName | silver.orders | customer_name | dim_customer | Deduplicated on CustomerName + State |
| City | silver.orders | city | dim_customer | Direct |
| State | silver.orders | state | dim_customer | Direct |

---

### 4. Silver → Gold: dim_product

| Source Field | Source Table | Target Field | Target Table | Transformation |
|---|---|---|---|---|
| — | — | product_key | dim_product | monotonically_increasing_id() — surrogate PK |
| Category | silver.order_details | category | dim_product | Deduplicated on Category + Sub-Category |
| Sub-Category | silver.order_details | sub_category | dim_product | Direct |

---

### 5. Silver → Gold: dim_geography

| Source Field | Source Table | Target Field | Target Table | Transformation |
|---|---|---|---|---|
| — | — | geo_key | dim_geography | monotonically_increasing_id() — surrogate PK |
| State | silver.orders | state | dim_geography | Distinct states |
| — | Region lookup table | region | dim_geography | LEFT JOIN on state → North/South/East/West/Central/Other |

---

### 6. Generated: dim_date

| Source | Target Field | Transformation |
|---|---|---|
| Derived (date spine) | date_key | date_format(full_date, 'yyyyMMdd').cast(INT) |
| Derived | full_date | sequence(min_order_date, max_order_date, 1 DAY) |
| Derived | year | year(full_date) |
| Derived | quarter | quarter(full_date) — 1 to 4 |
| Derived | month_num | month(full_date) — 1 to 12 |
| Derived | month_name | date_format(full_date, 'MMMM') |
| Derived | week_of_year | weekofyear(full_date) |
| Derived | day_of_month | dayofmonth(full_date) |
| Derived | day_name | date_format(full_date, 'EEEE') |
| Derived | is_weekend | dayofweek(full_date).isin([1, 7]) |

---

### 7. Silver → Gold: fact_sales

| Source Field | Source Table | Target Field | Target Table | Transformation |
|---|---|---|---|---|
| — | — | sales_key | fact_sales | monotonically_increasing_id() — surrogate PK |
| Order ID | silver.orders | order_id | fact_sales | Natural key retained for traceability |
| Order Date (resolved) | dim_date | date_key | fact_sales | JOIN silver.orders.Order Date = dim_date.full_date → date_key |
| CustomerName + State | dim_customer | customer_key | fact_sales | JOIN on name + state → customer_key |
| Category + Sub-Category | dim_product | product_key | fact_sales | JOIN on category + sub_category → product_key |
| State | dim_geography | geo_key | fact_sales | JOIN on state → geo_key |
| Amount | silver.order_details | amount | fact_sales | Direct |
| Quantity | silver.order_details | quantity | fact_sales | Direct |
| Profit | silver.order_details | profit | fact_sales | Direct |
| profit_margin_pct | silver.order_details | profit_margin_pct | fact_sales | Direct (derived in Silver) |

---

## Data Lineage Summary

```
List of Orders.csv ──────────────┬──▶ silver.orders ──┬──▶ dim_customer
                                 │                     ├──▶ dim_geography
                                 │                     └──▶ fact_sales (order_id, date_key, customer_key, geo_key)

Order Details.csv ───────────────┬──▶ silver.order_details ──┬──▶ dim_product
                                 │                            └──▶ fact_sales (amount, quantity, profit)

[Date spine — generated] ────────────────────────────────────▶ dim_date

dim_customer + dim_product + dim_geography + dim_date ───────▶ fact_sales (FK resolution)
```
