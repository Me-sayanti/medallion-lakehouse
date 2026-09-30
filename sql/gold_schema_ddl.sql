-- =============================================================================
-- GOLD LAYER — Star Schema DDL
-- Project: Medallion Lakehouse — Indian E-Commerce Sales
-- Author: Sayanti Chowdhury
-- =============================================================================
-- These CREATE TABLE statements define the Gold layer schema.
-- Tables are backed by Delta Lake files in ADLS Gen2.
-- Connect Power BI directly to the Databricks SQL Warehouse using these tables.
-- =============================================================================

-- ─────────────────────────────────────────
-- DIM_DATE — Calendar dimension (date spine)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        INT           NOT NULL,   -- Format: YYYYMMDD e.g. 20180401
    full_date       DATE          NOT NULL,
    year            INT           NOT NULL,
    quarter         INT           NOT NULL,   -- 1–4
    month_num       INT           NOT NULL,   -- 1–12
    month_name      STRING        NOT NULL,   -- 'January', 'February' ...
    week_of_year    INT           NOT NULL,
    day_of_month    INT           NOT NULL,
    day_name        STRING        NOT NULL,   -- 'Monday', 'Tuesday' ...
    is_weekend      BOOLEAN       NOT NULL,   -- True for Saturday/Sunday
    _gold_loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://ecommerce@yourstorageaccount.dfs.core.windows.net/gold/delta/dim_date'
COMMENT 'Date dimension — full calendar spine from first to last order date';

-- ─────────────────────────────────────────
-- DIM_CUSTOMER
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key    BIGINT        NOT NULL,   -- Surrogate key
    customer_name   STRING        NOT NULL,
    city            STRING,
    state           STRING,
    _gold_loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://ecommerce@yourstorageaccount.dfs.core.windows.net/gold/delta/dim_customer'
COMMENT 'Customer dimension — unique customers derived from order headers';

-- ─────────────────────────────────────────
-- DIM_PRODUCT
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key     BIGINT        NOT NULL,   -- Surrogate key
    category        STRING        NOT NULL,   -- e.g. 'Electronics', 'Clothing'
    sub_category    STRING,                   -- e.g. 'Phones', 'Sarees'
    _gold_loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://ecommerce@yourstorageaccount.dfs.core.windows.net/gold/delta/dim_product'
COMMENT 'Product dimension — category and sub-category hierarchy';

-- ─────────────────────────────────────────
-- DIM_GEOGRAPHY
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_geography (
    geo_key         BIGINT        NOT NULL,   -- Surrogate key
    state           STRING        NOT NULL,   -- Indian state name (standardised, uppercased)
    region          STRING        NOT NULL,   -- North / South / East / West / Central / Other
    _gold_loaded_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://ecommerce@yourstorageaccount.dfs.core.windows.net/gold/delta/dim_geography'
COMMENT 'Geography dimension — Indian states with region classification';

-- ─────────────────────────────────────────
-- FACT_SALES — Core fact table
-- Grain: one row per order line item (Order ID + Category + Sub-Category)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_key           BIGINT        NOT NULL,   -- Surrogate key (PK)
    order_id            STRING        NOT NULL,   -- Natural key from source
    date_key            INT           NOT NULL,   -- FK → dim_date
    customer_key        BIGINT        NOT NULL,   -- FK → dim_customer
    product_key         BIGINT        NOT NULL,   -- FK → dim_product
    geo_key             BIGINT        NOT NULL,   -- FK → dim_geography
    -- Measures
    amount              DOUBLE        NOT NULL,   -- Sale amount in INR
    quantity            INT           NOT NULL,   -- Units sold
    profit              DOUBLE        NOT NULL,   -- Profit in INR (0 if null in source)
    profit_margin_pct   DOUBLE,                   -- (profit / amount) * 100
    -- Metadata
    _gold_loaded_at     TIMESTAMP
)
USING DELTA
LOCATION 'abfss://ecommerce@yourstorageaccount.dfs.core.windows.net/gold/delta/fact_sales'
COMMENT 'Sales fact table — grain: one row per order line item. Join to all four dims for full analysis.';
