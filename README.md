# 🏗️ End-to-End Medallion Lakehouse — Indian E-Commerce Sales Platform

> **Role demonstrated:** Junior Azure Data Engineer · Data Modeller  
> **Stack:** Azure Databricks · PySpark · Delta Lake · Azure Data Factory · ADLS Gen2 · Power BI  
> **Dataset:** [E-Commerce Sales — Kaggle](https://www.kaggle.com/datasets/benroshan/ecommerce-data) (free, public)

---

## Business Problem

An Indian e-commerce company sells products across 6 categories in 10+ states. Raw data arrives as daily CSV dumps from their order management system. Analysts run conflicting Excel reports, KPIs differ by team, and no one trusts the numbers.

**Goal:** Design and build a governed Medallion Lakehouse (Bronze → Silver → Gold) so:
- Raw data lands untouched in **Bronze** (source of truth)
- **Silver** holds clean, validated, business-rule-applied data
- **Gold** holds a dimensional star schema consumed directly by Power BI — zero additional transformation required

---

## Architecture

```
[Source CSVs]
     │
     ▼
┌──────────┐     ┌──────────┐     ┌──────────┐
│  BRONZE  │────▶│  SILVER  │────▶│   GOLD   │────▶ Power BI
│  Raw     │     │  Cleaned │     │  Star    │
│  Delta   │     │  Validated│    │  Schema  │
└──────────┘     └──────────┘     └──────────┘
          Azure Data Lake Storage Gen2
          Azure Databricks + Delta Lake
```

| Layer | Purpose | Format | Key Transformations |
|---|---|---|---|
| **Bronze** | Raw ingestion, no changes | Delta | Metadata columns added (`_ingestion_ts`, `_source_file`) |
| **Silver** | Cleaned, validated, enriched | Delta | Nulls handled, dates parsed, duplicates removed, DQ flags |
| **Gold** | Dimensional model, analytics-ready | Delta | Star schema: `fact_sales` + 4 dimension tables |

---

## Repository Structure

```
P1-medallion-lakehouse/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_dimensional_model.py
│   └── 04_data_quality_checks.py
├── sql/
│   ├── gold_schema_ddl.sql
│   └── reconciliation_queries.sql
├── docs/
│   ├── source_to_target_mapping.md
│   ├── data_dictionary.md
│   └── business_rules.md
├── diagrams/
│   └── star_schema_erd.png          ← ER diagram of Gold layer
└── screenshots/
    ├── silver_quality_report.png
    └── gold_powerbi_dashboard.png
```

---

## Gold Layer — Star Schema

```
              ┌─────────────┐
              │  dim_date   │
              │  date_key PK│
              └──────┬──────┘
                     │
┌──────────────┐ ┌───▼──────────┐ ┌─────────────────┐
│ dim_customer │ │  fact_sales  │ │   dim_product   │
│ customer_key ├▶│  sales_key PK│◀┤  product_key PK │
│ customer_name│ │  date_key FK │ │  product_name   │
│ city         │ │  customer_key│ │  category       │
└──────────────┘ │  product_key │ │  sub_category   │
                 │  geo_key     │ └─────────────────┘
┌──────────────┐ │  amount      │
│ dim_geography│ │  quantity    │
│ geo_key PK   ├▶│  profit      │
│ state        │ │  profit_pct  │
│ region       │ └──────────────┘
└──────────────┘
```

---

## Data Quality Rules (Silver Layer)

| Rule ID | Field | Rule | Action |
|---|---|---|---|
| DQ-01 | Order ID | Not null | Reject row |
| DQ-02 | Order Date | Valid date, not future | Reject row |
| DQ-03 | Amount | Must be > 0 | Flag `dq_passed = False` |
| DQ-04 | Customer Name | Not null | Flag `dq_passed = False` |
| DQ-05 | State | Match Indian state reference list | Standardise or flag |
| DQ-06 | Profit | Nulls allowed | Fill null → 0 |

---

## Step-by-Step Build Plan

### Day 1 — Environment Setup
1. Create free Azure account (USD 200 credit for new signups)
2. Create ADLS Gen2 storage account → containers: `bronze/`, `silver/`, `gold/`
3. Create Azure Databricks workspace (Community Edition is free)
4. Download the 3 CSVs from Kaggle; upload to `bronze/raw/ecommerce/`
5. Mount ADLS in Databricks using a service principal or SAS token

### Day 1–2 — Bronze Layer
- Run `01_bronze_ingestion.py`
- Read CSVs with PySpark `spark.read.csv()`
- Add: `_ingestion_timestamp = current_timestamp()`, `_source_file`, `_batch_id`
- Write as Delta: `bronze/delta/orders/`, `bronze/delta/order_details/`, `bronze/delta/targets/`
- **No transformations** — Bronze is the raw source of truth

### Day 2–3 — Silver Layer
- Run `02_silver_transformation.py`
- Parse `Order Date` string → `DateType`
- Fill null `Profit` → 0; fill null `Sub-Category` → "Unknown"
- Drop duplicate Order IDs (keep first)
- Standardise state names to a reference list
- Add `profit_margin_pct = profit / amount * 100`
- Add `dq_passed` boolean flag per business rules above
- Write to `silver/delta/`

### Day 3–4 — Gold Layer
- Run `03_gold_dimensional_model.py`
- Build `dim_customer` — deduplicate, assign surrogate key (`monotonically_increasing_id`)
- Build `dim_product` — product + category + sub-category
- Build `dim_date` — date spine: year, quarter, month, week number, day name, is_weekend
- Build `dim_geography` — state → region mapping (North/South/East/West/Central)
- Build `fact_sales` — join all dims, carry measures: amount, quantity, profit, profit_margin_pct
- Write all to `gold/delta/`

### Day 4 — Validation & Reconciliation
- Run `04_data_quality_checks.py` + `reconciliation_queries.sql`
- Row count check: Bronze = Silver + rejected rows
- Sum check: total `amount` matches across Bronze → Silver → Gold
- No orphan FKs in fact table
- All dim tables have 100% coverage of fact FK values

### Day 5 — Power BI
- Connect Power BI Desktop → Databricks SQL Warehouse → import Gold Delta tables
- Build 3 report pages: **Sales Overview**, **Regional Performance**, **Category vs Target**
- Publish to Power BI Service (free tier)
- Take a screenshot of the final dashboard

### Day 5–6 — GitHub & Documentation
- Push all notebooks, SQL, docs to this repo
- Fill in `source_to_target_mapping.md` and `data_dictionary.md` using the templates in `/docs`
- Take screenshots: Delta table view in Databricks, Silver DQ report, Gold Power BI dashboard

---

## Key Metrics

| Metric | Value |
|---|---|
| Source files | 3 CSVs |
| Bronze rows | ~2,000 |
| Silver rows (after DQ) | ~1,960 (98% pass rate) |
| Gold fact rows | ~1,960 |
| Dimension tables | 4 |
| Power BI pages | 3 |

---

## CV Bullet Points

*Add these under "Projects" on your CV once complete:*

- Designed and implemented an end-to-end **Medallion Architecture Lakehouse** (Bronze → Silver → Gold) on **Azure Databricks** using **PySpark** and **Delta Lake**, ingesting an Indian e-commerce dataset across 3 governed pipeline layers with automated **data quality validation** at each stage.
- Built a **star schema dimensional model** in the Gold layer (1 fact table, 4 dimension tables) consumed directly by **Power BI** — eliminating ad hoc transformation and enabling consistent KPI reporting across sales, regional, and category dimensions.
- Produced enterprise-grade governance artefacts: **source-to-target mapping**, **data dictionary**, and **business rules document** — mirroring real client deliverables.
