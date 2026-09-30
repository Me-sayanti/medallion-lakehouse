# Data Dictionary
**Project:** Medallion Lakehouse — Indian E-Commerce Sales  
**Author:** Sayanti Chowdhury  
**Layer:** Gold (analytical layer — Power BI source)

---

## fact_sales

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| sales_key | BIGINT | No | Surrogate primary key — system generated | 1, 2, 3 |
| order_id | STRING | No | Natural key from source order system | B-25601 |
| date_key | INT | No | FK to dim_date — format YYYYMMDD | 20180401 |
| customer_key | BIGINT | No | FK to dim_customer | 42 |
| product_key | BIGINT | No | FK to dim_product | 7 |
| geo_key | BIGINT | No | FK to dim_geography | 3 |
| amount | DOUBLE | No | Sale value in Indian Rupees (INR) | 1450.00 |
| quantity | INT | No | Number of units sold in this line item | 3 |
| profit | DOUBLE | No | Profit in INR; 0 if source was null | 210.50 |
| profit_margin_pct | DOUBLE | Yes | (profit / amount) × 100; null if amount = 0 | 14.52 |
| _gold_loaded_at | TIMESTAMP | Yes | Pipeline load timestamp — audit column | 2024-03-15 10:22:00 |

---

## dim_customer

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| customer_key | BIGINT | No | Surrogate PK — system generated | 1 |
| customer_name | STRING | No | Full customer name from source | Harivansh Sharma |
| city | STRING | Yes | City of customer | Mumbai |
| state | STRING | Yes | Indian state — standardised, uppercased | MAHARASHTRA |

---

## dim_product

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| product_key | BIGINT | No | Surrogate PK | 1 |
| category | STRING | No | Top-level product category | Electronics |
| sub_category | STRING | Yes | Sub-category; "Unknown" if source null | Phones |

---

## dim_date

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| date_key | INT | No | Surrogate PK — format YYYYMMDD | 20180401 |
| full_date | DATE | No | Calendar date | 2018-04-01 |
| year | INT | No | Calendar year | 2018 |
| quarter | INT | No | Quarter 1–4 | 2 |
| month_num | INT | No | Month number 1–12 | 4 |
| month_name | STRING | No | Full month name | April |
| week_of_year | INT | No | ISO week number | 13 |
| day_of_month | INT | No | Day number 1–31 | 1 |
| day_name | STRING | No | Full day name | Sunday |
| is_weekend | BOOLEAN | No | True if Saturday or Sunday | true |

---

## dim_geography

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| geo_key | BIGINT | No | Surrogate PK | 1 |
| state | STRING | No | Indian state — standardised, uppercased | WEST BENGAL |
| region | STRING | No | Geographic region of India | East |

**Region values:** North · South · East · West · Central · Other

---

## Business Terms Glossary

| Term | Definition |
|---|---|
| Amount | Total sale value for a line item in INR — the revenue figure |
| Profit | Net profit on the line item after cost. Source can be negative (loss) |
| Profit Margin % | (Profit ÷ Amount) × 100. Negative = loss-making sale |
| Order | A single customer transaction, identified by Order ID |
| Line Item | One row within an order — one product category + sub-category combination |
| Surrogate Key | System-generated integer PK — has no business meaning; used only for joins |
| Natural Key | The business identifier from the source system (e.g. Order ID) |
| DQ Passed | Data Quality flag — True means the row passed all validation rules and is safe for analytics |
| Medallion Architecture | A data design pattern with 3 layers: Bronze (raw), Silver (clean), Gold (analytics-ready) |
| Star Schema | Dimensional model with one central fact table surrounded by dimension tables |
