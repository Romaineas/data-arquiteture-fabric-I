# Guia Prático: SQL, DAX e Dashboards para Analytics Engineer Fabric

---

## 📊 PARTE 1: QUERIES SQL PRONTAS PARA COPIAR E COLAR

### 1.1 Criar Tabelas Silver com Spark SQL

#### Query 1: Silver Orders Limpa
```sql
-- Cria tabela de orders limpa e validada
CREATE OR REPLACE TABLE silver_orders AS
SELECT
    CAST(order_id AS STRING) as order_id,
    CAST(customer_id AS STRING) as customer_id,
    CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE) as order_date,
    CAST(total_value AS DECIMAL(10, 2)) as total_value,
    CAST(order_value_freight AS DECIMAL(10, 2)) as freight_value,
    CAST(order_value_tax AS DECIMAL(10, 2)) as tax_value,
    order_status,
    CURRENT_TIMESTAMP as ingestion_time,
    YEAR(CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE)) as year_order,
    MONTH(CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE)) as month_order,
    QUARTER(CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE)) as quarter_order,
    DAYOFWEEK(CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE)) as day_of_week_order
FROM raw_orders
WHERE 
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND total_value > 0
    AND order_status IN ('delivered', 'shipped', 'pending', 'processing', 'canceled')
```

**O que faz:**
- Remove linhas com dados nulos ou inválidos
- Converte tipos de dados (STRING, DATE, DECIMAL)
- Adiciona colunas de tempo (year, month, quarter)
- Registra hora de ingestão

---

#### Query 2: Silver Customer Metrics (Agregação)
```sql
-- Calcula métricas por cliente (RFM - Recency, Frequency, Monetary)
CREATE OR REPLACE TABLE silver_customer_metrics AS
SELECT
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT YEAR(order_date)) as years_active,
    ROUND(SUM(total_value), 2) as lifetime_value,
    ROUND(SUM(total_value) / COUNT(DISTINCT order_id), 2) as avg_order_value,
    MIN(order_date) as first_purchase_date,
    MAX(order_date) as last_purchase_date,
    DATEDIFF(day, MIN(order_date), MAX(order_date)) as customer_tenure_days,
    DATEDIFF(day, MAX(order_date), CURRENT_DATE) as days_since_last_purchase,
    ROUND(
        SUM(CASE WHEN order_status = 'delivered' THEN total_value ELSE 0 END) / 
        SUM(total_value) * 100, 2
    ) as completed_order_rate,
    COUNT(CASE WHEN order_status = 'canceled' THEN 1 END) as canceled_orders
FROM silver_orders
GROUP BY customer_id
HAVING COUNT(order_id) > 0
ORDER BY lifetime_value DESC
```

**Métricas geradas (RFM Analysis):**
- **Recency**: dias_desde_última_compra
- **Frequency**: total_orders
- **Monetary**: lifetime_value

---

#### Query 3: Silver Order Items Enriquecida
```sql
-- Tabela de itens de pedido com product details
CREATE OR REPLACE TABLE silver_order_items_enriched AS
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    CAST(oi.shipping_limit_date AS DATE) as shipping_limit_date,
    CAST(oi.price AS DECIMAL(10, 2)) as item_price,
    CAST(oi.freight_value AS DECIMAL(10, 2)) as item_freight,
    CAST(oi.quantity AS INT) as quantity,
    ROUND(CAST(oi.quantity AS DECIMAL) * CAST(oi.price AS DECIMAL), 2) as line_amount,
    COALESCE(p.product_category, 'Unknown') as product_category,
    COALESCE(p.product_name, 'Unknown') as product_name,
    ROUND(CAST(p.product_weight_g AS DECIMAL) / 1000, 2) as product_weight_kg
FROM raw_order_items oi
LEFT JOIN raw_products p ON oi.product_id = p.product_id
WHERE 
    oi.order_id IS NOT NULL
    AND oi.product_id IS NOT NULL
    AND oi.price > 0
    AND oi.quantity > 0
```

---

#### Query 4: Silver Seller Performance
```sql
-- Métricas por seller (fornecedor)
CREATE OR REPLACE TABLE silver_seller_metrics AS
SELECT
    s.seller_id,
    COALESCE(s.seller_city, 'Unknown') as seller_city,
    COALESCE(s.seller_state, 'Unknown') as seller_state,
    COUNT(DISTINCT oi.order_id) as total_orders_sold,
    COUNT(DISTINCT oi.product_id) as unique_products,
    ROUND(SUM(oi.price * oi.quantity), 2) as total_revenue,
    ROUND(AVG(oi.price * oi.quantity), 2) as avg_order_value,
    ROUND(SUM(oi.freight_value), 2) as total_freight,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    ROUND(CAST(SUM(r.review_score) AS DECIMAL) / COUNT(r.review_id), 2) as avg_rating
FROM raw_sellers s
LEFT JOIN raw_order_items oi ON s.seller_id = oi.seller_id
LEFT JOIN raw_orders o ON oi.order_id = o.order_id
LEFT JOIN raw_reviews r ON o.order_id = r.order_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
HAVING COUNT(DISTINCT oi.order_id) > 0
```

---

### 1.2 Criar Dimensões (Gold Layer)

#### Query 5: DIM_DATE Completa
```sql
-- Tabela de calendário de 3 anos com flags de feriados
CREATE OR REPLACE TABLE dim_date AS
WITH date_range AS (
    SELECT EXPLODE(SEQUENCE(
        TO_DATE('2023-01-01'), 
        TO_DATE('2025-12-31'), 
        INTERVAL 1 DAY
    )) as calendar_date
),
with_flags AS (
    SELECT
        CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) as date_id,
        calendar_date as date_key,
        YEAR(calendar_date) as year,
        MONTH(calendar_date) as month,
        QUARTER(calendar_date) as quarter,
        DAYOFWEEK(calendar_date) as day_of_week,
        DATE_FORMAT(calendar_date, 'EEEE') as day_name,
        DATE_FORMAT(calendar_date, 'MMMM') as month_name,
        WEEK(calendar_date) as week_number,
        DAY(calendar_date) as day_of_month,
        DAYOFYEAR(calendar_date) as day_of_year,
        CASE 
            WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN 1 
            ELSE 0 
        END as is_weekend,
        CASE 
            WHEN (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 25) THEN 'Christmas'
            WHEN (MONTH(calendar_date) = 1 AND DAY(calendar_date) = 1) THEN 'New Year'
            WHEN (MONTH(calendar_date) = 11 AND DAY(calendar_date) = 20) THEN 'Black Friday'
            ELSE NULL
        END as holiday_name
    FROM date_range
)
SELECT *
FROM with_flags
ORDER BY date_id
```

---

#### Query 6: DIM_CUSTOMER Segmentada
```sql
-- Dimensão de clientes com segmentação RFM
CREATE OR REPLACE TABLE dim_customer AS
WITH customer_base AS (
    SELECT
        customer_id,
        COALESCE(customer_unique_id, 'Unknown') as customer_unique_id,
        COALESCE(customer_zip_code_prefix, '00000') as customer_zip_code,
        COALESCE(customer_city, 'Unknown') as customer_city,
        COALESCE(customer_state, 'XX') as customer_state
    FROM raw_customers
),
metrics AS (
    SELECT
        cm.customer_id,
        cm.total_orders,
        cm.lifetime_value,
        cm.avg_order_value,
        cm.first_purchase_date,
        cm.last_purchase_date,
        cm.days_since_last_purchase,
        cm.completed_order_rate,
        -- RFM Segmentação
        NTILE(4) OVER (ORDER BY cm.days_since_last_purchase DESC) as recency_score,
        NTILE(4) OVER (ORDER BY cm.total_orders ASC) as frequency_score,
        NTILE(4) OVER (ORDER BY cm.lifetime_value ASC) as monetary_score
    FROM silver_customer_metrics cm
)
SELECT
    cb.customer_id,
    cb.customer_unique_id,
    cb.customer_city,
    cb.customer_state,
    cb.customer_zip_code,
    m.total_orders,
    ROUND(m.lifetime_value, 2) as lifetime_value,
    ROUND(m.avg_order_value, 2) as avg_order_value,
    m.first_purchase_date,
    m.last_purchase_date,
    m.days_since_last_purchase,
    ROUND(m.completed_order_rate, 2) as completed_order_rate,
    -- Segmentação por Lifetime Value
    CASE 
        WHEN m.lifetime_value > 5000 THEN 'Premium'
        WHEN m.lifetime_value > 1000 THEN 'VIP'
        WHEN m.lifetime_value > 500 THEN 'Gold'
        WHEN m.lifetime_value > 100 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_segment,
    -- RFM Quadrant
    CASE 
        WHEN m.recency_score >= 3 AND m.frequency_score >= 3 AND m.monetary_score >= 3 
            THEN 'Champions'
        WHEN m.recency_score >= 3 AND m.frequency_score >= 3 
            THEN 'Loyal Customers'
        WHEN m.recency_score >= 3 AND m.monetary_score >= 3 
            THEN 'Big Spenders'
        WHEN m.recency_score < 2 
            THEN 'At Risk'
        ELSE 'Potential'
    END as rfm_segment,
    CURRENT_TIMESTAMP as loaded_date
FROM customer_base cb
LEFT JOIN metrics m ON cb.customer_id = m.customer_id
ORDER BY lifetime_value DESC
```

---

#### Query 7: DIM_PRODUCT com Análise de Categoria
```sql
-- Dimensão de produtos com benchmarking
CREATE OR REPLACE TABLE dim_product AS
WITH base_products AS (
    SELECT
        product_id,
        COALESCE(product_category, 'Unknown') as product_category,
        COALESCE(product_name, 'Unknown') as product_name,
        COALESCE(product_name_lenght, 0) as product_name_length,
        COALESCE(ROUND(product_weight_g / 1000.0, 2), 0) as product_weight_kg,
        COALESCE(product_length_cm, 0) as product_length_cm,
        COALESCE(product_height_cm, 0) as product_height_cm,
        COALESCE(product_width_cm, 0) as product_width_cm
    FROM raw_products
),
category_stats AS (
    SELECT
        product_category,
        COUNT(*) as products_in_category,
        ROUND(AVG(product_weight_kg), 2) as avg_weight_kg,
        ROUND(AVG(product_name_length), 0) as avg_name_length
    FROM base_products
    GROUP BY product_category
)
SELECT
    bp.product_id,
    bp.product_category,
    bp.product_name,
    bp.product_name_length,
    bp.product_weight_kg,
    bp.product_length_cm,
    bp.product_height_cm,
    bp.product_width_cm,
    ROUND(
        (bp.product_length_cm * bp.product_height_cm * bp.product_width_cm) / 1000000, 
        4
    ) as product_volume_m3,
    cs.products_in_category,
    CASE 
        WHEN bp.product_weight_kg < 1 THEN 'Light'
        WHEN bp.product_weight_kg < 5 THEN 'Medium'
        ELSE 'Heavy'
    END as weight_category,
    CURRENT_TIMESTAMP as loaded_date
FROM base_products bp
LEFT JOIN category_stats cs ON bp.product_category = cs.product_category
WHERE bp.product_id IS NOT NULL
```

---

### 1.3 Criar Fact Table

#### Query 8: FACT_SALES Completa
```sql
-- Tabela de fatos principal (star schema)
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) as date_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.order_id,
    oi.order_item_id,
    CAST(oi.quantity AS INT) as quantity,
    CAST(oi.price AS DECIMAL(10, 2)) as unit_price,
    ROUND(CAST(oi.quantity AS DECIMAL) * CAST(oi.price AS DECIMAL), 2) as line_amount,
    CAST(oi.freight_value AS DECIMAL(10, 2)) as freight_value,
    CAST(o.total_value AS DECIMAL(10, 2)) as order_total,
    o.order_status,
    CASE 
        WHEN o.order_status = 'delivered' THEN 1 
        ELSE 0 
    END as is_delivered,
    CAST(r.review_score AS INT) as review_score,
    r.review_message,
    CURRENT_TIMESTAMP as loaded_timestamp,
    -- Calcs úteis
    ROUND(
        CAST(oi.quantity AS DECIMAL) * CAST(oi.price AS DECIMAL) + 
        CAST(oi.freight_value AS DECIMAL),
        2
    ) as total_cost
FROM raw_orders o
LEFT JOIN raw_order_items oi ON o.order_id = oi.order_id
LEFT JOIN raw_reviews r ON o.order_id = r.order_id
WHERE 
    o.order_id IS NOT NULL
    AND oi.product_id IS NOT NULL
    AND oi.price > 0
    AND oi.quantity > 0
ORDER BY o.order_date DESC, o.order_id, oi.order_item_id
```

**Índices recomendados para performance:**
```sql
CREATE INDEX idx_fact_sales_date_id ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_customer_id ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product_id ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_seller_id ON fact_sales(seller_id);
CREATE INDEX idx_fact_sales_status ON fact_sales(order_status);
```

---

## 📈 PARTE 2: MEDIDAS DAX PARA POWER BI

### 2.1 Medidas Básicas

```dax
-- RECEITA E VENDAS

Total Revenue = 
SUM(fact_sales[line_amount])

Total Orders = 
DISTINCTCOUNT(fact_sales[order_id])

Total Customers = 
DISTINCTCOUNT(fact_sales[customer_id])

Total Items Sold = 
SUM(fact_sales[quantity])

Average Order Value = 
DIVIDE([Total Revenue], [Total Orders], 0)

Average Item Price = 
DIVIDE([Total Revenue], [Total Items Sold], 0)

Orders Completed = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[order_id]),
    fact_sales[is_delivered] = 1
)

Completion Rate % = 
DIVIDE([Orders Completed], [Total Orders], 0) * 100

Revenue per Customer = 
DIVIDE([Total Revenue], [Total Customers], 0)
```

---

### 2.2 Medidas de Tempo (Year-over-Year, Month-over-Month)

```dax
-- ANÁLISE TEMPORAL

Previous Year Revenue = 
CALCULATE(
    [Total Revenue],
    DATEADD(dim_date[date_key], -1, YEAR)
)

YoY Growth $ = 
[Total Revenue] - [Previous Year Revenue]

YoY Growth % = 
DIVIDE(
    [YoY Growth $],
    [Previous Year Revenue],
    0
) * 100

Previous Month Revenue = 
CALCULATE(
    [Total Revenue],
    DATEADD(dim_date[date_key], -1, MONTH)
)

MoM Growth % = 
DIVIDE(
    [Total Revenue] - [Previous Month Revenue],
    [Previous Month Revenue],
    0
) * 100

Running Total Revenue = 
CALCULATE(
    [Total Revenue],
    FILTER(
        ALL(dim_date[date_key]),
        dim_date[date_key] <= MAX(dim_date[date_key])
    )
)

Revenue MTD = 
CALCULATE(
    [Total Revenue],
    FILTER(
        dim_date,
        MONTH(dim_date[date_key]) = MONTH(MAX(dim_date[date_key]))
        && YEAR(dim_date[date_key]) = YEAR(MAX(dim_date[date_key]))
    )
)

Revenue YTD = 
CALCULATE(
    [Total Revenue],
    FILTER(
        dim_date,
        YEAR(dim_date[date_key]) = YEAR(MAX(dim_date[date_key]))
        && dim_date[date_key] <= MAX(dim_date[date_key])
    )
)
```

---

### 2.3 Medidas de Cliente (Segmentação)

```dax
-- ANÁLISE DE CLIENTE

Premium Customers Revenue = 
CALCULATE(
    [Total Revenue],
    dim_customer[customer_segment] = "Premium"
)

VIP Customers Revenue = 
CALCULATE(
    [Total Revenue],
    dim_customer[customer_segment] = "VIP"
)

Customer Count by Segment = 
SWITCH(
    SELECTEDVALUE(dim_customer[customer_segment]),
    "Premium", [Premium Customer Count],
    "VIP", [VIP Customer Count],
    "Gold", [Gold Customer Count],
    "Silver", [Silver Customer Count],
    "Bronze", [Bronze Customer Count],
    BLANK()
)

Premium Customer Count = 
CALCULATE(
    [Total Customers],
    dim_customer[customer_segment] = "Premium"
)

VIP Customer Count = 
CALCULATE(
    [Total Customers],
    dim_customer[customer_segment] = "VIP"
)

-- RFM ANALYSIS

Champion Customers = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[customer_id]),
    dim_customer[rfm_segment] = "Champions"
)

At Risk Customers = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[customer_id]),
    dim_customer[rfm_segment] = "At Risk"
)

At Risk Revenue = 
CALCULATE(
    [Total Revenue],
    dim_customer[rfm_segment] = "At Risk"
)

Churn Risk % = 
DIVIDE(
    [At Risk Customers],
    [Total Customers],
    0
) * 100
```

---

### 2.4 Medidas de Produto

```dax
-- ANÁLISE DE PRODUTO

Revenue by Category = 
CALCULATE(
    [Total Revenue],
    FILTER(
        dim_product,
        dim_product[product_category] = SELECTEDVALUE(dim_product[product_category])
    )
)

Top Category = 
MAXX(
    FILTER(
        VALUES(dim_product[product_category]),
        [Revenue by Category] = CALCULATE(MAX([Revenue by Category]), ALL(dim_product))
    ),
    dim_product[product_category]
)

Average Rating = 
AVERAGEX(
    SUMMARIZE(fact_sales, fact_sales[order_id], "score", [Average Order Score]),
    [score]
)

Products with Reviews = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[product_id]),
    NOT(ISBLANK(fact_sales[review_score]))
)

High Rated Products = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[product_id]),
    fact_sales[review_score] >= 4
)
```

---

### 2.5 Medidas Avançadas

```dax
-- ANÁLISE AVANÇADA

Customer Lifetime Value Avg = 
AVERAGEX(
    SUMMARIZE(dim_customer, dim_customer[customer_id], "LTV", dim_customer[lifetime_value]),
    [LTV]
)

Purchase Frequency Avg = 
AVERAGEX(
    SUMMARIZE(dim_customer, dim_customer[customer_id], "freq", dim_customer[total_orders]),
    [freq]
)

Days Since Last Purchase Avg = 
AVERAGEX(
    SUMMARIZE(
        dim_customer,
        dim_customer[customer_id],
        "days_since", dim_customer[days_since_last_purchase]
    ),
    [days_since]
)

New Customers This Month = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[customer_id]),
    FILTER(
        dim_customer,
        YEAR(dim_customer[first_purchase_date]) = YEAR(MAX(dim_date[date_key]))
        && MONTH(dim_customer[first_purchase_date]) = MONTH(MAX(dim_date[date_key]))
    )
)

Repeat Purchase Rate = 
VAR total_unique_customers = [Total Customers]
VAR repeat_customers = CALCULATE(
    DISTINCTCOUNT(fact_sales[customer_id]),
    FILTER(
        dim_customer,
        dim_customer[total_orders] > 1
    )
)
RETURN DIVIDE(repeat_customers, total_unique_customers, 0) * 100

Revenue per Transaction = 
DIVIDE([Total Revenue], [Total Orders], 0)

Average Transaction Size = 
DIVIDE(
    [Total Items Sold],
    [Total Orders],
    0
)
```

---

## 🎨 PARTE 3: ESTRUTURA DE DASHBOARDS

### Dashboard 1: Executive Summary (Executivos)

**Objetivo**: Visão geral do negócio em 1 página

**Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│  KPI CARDS (TOP)                                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │ Revenue │  │ Orders  │  │ Customers│  │AOV      │       │
│  │ R$ XXX  │  │ X.XXX   │  │ X.XXX    │  │ R$ XX   │       │
│  │ +XX% MoM│  │+XX% MoM │  │ +XX% MoM │  │+XX% MoM │       │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘       │
│                                                             │
│  CHARTS (MAIN)                                              │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Revenue Trend                 │ Orders by Status    │   │
│  │ (Line Chart - Last 12 months) │ (Pie Chart)        │   │
│  │                               │                      │   │
│  │                               │                      │   │
│  └──────────────────────────────┴──────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Top 5 Categories             │ Top 5 Customers     │   │
│  │ (Horizontal Bar)              │ (Horizontal Bar)    │   │
│  │                               │                      │   │
│  └──────────────────────────────┴──────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Filtros:**
- Período (Data)
- Região (Estado/Cidade)
- Categoria de Produto

**Medidas principais:**
- Total Revenue, Total Orders, Total Customers, Avg Order Value
- Revenue MoM growth, Order completion rate

---

### Dashboard 2: Customer Analytics

**Objetivo**: Entender segmentação, comportamento e retenção de clientes

**Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│  CUSTOMER SEGMENTS OVERVIEW                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │ Premium │  │  VIP    │  │  Gold   │  │ Bronze  │       │
│  │ X.XXX   │  │ X.XXX   │  │ X.XXX   │  │ X.XXX   │       │
│  │ R$ XXX  │  │ R$ XXX  │  │ R$ XXX  │  │ R$ XXX  │       │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘       │
│                                                             │
│  RFM ANALYSIS                                               │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Customers by RFM Segment     │ Revenue by RFM      │   │
│  │ (Donut Chart)                │ (Stacked Bar)       │   │
│  │                               │                      │   │
│  │ - Champions                  │                      │   │
│  │ - Loyal                       │                      │   │
│  │ - At Risk                     │                      │   │
│  └──────────────────────────────┴──────────────────────┘   │
│                                                             │
│  CUSTOMER METRICS                                           │
│  ┌──────────────────────┬──────────────────────────────┐   │
│  │ Scatter Plot         │ Customer Details Table      │   │
│  │ (LTV vs Frequency)   │ (Top 20 Customers)         │   │
│  │                      │ Name | LTV | Orders | Status│   │
│  └──────────────────────┴──────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Filtros:**
- Customer Segment
- RFM Segment
- Tempo (First Purchase Date)

**Medidas principais:**
- Customer Lifetime Value, Repeat Purchase Rate
- Days Since Last Purchase, At Risk Customers
- Champion Customers

---

### Dashboard 3: Product Performance

**Objetivo**: Analisar desempenho de categorias, produtos e pricing

**Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│  PRODUCT KPIs                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Total Revenue│  │ Unique SKUs  │  │ Avg Rating  │     │
│  │ R$ XXX       │  │ X.XXX        │  │ X.X / 5.0   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                             │
│  SALES BY CATEGORY                                          │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Revenue by Category          │ Qty by Category     │   │
│  │ (Horizontal Bar)              │ (Horizontal Bar)    │   │
│  │                               │                      │   │
│  │ 1. Categoria A                │ 1. Categoria X     │   │
│  │ 2. Categoria B                │ 2. Categoria Y     │   │
│  │ 3. Categoria C                │ 3. Categoria Z     │   │
│  └──────────────────────────────┴──────────────────────┘   │
│                                                             │
│  TOP/BOTTOM PRODUCTS                                        │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Top 10 Products by Revenue   │ Bottom 10 Products  │   │
│  │ (Table)                       │ (Table)             │   │
│  │ SKU | Revenue | Quantity     │ SKU | Revenue       │   │
│  └──────────────────────────────┴──────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Filtros:**
- Product Category
- Price Range
- Rating (High-rated only)

**Medidas principais:**
- Revenue by Category, Quantity Sold
- Average Rating, High Rated Products %
- Revenue per Product

---

### Dashboard 4: Temporal Analysis (Sazonalidade e Trends)

**Objetivo**: Identificar padrões, sazonalidade e tendências temporais

**Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│  GROWTH METRICS                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ YoY Growth % │  │ MoM Growth % │  │ Revenue MTD  │     │
│  │ +XX.X%       │  │ +XX.X%       │  │ R$ XXX       │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                             │
│  TRENDS                                                     │
│  ┌──────────────────────────────┬──────────────────────┐   │
│  │ Daily Revenue Trend          │ Revenue by Day of   │   │
│  │ (Line Chart - 30/90 days)    │ Week (Column Chart) │   │
│  │ with Forecast                │                      │   │
│  └──────────────────────────────┴──────────────────────┘   │
│                                                             │
│  SEASONALITY HEATMAP                                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Revenue by Month & Day of Week (Matrix Heatmap)     │   │
│  │ Jan  Feb  Mar  Apr  May  Jun  Jul  Aug  Sep  Oct    │   │
│  │ Mon  🟦  🟦  🟥  🟥  🟥  🟥  🟥  🟥  🟦  🟦    │   │
│  │ Tue  🟦  🟦  🟥  🟥  🟥  🟥  🟥  🟥  🟦  🟦    │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Filtros:**
- Data range customizável
- Comparação período anterior

**Medidas principais:**
- YoY Growth, MoM Growth, Revenue MTD
- Revenue Forecast, Seasonality Index

---

## 🔧 PARTE 4: CHECKLIST DE IMPLEMENTAÇÃO

### Antes de começar (Setup)
- [ ] Conta Kaggle criada e dataset baixado
- [ ] Microsoft Fabric workspace criado
- [ ] Power BI Desktop instalado
- [ ] Acesso ao Lakehouse no Fabric

### Fase 1: Ingestão
- [ ] CSV/arquivo importado no Lakehouse
- [ ] Pasta /raw criada
- [ ] raw_orders, raw_customers, raw_products carregados
- [ ] Contagem de linhas validada

### Fase 2: Armazenamento
- [ ] Pastas /raw, /silver, /gold criadas
- [ ] Estrutura de dados documentada
- [ ] Tabelas raw_* acessíveis

### Fase 3: Transformação
- [ ] silver_orders criada e testada
- [ ] silver_customer_metrics criada
- [ ] silver_order_items_enriched criada
- [ ] silver_seller_metrics criada
- [ ] Data quality checks passando

### Fase 4: Modelagem
- [ ] dim_date criada (1095 dias)
- [ ] dim_customer criada com segmentação
- [ ] dim_product criada com categoria
- [ ] fact_sales criada
- [ ] Índices criados para performance
- [ ] Relacionamentos validados

### Fase 5: Power BI
- [ ] Fabric Lakehouse conectado ao Power BI
- [ ] Tabelas gold_* importadas
- [ ] Relacionamentos criados no modelo
- [ ] Medidas DAX implementadas (mínimo 20)
- [ ] 4 dashboards principais criados
- [ ] Filtros cruzados funcionando
- [ ] Formatação visual finalizada

### Fase 6: Governança
- [ ] Lineage documentado
- [ ] README/METADATA.md criado
- [ ] Data quality checks agendados
- [ ] Alertas configurados

### Fase 7: Manutenção
- [ ] Notebook com testes agendado diariamente
- [ ] Logs de execução armazenados
- [ ] Versionamento iniciado
- [ ] Documentação atualizada

---

## 🚀 PRÓXIMAS ITERAÇÕES (Avançado)

### Machine Learning Integration
```python
# Churn Prediction
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Features: days_since_last_purchase, lifetime_value, frequency_score
# Target: customer_active (1/0)
```

### Real-time Streaming
- Apache Kafka → Fabric
- Real-time customer events
- Live dashboard updates

### Advanced Segmentation
- K-means clustering
- RFM scoring refinement
- Propensity modeling

---

