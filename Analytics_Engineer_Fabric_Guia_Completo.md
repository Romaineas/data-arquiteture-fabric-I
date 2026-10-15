# Analytics Engineer no Fabric - Guia Completo do Zero

## 📌 Objetivo deste Guia

Construir um projeto **Analytics Engineer completo** no Microsoft Fabric, desde a ingestão de dados até visualizações prontas em Power BI. Será usado um dataset do **Kaggle** para demonstrar cada fase em detalhes.

---

## 🎯 Cenário: Projeto de Analytics - E-commerce (Marketing)

**Dataset Recomendado**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) ou [Online Retail Dataset](https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset)

**Objetivo**: Analisar vendas, comportamento de clientes e performance de marketing para insights actionáveis.

---

# FASE 1: INGESTÃO DE DADOS

## O que é?
Trazer dados de fontes externas para o Fabric (APIs, arquivos, databases, data lakes).

## Métodos no Fabric:

### 1.1 Upload de Arquivo CSV/Excel
```
Lakehouse → Get Data → Upload files → Selecione seu CSV do Kaggle
```

### 1.2 Usando Dataflows (UI)
- **Quando usar**: Não quer programar, quer interface visual
- **Como**:
  1. Vá a `Workspaces → New → Dataflow Gen2`
  2. `Get Data → Upload file`
  3. Configure transformações básicas (rename, filter)
  4. Publish para o Lakehouse

### 1.3 Usando Python/Spark (Notebook)
**Melhor para**: Dados em grande volume, APIs, processamento complexo

```python
# Notebook no Fabric
import pandas as pd

# Exemplo 1: Upload CSV local
orders_df = pd.read_csv('/Workspace/dados/orders.csv')

# Exemplo 2: Leitura de arquivo no Lakehouse
# (se você já tem o arquivo lá)
orders_df = spark.read.csv('/lakehouse/default/orders.csv', header=True)

# Exemplo 3: API externa (se tiver token)
import requests
response = requests.get('https://api.example.com/orders')
data = response.json()
df = pd.DataFrame(data)

# Salvar no Lakehouse (Raw/Bronze)
table_name = 'raw_orders'
df.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable(table_name)
```

### 1.4 Checklist da Fase 1:
- [ ] Dataset do Kaggle baixado
- [ ] Arquivo CSV/arquivo em formato estruturado
- [ ] Estrutura de pastas no Lakehouse criada (`/raw`, `/bronze`)
- [ ] Dados importados e verificados (row count, colunas)

---

# FASE 2: ARMAZENAMENTO (LAKEHOUSE)

## Estrutura Medallion Architecture (Melhor Prática)

```
Lakehouse/
├── raw/              ← Dados exatamente como chegaram (imutável)
├── bronze/           ← Validação + limpeza básica
├── silver/           ← Dados transformados e estruturados
└── gold/             ← Modelagem final (dimensões + fatos)
```

### 2.1 Criar a Estrutura no Fabric

```python
# Notebook - Criar pastas e tabelas iniciais

# 1. Ler CSV bruto
raw_orders = spark.read.csv('/lakehouse/default/raw/orders.csv', header=True, inferSchema=True)

# 2. Verificar qualidade básica
print(f"Linhas: {raw_orders.count()}")
print(f"Colunas: {raw_orders.columns}")
raw_orders.display()

# 3. Salvar como tabela no Lakehouse (Raw Layer)
raw_orders.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('raw_orders')

# 4. Repetir para outros datasets (customers, products, reviews)
# raw_customers, raw_products, raw_reviews
```

### 2.2 Estrutura de Dados Recomendada (E-commerce)

| Tabela | Descrição |
|--------|-----------|
| `raw_orders` | Pedidos brutos (order_id, customer_id, order_date, total_value) |
| `raw_customers` | Clientes brutos (customer_id, email, location, signup_date) |
| `raw_products` | Produtos brutos (product_id, category, price, name) |
| `raw_reviews` | Avaliações brutos (review_id, order_id, rating, comment) |
| `raw_order_items` | Itens do pedido (item_id, order_id, product_id, quantity) |

### 2.3 Checklist da Fase 2:
- [ ] Lakehouse criado com estrutura Medallion
- [ ] Pastas raw/, bronze/, silver/, gold/ criadas
- [ ] Tabelas raw_ criadas com dados validados
- [ ] Schemas documentados

---

# FASE 3: TRANSFORMAÇÃO (ETL)

## O que é?
**E**xtract → **T**ransform → **L**oad
- Limpar dados (remover nulos, duplicatas)
- Validar tipos de dados
- Enriquecer dados (combinar tabelas)
- Criar camada Silver estruturada

## 3.1 Transformação com Spark SQL (Recomendado)

```python
# Notebook - Criar Silver Layer

# 1. LIMPEZA: Remove nulos, duplicatas, valida datas
spark.sql("""
CREATE OR REPLACE TABLE silver_orders AS
SELECT
    CAST(order_id AS STRING) as order_id,
    CAST(customer_id AS STRING) as customer_id,
    CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE) as order_date,
    CAST(total_value AS DECIMAL(10, 2)) as total_value,
    order_status,
    CURRENT_TIMESTAMP as ingestion_time
FROM raw_orders
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND total_value > 0
  AND order_status IN ('delivered', 'shipped', 'pending', 'cancelled')
""")

# 2. ENRIQUECIMENTO: Join com clientes
spark.sql("""
CREATE OR REPLACE TABLE silver_orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_value,
    o.order_status,
    c.customer_email,
    c.customer_state,
    c.customer_city,
    YEAR(o.order_date) as year_order,
    MONTH(o.order_date) as month_order,
    QUARTER(o.order_date) as quarter_order
FROM silver_orders o
LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
WHERE o.order_status != 'cancelled'
""")

# 3. AGREGAÇÃO: Cria métricas por cliente
spark.sql("""
CREATE OR REPLACE TABLE silver_customer_metrics AS
SELECT
    customer_id,
    customer_email,
    customer_state,
    COUNT(order_id) as total_orders,
    ROUND(SUM(total_value), 2) as lifetime_value,
    MAX(order_date) as last_purchase_date,
    ROUND(AVG(total_value), 2) as avg_order_value,
    MIN(order_date) as first_purchase_date
FROM silver_orders_enriched
GROUP BY customer_id, customer_email, customer_state
HAVING COUNT(order_id) > 0
""")
```

## 3.2 Transformação com PySpark (Dados Maiores)

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Ler dados brutos
orders_raw = spark.table('raw_orders')

# 1. Validar tipos e limpar
orders_clean = (orders_raw
    .filter(F.col('order_id').isNotNull())
    .filter(F.col('customer_id').isNotNull())
    .filter(F.col('total_value') > 0)
    .withColumn('order_date', F.to_date(F.col('order_date'), 'yyyy-MM-dd'))
    .withColumn('total_value', F.round(F.col('total_value'), 2))
    .dropDuplicates(['order_id'])
)

# 2. Adicionar dimensões de tempo
orders_temporal = (orders_clean
    .withColumn('year', F.year(F.col('order_date')))
    .withColumn('month', F.month(F.col('order_date')))
    .withColumn('quarter', F.quarter(F.col('order_date')))
    .withColumn('week', F.weekofyear(F.col('order_date')))
    .withColumn('day_of_week', F.dayofweek(F.col('order_date')))
)

# 3. Salvar como tabela
orders_temporal.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('silver_orders_v2')
```

## 3.3 Validação de Dados (Data Quality Checks)

```python
# Verificar qualidade após transformação

silver_orders = spark.table('silver_orders')

print("=== DATA QUALITY CHECKS ===")
print(f"Total de linhas: {silver_orders.count()}")
print(f"Pedidos com NULL order_id: {silver_orders.filter(F.col('order_id').isNull()).count()}")
print(f"Pedidos com NULL customer_id: {silver_orders.filter(F.col('customer_id').isNull()).count()}")

# Distribuição por status
print("\nDistribuição de Status:")
silver_orders.groupBy('order_status').count().display()

# Range de valores
print("\nRanges:")
silver_orders.agg(
    F.min('total_value'),
    F.max('total_value'),
    F.avg('total_value')
).display()
```

### 3.4 Checklist da Fase 3:
- [ ] Dados limpos (sem nulos críticos, sem duplicatas)
- [ ] Tipos de dados validados (STRING, INT, DECIMAL, DATE)
- [ ] Tabelas Silver criadas
- [ ] Joins com outras tabelas realizados
- [ ] Testes de qualidade executados

---

# FASE 4: MODELAGEM (GOLD LAYER - STAR SCHEMA)

## O que é Star Schema?

```
        ┌─────────────┐
        │  DIM_DATE   │
        │  (calendário)
        └──────┬──────┘
               │
    ┌──────────┼──────────┐
    │          │          │
┌───┴────┐ ┌──┴───┐ ┌────┴──┐
│DIM_CUST │ │FACT_ │ │DIM_PRD│
│OMER    │ │SALES │ │UCT    │
└────────┘ └──────┘ └───────┘
    ↑          ↑         ↑
  Lookup  Measures   Lookup
```

- **FACT_SALES**: Transações (order_id, customer_id, product_id, date_id, revenue, quantity)
- **DIM_CUSTOMER**: Clientes (customer_id, name, state, city, segment)
- **DIM_PRODUCT**: Produtos (product_id, name, category, price)
- **DIM_DATE**: Calendário (date_id, date, year, month, quarter)

## 4.1 Criar Dimensões

```python
# DIM_DATE - Tabela de calendário (essencial para BI)
spark.sql("""
CREATE OR REPLACE TABLE dim_date AS
WITH dates AS (
  SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2025-12-31'), INTERVAL 1 DAY)) as date
)
SELECT
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) as date_id,
    date as date_key,
    YEAR(date) as year,
    MONTH(date) as month,
    QUARTER(date) as quarter,
    DAYOFWEEK(date) as day_of_week,
    DATE_FORMAT(date, 'EEEE') as day_name,
    WEEK(date) as week_number
FROM dates
ORDER BY date_id
""")

# DIM_CUSTOMER - Dimensão de clientes
spark.sql("""
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    CAST(customer_id AS STRING) as customer_id,
    customer_name,
    customer_email,
    customer_state,
    customer_city,
    CASE 
        WHEN lifetime_value > 1000 THEN 'Premium'
        WHEN lifetime_value > 500 THEN 'Gold'
        ELSE 'Standard'
    END as customer_segment,
    first_purchase_date,
    CURRENT_TIMESTAMP as loaded_date
FROM silver_customer_metrics
""")

# DIM_PRODUCT - Dimensão de produtos
spark.sql("""
CREATE OR REPLACE TABLE dim_product AS
SELECT
    CAST(product_id AS STRING) as product_id,
    product_name,
    product_category,
    CAST(product_price AS DECIMAL(10, 2)) as product_price,
    CASE 
        WHEN product_price > 500 THEN 'High'
        WHEN product_price > 100 THEN 'Medium'
        ELSE 'Low'
    END as price_range,
    CURRENT_TIMESTAMP as loaded_date
FROM raw_products
WHERE product_id IS NOT NULL
""")
```

## 4.2 Criar Fact Table

```python
# FACT_SALES - Tabela de fatos (transações)
spark.sql("""
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) as date_id,
    o.customer_id,
    oi.product_id,
    o.order_id,
    oi.item_id as line_item_id,
    CAST(oi.quantity AS INT) as quantity,
    CAST(oi.item_price AS DECIMAL(10, 2)) as unit_price,
    CAST(oi.quantity * oi.item_price AS DECIMAL(10, 2)) as line_amount,
    o.total_value as order_total,
    o.order_status,
    CASE 
        WHEN o.order_status = 'delivered' THEN 1
        ELSE 0
    END as is_completed,
    CURRENT_TIMESTAMP as loaded_timestamp
FROM silver_orders_enriched o
LEFT JOIN raw_order_items oi ON o.order_id = oi.order_id
WHERE o.order_date IS NOT NULL
  AND oi.quantity > 0
""")

-- Criar índices para performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
```

## 4.3 Checklist da Fase 4:
- [ ] DIM_DATE criada (tabela de calendário)
- [ ] DIM_CUSTOMER criada
- [ ] DIM_PRODUCT criada
- [ ] FACT_SALES criada
- [ ] Relacionamentos validados (joins funcionam)
- [ ] Performance testada (queries retornam rápido)

---

# FASE 5: VISUALIZAÇÃO (POWER BI)

## 5.1 Conectar ao Power BI Desktop

1. **Abra Power BI Desktop**
2. **Get Data → Fabric Lakehouse**
3. **Selecione seu Lakehouse**
4. **Selecione tabelas**: `dim_date`, `dim_customer`, `dim_product`, `fact_sales`
5. **Load**

## 5.2 Modelagem no Power BI

```
Vá em: Model View

1. Crie relacionamentos:
   - FACT_SALES.date_id ← DIM_DATE.date_id (1 para muitos)
   - FACT_SALES.customer_id ← DIM_CUSTOMER.customer_id (1 para muitos)
   - FACT_SALES.product_id ← DIM_PRODUCT.product_id (1 para muitos)

2. Marque DIM_* como dimensões (Mark as Date Table para DIM_DATE)

3. Configure atributos:
   - Hide IDs das tabelas de dimensão
   - Hide Technical columns
```

## 5.3 Criar Medidas (DAX)

```dax
// Medidas de Vendas
Total Revenue = SUM(fact_sales[line_amount])

Total Orders = DISTINCTCOUNT(fact_sales[order_id])

Average Order Value = [Total Revenue] / [Total Orders]

Completed Orders = CALCULATE(
    DISTINCTCOUNT(fact_sales[order_id]),
    fact_sales[is_completed] = 1
)

Completion Rate = 
DIVIDE(
    [Completed Orders],
    [Total Orders],
    0
) * 100

// Medidas de Cliente
Total Customers = DISTINCTCOUNT(fact_sales[customer_id])

Customer Lifetime Value = 
CALCULATE(
    SUM(dim_customer[lifetime_value]),
    FILTER(dim_customer, dim_customer[customer_segment] <> "")
)

// Medidas de Crescimento
MoM Growth % = 
VAR current = [Total Revenue]
VAR previous = CALCULATE([Total Revenue], DATEADD(dim_date[date_key], -1, MONTH))
RETURN DIVIDE(current - previous, previous, 0) * 100
```

## 5.4 Dashboards Recomendados

### Dashboard 1: Executive Summary
- Card: Total Revenue, Total Orders, Avg Order Value
- Line Chart: Revenue por mês (time series)
- Map: Revenue por estado
- Card: Top Customer Segment

### Dashboard 2: Customer Analytics
- Scatter: Lifetime Value vs Orders Quantity
- Bar: Clientes por segmento
- Table: Top 20 clientes
- Gauge: Completion Rate

### Dashboard 3: Product Performance
- Bar: Revenue por categoria
- Bar: Quantidade vendida por produto (Top 10)
- Pie: Market Share por categoria
- Table: Produtos com margens e volumes

### Dashboard 4: Temporal Analysis
- Line: Revenue trend (diário, semanal, mensal)
- Heat Map: Revenue por dia da semana + hora
- KPI Cards: YoY Growth, MoM Growth
- Decomposition Tree: Revenue drivers

## 5.5 Checklist da Fase 5:
- [ ] Power BI conectado ao Lakehouse
- [ ] Tabelas importadas com sucesso
- [ ] Relacionamentos criados e validados
- [ ] Medidas DAX implementadas
- [ ] 4 dashboards principais criados
- [ ] Formatação e interatividade adicionadas

---

# FASE 6: GOVERNANÇA & QUALIDADE

## 6.1 Data Quality Tests

```python
# Notebook - Testes de Qualidade

import pandas as pd
from datetime import datetime

def run_data_quality_checks():
    
    # Test 1: Null values
    orders = spark.table('silver_orders')
    null_check = orders.select([
        sum(F.col(c).isNull().cast('int')).alias(c) 
        for c in orders.columns
    ]).collect()
    
    print("=== NULL VALUES ===")
    for col, count in null_check[0].asDict().items():
        if count > 0:
            print(f"⚠️ {col}: {count} nulls")
    
    # Test 2: Duplicates
    duplicates = orders.groupBy(orders.columns).count().filter('count > 1').count()
    print(f"\n=== DUPLICATES ===")
    print(f"Linhas duplicadas: {duplicates}")
    
    # Test 3: Value Range
    print(f"\n=== VALUE RANGES ===")
    orders.agg(
        F.min('total_value').alias('min_value'),
        F.max('total_value').alias('max_value'),
        F.avg('total_value').alias('avg_value'),
        F.stddev('total_value').alias('stddev')
    ).display()
    
    # Test 4: Referential Integrity
    print(f"\n=== REFERENTIAL INTEGRITY ===")
    
    # Verificar se todos os customer_ids em orders existem em customers
    orders_cust = spark.table('silver_orders').select('customer_id').distinct()
    customers = spark.table('raw_customers').select('customer_id').distinct()
    
    missing = orders_cust.join(customers, 'customer_id', 'leftanti').count()
    print(f"Customer IDs em orders que NÃO estão em customers: {missing}")
    
    # Test 5: Data Freshness
    print(f"\n=== DATA FRESHNESS ===")
    latest_date = orders.agg(F.max('order_date')).collect()[0][0]
    days_old = (datetime.now() - latest_date.date()).days
    print(f"Dados mais recentes: {latest_date}")
    print(f"Dias de atraso: {days_old} dias")
    
    if days_old > 7:
        print("⚠️ AVISO: Dados estão desatualizados!")
    
    return True

run_data_quality_checks()
```

## 6.2 Data Lineage (Documentar)

```python
# Criar documento de lineage
lineage = {
    "raw_orders": {
        "source": "Kaggle Brazilian E-commerce",
        "description": "Dados brutos de pedidos",
        "columns": ["order_id", "customer_id", "order_date", "total_value"],
        "updated_date": "2025-01-15",
        "quality_score": 0.95
    },
    "silver_orders": {
        "source": "raw_orders",
        "transformation": "Limpeza, validação de tipos, remoção de nulos",
        "description": "Pedidos validados e estruturados",
        "updated_date": "2025-01-15",
        "quality_score": 0.98
    },
    "fact_sales": {
        "source": ["silver_orders", "raw_order_items"],
        "transformation": "Star schema, joins, agregações",
        "description": "Tabela de fatos pronta para BI",
        "updated_date": "2025-01-15",
        "quality_score": 0.99
    }
}

# Salvar como tabela
import json
lineage_df = pd.DataFrame([
    {
        'table_name': table,
        'source': str(info.get('source')),
        'description': info.get('description'),
        'quality_score': info.get('quality_score'),
        'lineage_json': json.dumps(info)
    }
    for table, info in lineage.items()
])

lineage_df.write.mode('overwrite').saveAsTable('metadata_lineage')
```

## 6.3 Documentação (README)

Crie um arquivo **METADATA.md** no seu workspace Fabric:

```markdown
# E-commerce Analytics Project

## Overview
Projeto de Analytics Engineer para análise de vendas e clientes em e-commerce.

## Data Source
- Dataset: Kaggle Brazilian E-commerce
- Last Updated: 2025-01-15
- Refresh Frequency: Daily

## Layers

### Raw Layer (raw_*)
Dados exatamente como recebidos. Não modifique.

### Silver Layer (silver_*)
Dados limpos e validados. Pronto para enriquecimento.

### Gold Layer (dim_*, fact_*)
Star schema pronto para Power BI.

## Key Metrics
- Total Revenue: R$ XX.XXX
- Total Orders: X.XXX
- Customer Lifetime Value: R$ XX.XX
- Completion Rate: XX%

## Lineage
Veja tabela `metadata_lineage` para traceabilidade completa.

## Quality Checks
- Executed daily at 02:00 AM
- Last run: 2025-01-15 02:15 AM
- Status: PASSED ✅
```

### 6.4 Checklist da Fase 6:
- [ ] Data quality checks implementados
- [ ] Lineage documentado
- [ ] README/METADATA criado
- [ ] Alertas configurados para dados atrasados
- [ ] Testes de integridade referencial passando

---

# FASE 7: MONITORAMENTO & MANUTENÇÃO

## 7.1 Configurar Alertas

```python
# Notebook - Alertas automáticos

from datetime import datetime, timedelta
import requests

def send_alert(title, message, severity='WARNING'):
    """Envia alerta via Teams/Email"""
    # Implementar com Teams webhook ou SendGrid
    print(f"[{severity}] {title}: {message}")

# Monitorar Pipeline
def monitor_pipeline():
    
    # 1. Verificar se há dados recentes
    orders = spark.table('silver_orders')
    latest = orders.agg(F.max('order_date')).collect()[0][0]
    days_old = (datetime.now() - latest).days
    
    if days_old > 1:
        send_alert(
            "Data Pipeline Alert",
            f"Last data update: {days_old} days ago",
            severity='CRITICAL'
        )
    
    # 2. Verificar volume
    current_count = orders.count()
    expected_min = 100  # Mínimo esperado
    
    if current_count < expected_min:
        send_alert(
            "Low Data Volume",
            f"Only {current_count} records (expected min: {expected_min})",
            severity='WARNING'
        )
    
    # 3. Verificar qualidade
    null_count = orders.filter(F.col('order_id').isNull()).count()
    if null_count > 0:
        send_alert(
            "Data Quality Issue",
            f"{null_count} null order_ids detected",
            severity='CRITICAL'
        )

monitor_pipeline()
```

## 7.2 Scheduling com Fabric

1. **Criar Schedule**:
   - Vá ao seu Notebook
   - Schedule → Schedule run
   - Configure para rodar todos os dias às 02:00 AM

2. **Configurar Retry**:
   - Se falhar, tentar 2x mais
   - Enviar notificação de falha

## 7.3 Versionamento & Changes

```python
# Manter histórico de mudanças

versioning = {
    "v1.0": {
        "date": "2025-01-15",
        "changes": "Initial release",
        "tables": ["raw_orders", "raw_customers"],
        "quality_score": 0.95
    },
    "v1.1": {
        "date": "2025-01-20",
        "changes": "Added silver layer transformations",
        "tables": ["silver_orders", "silver_customer_metrics"],
        "quality_score": 0.98
    },
    "v2.0": {
        "date": "2025-01-25",
        "changes": "Implemented star schema (fact_sales, dim_*)",
        "tables": ["fact_sales", "dim_customer", "dim_product", "dim_date"],
        "quality_score": 0.99
    }
}

# Salvar como tabela
version_df = pd.DataFrame([
    {
        'version': version,
        'date': info['date'],
        'changes': info['changes'],
        'quality_score': info['quality_score']
    }
    for version, info in versioning.items()
])

version_df.write.mode('overwrite').saveAsTable('metadata_versions')
```

## 7.4 Performance Monitoring

```python
# Monitor query performance
spark.sql("""
SELECT 
    table_name,
    COUNT(*) as row_count,
    SIZE(table_name) / 1024 / 1024 as size_mb
FROM information_schema.tables
WHERE table_schema = 'default'
""").display()
```

### 7.5 Checklist da Fase 7:
- [ ] Monitoramento implementado
- [ ] Alertas configurados
- [ ] Schedulings de pipeline criados
- [ ] Versionamento documentado
- [ ] Performance baseline estabelecida
- [ ] Runbooks de troubleshooting criados

---

# 📋 RESUMO EXECUTIVO - CHECKLIST FINAL

## Antes de começar:
- [ ] Kaggle account criado
- [ ] Microsoft Fabric workspace criado
- [ ] Power BI Desktop instalado

## Fase 1: Ingestão
- [ ] Dataset baixado do Kaggle
- [ ] Dados importados para Lakehouse

## Fase 2: Armazenamento
- [ ] Estrutura Medallion criada
- [ ] Tabelas raw_ criadas

## Fase 3: Transformação
- [ ] Tabelas silver_ criadas
- [ ] Data quality checks passando

## Fase 4: Modelagem
- [ ] Dimensões (dim_*) criadas
- [ ] Fact table (fact_sales) criada
- [ ] Relacionamentos validados

## Fase 5: Visualização
- [ ] Power BI conectado
- [ ] 4+ dashboards criados
- [ ] DAX measures implementadas

## Fase 6: Governança
- [ ] Lineage documentado
- [ ] README criado
- [ ] Qualidade testada

## Fase 7: Manutenção
- [ ] Alertas configurados
- [ ] Schedulings criados
- [ ] Versionamento iniciado

---

# 🚀 PRÓXIMOS PASSOS

1. **Expandir modelo**: Adicionar previsões (ML), cohort analysis, churn modeling
2. **Automatizar**: Criar pipelines de CI/CD com Fabric
3. **Documentação**: Criar wiki com business rules e cálculos
4. **Segurança**: Implementar Row-Level Security (RLS) no Power BI
5. **Portfolio**: Publicar projeto no GitHub com screenshots de dashboards

---

# 📚 RECURSOS

- [Microsoft Fabric Docs](https://learn.microsoft.com/en-us/fabric/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/onboarding-data-to-medallion-lakehouse-architecture.html)
- [Kaggle Datasets](https://www.kaggle.com/datasets)
- [Star Schema Best Practices](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema)
- [DAX Functions](https://dax.guide/)

