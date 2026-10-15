"""
Analytics Engineer no Fabric - Código Completo Pronto para Executar
=====================================================================

Este script contém todas as fases de um projeto Analytics Engineer.
Executar passo a passo em um Notebook do Fabric.

Dataset Recomendado: Brazilian E-Commerce Public Dataset (Kaggle)
"""

# ============================================================================
# FASE 1: INGESTÃO DE DADOS
# ============================================================================
# Execute isto no seu Lakehouse

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("=" * 60)
print("FASE 1: INGESTÃO DE DADOS")
print("=" * 60)

# Opção 1: Se você já tem o CSV no seu Lakehouse
# (após fazer upload via UI)

try:
    # Ler CSV bruto
    orders_raw = spark.read.csv(
        '/lakehouse/default/Files/orders.csv', 
        header=True, 
        inferSchema=True
    )
    
    print(f"✅ Arquivo orders.csv lido com sucesso")
    print(f"   Linhas: {orders_raw.count()}")
    print(f"   Colunas: {', '.join(orders_raw.columns)}")
    
    # Visualizar primeiras linhas
    orders_raw.display()
    
except Exception as e:
    print(f"❌ Erro ao ler CSV: {e}")
    print("   Solução: Faça upload do arquivo via Lakehouse → Files")

# Opção 2: Criar dados de exemplo (se não tiver dataset Kaggle)
print("\n--- Criando dados de exemplo ---")

from datetime import datetime, timedelta
import random

# Simular dados de orders
dates = [datetime(2024, 1, 1) + timedelta(days=x) for x in range(365)]
orders_data = [
    {
        'order_id': f'ORD-{10000 + i}',
        'customer_id': f'CUST-{1000 + (i % 100)}',
        'order_date': random.choice(dates).strftime('%Y-%m-%d'),
        'total_value': round(random.uniform(50, 5000), 2),
        'order_status': random.choice(['delivered', 'shipped', 'pending', 'cancelled'])
    }
    for i in range(1000)  # 1000 registros de exemplo
]

orders_df = spark.createDataFrame(orders_data)
print(f"✅ Dados de exemplo criados: {orders_df.count()} registros")

# Salvar como tabela no Lakehouse (RAW LAYER)
orders_df.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('raw_orders')
print("✅ Tabela 'raw_orders' criada no Lakehouse")

# ============================================================================
# FASE 2: ARMAZENAMENTO (LAKEHOUSE)
# ============================================================================

print("\n" + "=" * 60)
print("FASE 2: ARMAZENAMENTO (LAKEHOUSE)")
print("=" * 60)

# Criar dados adicionais (clientes, produtos)
customers_data = [
    {
        'customer_id': f'CUST-{1000 + i}',
        'customer_name': f'Customer {i}',
        'customer_email': f'customer{i}@example.com',
        'customer_state': random.choice(['SP', 'RJ', 'MG', 'BA', 'RS']),
        'customer_city': f'City {i}',
        'signup_date': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
    }
    for i in range(100)
]

customers_df = spark.createDataFrame(customers_data)
customers_df.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('raw_customers')
print("✅ Tabela 'raw_customers' criada")

# Produtos
products_data = [
    {
        'product_id': f'PROD-{2000 + i}',
        'product_name': f'Product {i}',
        'product_category': random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']),
        'product_price': round(random.uniform(10, 1000), 2)
    }
    for i in range(50)
]

products_df = spark.createDataFrame(products_data)
products_df.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('raw_products')
print("✅ Tabela 'raw_products' criada")

print("\n📊 Resumo das tabelas RAW:")
spark.sql("""
SELECT 
    'raw_orders' as table_name, COUNT(*) as row_count 
FROM raw_orders
UNION ALL
SELECT 'raw_customers', COUNT(*) FROM raw_customers
UNION ALL
SELECT 'raw_products', COUNT(*) FROM raw_products
""").display()

# ============================================================================
# FASE 3: TRANSFORMAÇÃO (SILVER LAYER)
# ============================================================================

print("\n" + "=" * 60)
print("FASE 3: TRANSFORMAÇÃO (SILVER LAYER)")
print("=" * 60)

# 3.1 Limpeza de dados
print("\n--- Limpeza de dados ---")

spark.sql("""
CREATE OR REPLACE TABLE silver_orders AS
SELECT
    CAST(order_id AS STRING) as order_id,
    CAST(customer_id AS STRING) as customer_id,
    CAST(TO_DATE(order_date, 'yyyy-MM-dd') AS DATE) as order_date,
    CAST(total_value AS DECIMAL(10, 2)) as total_value,
    order_status,
    CURRENT_TIMESTAMP as ingestion_time,
    YEAR(TO_DATE(order_date, 'yyyy-MM-dd')) as year_order,
    MONTH(TO_DATE(order_date, 'yyyy-MM-dd')) as month_order,
    QUARTER(TO_DATE(order_date, 'yyyy-MM-dd')) as quarter_order
FROM raw_orders
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND total_value > 0
""")
print("✅ Tabela 'silver_orders' criada (dados limpos)")

# 3.2 Enriquecimento com join
print("\n--- Enriquecimento com customer data ---")

spark.sql("""
CREATE OR REPLACE TABLE silver_orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_value,
    o.order_status,
    c.customer_name,
    c.customer_email,
    c.customer_state,
    c.customer_city,
    o.year_order,
    o.month_order,
    o.quarter_order
FROM silver_orders o
LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
WHERE o.order_status != 'cancelled'
""")
print("✅ Tabela 'silver_orders_enriched' criada")

# 3.3 Agregações por cliente
print("\n--- Criando métricas de cliente ---")

spark.sql("""
CREATE OR REPLACE TABLE silver_customer_metrics AS
SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_state,
    COUNT(DISTINCT order_id) as total_orders,
    ROUND(SUM(total_value), 2) as lifetime_value,
    MAX(order_date) as last_purchase_date,
    ROUND(AVG(total_value), 2) as avg_order_value,
    MIN(order_date) as first_purchase_date,
    DATEDIFF(day, MIN(order_date), MAX(order_date)) as customer_tenure_days
FROM silver_orders_enriched
GROUP BY customer_id, customer_name, customer_email, customer_state
HAVING COUNT(order_id) > 0
""")
print("✅ Tabela 'silver_customer_metrics' criada")

# ============================================================================
# FASE 4: MODELAGEM (GOLD LAYER - STAR SCHEMA)
# ============================================================================

print("\n" + "=" * 60)
print("FASE 4: MODELAGEM (GOLD LAYER - STAR SCHEMA)")
print("=" * 60)

# 4.1 Criar DIM_DATE
print("\n--- Criando DIM_DATE (Tabela de Calendário) ---")

spark.sql("""
CREATE OR REPLACE TABLE dim_date AS
WITH dates AS (
  SELECT EXPLODE(SEQUENCE(TO_DATE('2023-01-01'), TO_DATE('2025-12-31'), INTERVAL 1 DAY)) as date
)
SELECT
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) as date_id,
    date as date_key,
    YEAR(date) as year,
    MONTH(date) as month,
    QUARTER(date) as quarter,
    DAYOFWEEK(date) as day_of_week,
    DATE_FORMAT(date, 'EEEE') as day_name,
    WEEK(date) as week_number,
    DAY(date) as day_of_month
FROM dates
ORDER BY date_id
""")
print(f"✅ Tabela 'dim_date' criada com {spark.table('dim_date').count()} dias")

# 4.2 Criar DIM_CUSTOMER
print("\n--- Criando DIM_CUSTOMER ---")

spark.sql("""
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_state,
    CASE 
        WHEN lifetime_value > 1000 THEN 'Premium'
        WHEN lifetime_value > 500 THEN 'Gold'
        WHEN lifetime_value > 100 THEN 'Silver'
        ELSE 'Standard'
    END as customer_segment,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    ROUND(lifetime_value, 2) as lifetime_value,
    CURRENT_TIMESTAMP as loaded_date
FROM silver_customer_metrics
""")
print("✅ Tabela 'dim_customer' criada")

# 4.3 Criar DIM_PRODUCT
print("\n--- Criando DIM_PRODUCT ---")

spark.sql("""
CREATE OR REPLACE TABLE dim_product AS
SELECT
    product_id,
    product_name,
    product_category,
    CAST(product_price AS DECIMAL(10, 2)) as product_price,
    CASE 
        WHEN product_price > 500 THEN 'Premium'
        WHEN product_price > 100 THEN 'Mid-range'
        ELSE 'Economy'
    END as price_segment,
    CURRENT_TIMESTAMP as loaded_date
FROM raw_products
WHERE product_id IS NOT NULL
""")
print("✅ Tabela 'dim_product' criada")

# 4.4 Criar FACT_SALES
print("\n--- Criando FACT_SALES (Tabela de Fatos) ---")

spark.sql("""
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) as date_id,
    o.customer_id,
    p.product_id,
    o.order_id,
    CAST(ABS(HASH(o.order_id, p.product_id)) % 1000 AS INT) as line_item_id,
    CAST(CAST(ABS(HASH(o.order_id)) % 10) + 1 AS INT) as quantity,
    p.product_price as unit_price,
    ROUND(CAST(ABS(HASH(o.order_id)) % 10 + 1 AS DECIMAL) * p.product_price, 2) as line_amount,
    o.total_value as order_total,
    o.order_status,
    CASE 
        WHEN o.order_status = 'delivered' THEN 1
        ELSE 0
    END as is_completed,
    CURRENT_TIMESTAMP as loaded_timestamp
FROM silver_orders o
LEFT JOIN dim_product p ON MOD(ABS(HASH(o.order_id)), 50) = MOD(ABS(HASH(p.product_id)), 50)
WHERE o.order_date IS NOT NULL
LIMIT 5000
""")
print("✅ Tabela 'fact_sales' criada")

# ============================================================================
# FASE 6: GOVERNANÇA & QUALIDADE
# ============================================================================

print("\n" + "=" * 60)
print("FASE 6: GOVERNANÇA & QUALIDADE")
print("=" * 60)

def run_data_quality_checks():
    """Executa testes de qualidade de dados"""
    
    print("\n--- Data Quality Checks ---")
    
    orders = spark.table('silver_orders')
    
    # Test 1: Null values
    print("\n✓ Verificando valores nulos...")
    null_check = orders.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(f'{c}_nulls') 
        for c in orders.columns
    ]).collect()[0].asDict()
    
    null_count = sum(v for v in null_check.values() if v and v > 0)
    if null_count == 0:
        print("  ✅ Nenhum valor nulo encontrado")
    else:
        print(f"  ⚠️ {null_count} valores nulos encontrados")
    
    # Test 2: Duplicates
    print("\n✓ Verificando duplicatas...")
    duplicates = orders.groupBy(orders.columns).count().filter('count > 1').count()
    if duplicates == 0:
        print("  ✅ Nenhuma duplicata encontrada")
    else:
        print(f"  ⚠️ {duplicates} linhas duplicadas")
    
    # Test 3: Data ranges
    print("\n✓ Verificando ranges de valores...")
    stats = orders.agg(
        F.min('total_value').alias('min_value'),
        F.max('total_value').alias('max_value'),
        F.avg('total_value').alias('avg_value')
    ).collect()[0].asDict()
    
    print(f"  Valor mínimo: R$ {stats['min_value']}")
    print(f"  Valor máximo: R$ {stats['max_value']}")
    print(f"  Valor médio: R$ {stats['avg_value']:.2f}")
    
    # Test 4: Record counts
    print("\n✓ Contagem de registros...")
    print(f"  Raw orders: {spark.table('raw_orders').count()}")
    print(f"  Silver orders: {spark.table('silver_orders').count()}")
    print(f"  Silver enriched: {spark.table('silver_orders_enriched').count()}")
    
    # Test 5: Data freshness
    print("\n✓ Verificando atualização de dados...")
    latest_date = orders.agg(F.max('order_date')).collect()[0][0]
    days_old = (datetime.now().date() - latest_date).days
    print(f"  Dados mais recentes: {latest_date}")
    print(f"  Dias desde última atualização: {days_old}")
    
    print("\n✅ Todos os testes concluídos!")

run_data_quality_checks()

# ============================================================================
# FASE 7: MONITORAMENTO & RESUMO
# ============================================================================

print("\n" + "=" * 60)
print("RESUMO DO PROJETO - PRONTO PARA POWER BI")
print("=" * 60)

summary = spark.sql("""
SELECT 
    'dim_date' as layer_table, COUNT(*) as row_count,
    'Gold' as layer
FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*), 'Gold' FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*), 'Gold' FROM dim_product
UNION ALL
SELECT 'fact_sales', COUNT(*), 'Gold' FROM fact_sales
UNION ALL
SELECT 'silver_orders', COUNT(*), 'Silver' FROM silver_orders
UNION ALL
SELECT 'silver_customer_metrics', COUNT(*), 'Silver' FROM silver_customer_metrics
UNION ALL
SELECT 'raw_orders', COUNT(*), 'Raw' FROM raw_orders
UNION ALL
SELECT 'raw_customers', COUNT(*), 'Raw' FROM raw_customers
UNION ALL
SELECT 'raw_products', COUNT(*), 'Raw' FROM raw_products
ORDER BY layer DESC, row_count DESC
""")

print("\n📊 Estrutura Final do Lakehouse:")
summary.display()

print("\n" + "=" * 60)
print("🎯 PRÓXIMOS PASSOS:")
print("=" * 60)
print("""
1. ✅ Abra Power BI Desktop
2. ✅ Get Data → Fabric Lakehouse
3. ✅ Selecione seu Lakehouse
4. ✅ Importe tabelas: dim_date, dim_customer, dim_product, fact_sales
5. ✅ Crie relacionamentos:
      - fact_sales.date_id ← dim_date.date_id
      - fact_sales.customer_id ← dim_customer.customer_id
      - fact_sales.product_id ← dim_product.product_id
6. ✅ Crie medidas DAX (exemplos abaixo)
7. ✅ Construa dashboards!

📋 Medidas DAX recomendadas:

Total Revenue = SUM(fact_sales[line_amount])
Total Orders = DISTINCTCOUNT(fact_sales[order_id])
Avg Order Value = [Total Revenue] / [Total Orders]
Completed Orders = CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[is_completed] = 1)
Completion Rate = DIVIDE([Completed Orders], [Total Orders]) * 100
Total Customers = DISTINCTCOUNT(fact_sales[customer_id])
""")

print("\n✅ PROJETO COMPLETO! Agora crie seus dashboards no Power BI.")
