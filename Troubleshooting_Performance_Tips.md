# Troubleshooting, Performance Tips e FAQ

---

## 🔴 PROBLEMAS COMUNS E SOLUÇÕES

### Problema 1: "Table not found" ou "Schema not found"

**Erro:**
```
pyspark.sql.utils.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND]
```

**Soluções:**
1. Verificar se o Lakehouse está selecionado como padrão
2. Confirmar que a tabela foi salva com `saveAsTable()` e não apenas com `write.parquet()`
3. No Fabric, ir a Lakehouse → Refresh para atualizar list of tables

```python
# ❌ ERRADO - não cria tabela gerenciada
df.write.parquet('/lakehouse/default/my_table')

# ✅ CORRETO - cria tabela gerenciada
df.write.mode('overwrite').option('mode', 'FAILFAST').saveAsTable('my_table')

# Verificar tabelas disponíveis
spark.sql("SHOW TABLES").display()
```

---

### Problema 2: Dados com NULL aparecem como 0 no Power BI

**Causa:** Power BI interpreta NULL como 0 em medidas agregadas

**Solução:**

No SQL, usar `COALESCE()` ou `CAST AS STRING`:
```sql
-- ❌ Ruim
SELECT 
    customer_state,
    SUM(revenue)
FROM fact_sales
GROUP BY customer_state
-- Resulta em linha 'NULL' com revenue = 0

-- ✅ Bom
SELECT 
    COALESCE(customer_state, 'Unknown') as customer_state,
    SUM(revenue)
FROM fact_sales
GROUP BY customer_state
```

No DAX:
```dax
-- Filtrar NULLs em medidas críticas
Total Revenue = 
CALCULATE(
    SUM(fact_sales[line_amount]),
    NOT(ISBLANK(fact_sales[customer_id]))
)
```

---

### Problema 3: Query muito lenta (timeout após 30 segundos)

**Causa:** Falta de índices, joins ineficientes, ou dados muito grandes

**Diagnóstico:**

```python
# Ver tempo de execução
import time
start = time.time()
result = spark.sql("SELECT * FROM fact_sales LIMIT 1000000").collect()
end = time.time()
print(f"Tempo: {end - start} segundos")

# Ver plano de execução
spark.sql("EXPLAIN ANALYZE SELECT * FROM fact_sales WHERE customer_id = 'CUST-1001'").display()
```

**Soluções:**

1. **Adicionar índices:**
```sql
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
```

2. **Particionar dados grandes:**
```python
df.write \
    .mode('overwrite') \
    .partitionBy('year_order', 'month_order') \
    .saveAsTable('fact_sales_partitioned')
```

3. **Usar agregações pré-calculadas:**
```sql
-- Em vez de sempre agregar fact_sales (milhões de linhas)
-- Criar uma tabela pré-agregada
CREATE TABLE daily_revenue_summary AS
SELECT
    date_id,
    SUM(line_amount) as daily_revenue,
    COUNT(DISTINCT order_id) as daily_orders,
    COUNT(DISTINCT customer_id) as daily_customers
FROM fact_sales
GROUP BY date_id
```

4. **Otimizar joins:**
```sql
-- ❌ Ruim - Join sem indices
SELECT o.*, c.customer_name
FROM fact_sales o
JOIN dim_customer c ON o.customer_id = c.customer_id
WHERE o.date_id BETWEEN 20240101 AND 20240131

-- ✅ Bom - usar broadcast para tabelas pequenas
SELECT /*+ BROADCAST(c) */ o.*, c.customer_name
FROM fact_sales o
JOIN dim_customer c ON o.customer_id = c.customer_id
WHERE o.date_id BETWEEN 20240101 AND 20240131
```

---

### Problema 4: Relacionamentos quebrados no Power BI

**Sintomas:**
- Filtros não funcionam
- Medidas retornam BLANK()
- Avisos de "relationship ambiguity"

**Solução:**

1. **Verificar relacionamentos:**
```
Model View → 
Selecionar relacionamento → 
Verificar:
- Cardinalidade (1:many)
- Cross filter direction (Single)
- Coluna de referência existe em ambas tabelas
```

2. **Criar relacionamento manualmente se necessário:**
```dax
-- No Power BI DAX, usar LOOKUPVALUE se relacionamento falhar
Customer Name = 
LOOKUPVALUE(
    dim_customer[customer_name],
    dim_customer[customer_id],
    fact_sales[customer_id]
)
```

3. **Validar IDs no SQL:**
```sql
-- Verificar referential integrity
SELECT 'Missing customers' as issue, COUNT(*) as count
FROM fact_sales
WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)
UNION ALL
SELECT 'Missing products', COUNT(*)
FROM fact_sales
WHERE product_id NOT IN (SELECT product_id FROM dim_product)
UNION ALL
SELECT 'Missing dates', COUNT(*)
FROM fact_sales
WHERE date_id NOT IN (SELECT date_id FROM dim_date)
```

---

### Problema 5: Medidas DAX retornam valores errados

**Exemplo: Revenue varia sem mudança de filtro**

**Solução:**

Verificar contexto de filtro e usar `CALCULATE()` corretamente:

```dax
-- ❌ Errado - contexto implícito
Total Revenue = SUM(fact_sales[line_amount])

-- ✅ Correto - contexto explícito
Total Revenue = 
CALCULATE(
    SUM(fact_sales[line_amount]),
    FILTER(ALL(fact_sales), fact_sales[line_amount] > 0)
)

-- Para comparações período anterior
Previous Month Revenue = 
CALCULATE(
    [Total Revenue],
    DATEADD(dim_date[date_key], -1, MONTH),
    ALL(dim_customer),  -- Remove filtro de cliente se houver
    ALL(dim_product)    -- Remove filtro de produto se houver
)
```

---

## ⚡ PERFORMANCE TIPS E BOAS PRÁTICAS

### 1. Otimização de Query SQL

**Antes:**
```sql
-- Lento: Agregação complexa em toda tabela
SELECT 
    customer_id,
    SUM(line_amount) as revenue
FROM fact_sales
WHERE DATE_FORMAT(order_date, 'yyyy-MM') = '2024-01'
GROUP BY customer_id
```

**Depois:**
```sql
-- Rápido: Usar partição e índice
SELECT 
    customer_id,
    SUM(line_amount) as revenue
FROM fact_sales
WHERE year_order = 2024 AND month_order = 1  -- Partition pruning
GROUP BY customer_id
```

---

### 2. Estratégia de Tamanho de Tabela

**Raw layer** (sempre completo):
- Manter todos os dados históricos
- Sem limpeza ou transformação

**Silver layer** (filtrado):
- Remover ~10-30% de dados (inválidos, duplicatas)
- Aplicar business logic básica

**Gold layer** (agregado):
- ~1-5% do tamanho original
- Pré-agregações para dashboards
- Índices otimizados

**Exemplo:**
```
raw_orders: 10 milhões de linhas
  ↓ (remove 5% inválidos)
silver_orders: 9.5 milhões
  ↓ (agregação por mês)
daily_revenue: 1.095 registros (365 dias × 3 anos)
```

---

### 3. Caching e Materialização

```python
# Dados usados frequentemente em dashboards
# Salvar como tabela materalizada

# ❌ Lento - recalcula sempre
spark.sql("""
SELECT date_id, SUM(line_amount) as daily_revenue
FROM fact_sales
GROUP BY date_id
""").display()

# ✅ Rápido - tabela pré-calculada
spark.sql("""
CREATE TABLE daily_revenue_cache AS
SELECT date_id, SUM(line_amount) as daily_revenue
FROM fact_sales
GROUP BY date_id
""")

# Power BI aponta para daily_revenue_cache, não para fact_sales
```

---

### 4. Otimização de Joins

```sql
-- ❌ Evitar: Cartesian product implícito
SELECT *
FROM fact_sales f
CROSS JOIN dim_customer c

-- ❌ Evitar: Join em coluna não-indexada
SELECT *
FROM fact_sales f
JOIN dim_customer c ON f.customer_name = c.customer_name

-- ✅ Melhor: Join em coluna primária indexada
SELECT *
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id

-- ✅ Otimal: Especificar tipo de join explicitamente
SELECT /*+ BROADCAST(c) */ f.*, c.customer_segment
FROM fact_sales f
INNER JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.date_id >= 20240101
```

---

### 5. Incrementalizar Cargas (Upsert)

**Problema:** Recarregar 10 milhões de linhas diariamente é lento

**Solução:** Carregar apenas dados novos/atualizados

```python
# Estratégia Append-Only (mais simples)
new_data = spark.read.csv('/new_orders.csv', header=True)
new_data.write.mode('append').saveAsTable('raw_orders')

# Estratégia Merge (mais eficiente)
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "raw_orders")

delta_table.alias("old").merge(
    new_data.alias("new"),
    "old.order_id = new.order_id"
) \
.whenMatched().updateAll() \
.whenNotMatched().insertAll() \
.execute()
```

---

### 6. Compactação de Arquivos Parquet

```python
# Após muitos writes/deletes, compactar
spark.sql("""
OPTIMIZE raw_orders 
ZORDER BY (customer_id, order_date)
""")

# Resultado: queries mais rápidas em 10-50%
```

---

## 📚 FAQ - PERGUNTAS FREQUENTES

### Q1: Quanto tempo leva para criar um projeto completo?

**R:** Depende da complexidade:
- **Simples** (100K linhas, 3 tabelas): 2-4 horas
- **Médio** (1M linhas, 10 tabelas): 1-2 dias
- **Complexo** (100M linhas, modelagem avançada): 1-2 semanas

O código deste guia reduz isso em 50%.

---

### Q2: Qual é o tamanho máximo de dados suportado pelo Fabric?

**R:** Lakehouse suporta até **PBs** (Petabytes). Prático:
- Até 1TB: Sem otimizações especiais
- 1-10TB: Adicionar índices e partições
- 10TB+: Considerar federação de dados ou sharding

---

### Q3: Posso usar Python ou só Spark SQL?

**R:** Ambos! Recomendação:
- **Spark SQL**: Transformações simples, joins, agregações
- **PySpark**: Lógica complexa, machine learning, custom transformations
- **Python puro**: Muito lento, evite em dados grandes

```python
# ✅ Bom: SQL + Python híbrido
result = spark.sql("SELECT * FROM raw_orders WHERE status = 'delivered'")
df = result.toPandas()  # Converter para pandas (máx 1-2M linhas)
# Processar em pandas (análise rápida)
```

---

### Q4: Como fazer backup dos dados?

**R:** No Fabric, dados são automaticamente replicados. Mas para segurança:

```python
# Exportar tabela como Parquet
spark.table('fact_sales').write.parquet('/backup/fact_sales_2025-01-15')

# Exportar como Delta (mais eficiente)
spark.table('fact_sales').write.format('delta').save('/backup/fact_sales_delta')

# Exportar como CSV (compatibilidade)
spark.table('fact_sales').write.csv('/backup/fact_sales_csv', header=True)
```

---

### Q5: Como implementar versionamento?

**R:** Usar Delta Lake tables (versionamento nativo):

```python
# Ler versão anterior
spark.read.format('delta').option('versionAsOf', 5).table('fact_sales').display()

# Ver histórico
spark.sql("DESCRIBE HISTORY fact_sales").display()

# Rollback para versão anterior
spark.sql("RESTORE TABLE fact_sales TO VERSION AS OF 5")
```

---

### Q6: Posso conectar múltiplos Fabric workspaces?

**R:** Sim, usando Workspace Identities e Fabric shortcuts:

```
Lakehouse → Create Shortcut → Selecionar outro Lakehouse
```

Depois referenciar:
```sql
SELECT * FROM `<workspace>`.`<lakehouse>`.<table>
```

---

### Q7: Como garantir segurança de dados (GDPR, LGPD)?

**R:** Implementar RLS (Row-Level Security) no Power BI:

1. **Adicionar coluna de proprietário:** `data_owner`
```sql
ALTER TABLE fact_sales ADD COLUMN data_owner STRING;
UPDATE fact_sales SET data_owner = 'TEAM_A' WHERE region = 'SP';
```

2. **No Power BI:** Model → Manage roles
```dax
[data_owner] = USERNAME()
```

3. **Para LGPD:** Implementar "right to be forgotten"
```sql
DELETE FROM fact_sales WHERE customer_id = 'CUST-1001';
DELETE FROM dim_customer WHERE customer_id = 'CUST-1001';
```

---

### Q8: Como monitorar custo do Fabric?

**R:** Acompanhar na Azure portal:

```
Azure Portal → Fabric → 
Workspaces → Seu workspace → 
Billing
```

**Dicas de economia:**
- Pausar capacidade fora do horário de negócio
- Usar streaming analytics apenas para dados reais-time
- Agregar dados antes de publicar em Power BI
- Remover dashboards/reports não utilizados

---

### Q9: Qual modelo escolher: Star Schema ou Snowflake?

**R:**

| Aspecto | Star Schema | Snowflake |
|--------|-----------|-----------|
| Complexidade | Simples | Moderada |
| Queries | Mais rápido | Um pouco mais lento |
| Storage | Mais espaço | Menos espaço |
| Manutenção | Fácil | Mais cuidadoso |

**Recomendação para começar:** **Star Schema**

```
Star Schema:
┌─ DIM_CUSTOMER ─┐
│                │
├─ FACT_SALES ─┼─ DIM_PRODUCT
│                │
└─ DIM_DATE ────┘

Snowflake (mais normalizado):
DIM_CATEGORY ─┐
              │
         DIM_PRODUCT ─┐
                      ├─ FACT_SALES
         DIM_CUSTOMER ├─ DIM_DATE
                      │
         DIM_REGION ──┘
```

---

### Q10: Posso usar Fabric para análise em tempo real?

**R:** Sim, mas depende do volume:

**Até 100K eventos/hora:** ✅ Fabric Streaming + SQL queries
**100K - 1M eventos/hora:** ✅ Com otimizações (clustering, partitioning)
**Acima de 1M eventos/hora:** ⚠️ Considerar Apache Kafka + Spark Streaming

```python
# Streaming setup
kafka_servers = "kafka:9092"

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers) \
  .option("subscribe", "orders-topic") \
  .load()

df.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .mode("append") \
  .toTable("streaming_orders")
```

---

## 🎓 RECURSOS PARA APRENDER MAIS

### Documentação Oficial
- [Microsoft Fabric Docs](https://learn.microsoft.com/en-us/fabric/)
- [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
- [DAX Function Reference](https://dax.guide/)

### Cursos Recomendados
- [Fabric by Microsoft Learn](https://learn.microsoft.com/en-us/training/paths/get-started-fabric/)
- [Spark by Databricks](https://academy.databricks.com/)
- [Power BI by Microsoft Learn](https://learn.microsoft.com/en-us/power-bi/)

### Comunidades
- [Reddit r/PowerBI](https://reddit.com/r/PowerBI)
- [Fabric Community Forum](https://community.fabric.microsoft.com/)
- [Stack Overflow - fabric, spark-sql, power-bi](https://stackoverflow.com/)

### Ferramentas Úteis
- **Fabric Explorer**: Visualizar estrutura do Lakehouse
- **Spark UI**: Monitorar jobs em execução
- **Power BI Performance Analyzer**: Otimizar dashboards
- **Query Insight**: Analisar queries lentas

---

## 🚀 CHECKLIST FINAL - ANTES DE COLOCAR EM PRODUÇÃO

- [ ] Todos os testes de qualidade passando
- [ ] Documentação atualizada
- [ ] Backups configurados
- [ ] Alerts configurados
- [ ] Segurança/Permissões validadas
- [ ] Performance testada (queries < 10 segundos)
- [ ] Dashboards aprovados por stakeholders
- [ ] Runbooks de troubleshooting criados
- [ ] Plano de manutenção definido
- [ ] Treinamento de usuários finalizado

---

**Boa sorte com seu projeto Analytics Engineer! 🚀**

