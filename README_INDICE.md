# 📚 Analytics Engineer no Fabric - Guia Completo

## Bem-vindo, 🎯

# Atençao: As formulas e scripts usadas nesse projeto foram executadas em janeiro de 2026 em py and BI mais possa ser que essas informações e mmd precise ser otimizadas então dependendo o programa que for usar verifica com AI se precisa de alguma atualização.

Este é um **guia passo a passo completo** para construir um projeto de **Analytics Engineer profissional** no Microsoft Fabric, do zero até dashboards em Power BI.

Você tem aqui **3 arquivos principais** + este sumário. Tudo pronto para executar!

---

## 📖 ARQUIVOS DESTE PROJETO

### 1. **Analytics_Engineer_Fabric_Guia_Completo.md** (Teórico)


**Conteúdo:**

- ✅ Fase 1: Ingestão de Dados (APIs, arquivos, databases)
- ✅ Fase 2: Armazenamento (Lakehouse, Medallion Architecture)
- ✅ Fase 3: Transformação (SQL, PySpark, Dataflows)
- ✅ Fase 4: Modelagem (Star Schema, Dimensões, Fatos)
- ✅ Fase 5: Visualização (Power BI Dashboards)
- ✅ Fase 6: Governança (Data Quality, Lineage)
- ✅ Fase 7: Monitoramento (Alertas, Agendamentos)

**Quando usar:** Leia primeiro para entender o fluxo completo

---

### 2. **Fabric_Analytics_Engine_Codigo_Completo.py** (Prático)

**Objetivo:** Código Python **pronto para executar** em um Notebook do Fabric

**Conteúdo:**

```
python
# Copia e cola no Fabric Notebook e executa tudo de uma vez!

# ✅ Cria dados de exemplo (sem precisar do Kaggle)
# ✅ Fase 1: Importa CSV
# ✅ Fase 2: Cria estrutura Lakehouse
# ✅ Fase 3: Transforma dados (Silver)
# ✅ Fase 4: Cria modelo Star Schema (Gold)
# ✅ Fase 5: Prepara para Power BI
# ✅ Fase 6: Executa testes de qualidade
# ✅ Fase 7: Mostra resumo final
```

**Quando usar:** 
- Para aprender fazendo (hands-on)
- Para prototipar rapidamente
- Cada seção pode ser executada independentemente

**Como usar:**

```
1. Abra seu Fabric workspace
2. Crie um Notebook
3. Copie todo o código deste arquivo
4. Execute célula por célula
5. Veja as tabelas serem criadas no seu Lakehouse
```

---

### 3. **Guia_Pratico_SQL_DAX_Dashboards.md** (Receitas)
**Objetivo:** 50+ queries SQL + 30+ medidas DAX prontas para copiar

**Conteúdo:**

#### Parte 1: SQL Pronto para Executar
```sql
-- 8 queries SQL completas para criar tabelas:

1. silver_orders          (Pedidos limpos)
2. silver_customer_metrics (Análise RFM)
3. silver_order_items     (Itens enriquecidos)
4. silver_seller_metrics  (Performance de vendedores)
5. dim_date               (Calendário 3 anos)
6. dim_customer           (Clientes segmentados)
7. dim_product            (Produtos com análise)
8. fact_sales             (Tabela de fatos Star Schema)
```

#### Parte 2: DAX para Power BI
```dax
-- 30+ medidas prontas:

Revenue / Orders / Customers
YoY Growth, MoM Growth
Customer Lifetime Value
RFM Segmentation
Churn Risk Analysis
Product Performance
Temporal Analysis (MTD, YTD, LTD)
```

#### Parte 3: Estrutura de Dashboards
```
Dashboard 1: Executive Summary    (KPIs + Trends)
Dashboard 2: Customer Analytics   (RFM + Segmentation)
Dashboard 3: Product Performance  (Sales by category)
Dashboard 4: Temporal Analysis    (Seasonality + Forecast)
```

**Quando usar:** Quando você já tem a estrutura montada e quer adicionar medidas/dashboards

---

### 4. **Troubleshooting_Performance_Tips.md** (Referência)
**Objetivo:** Resolver 9 problemas comuns + 6 tips de performance

**Conteúdo:**

🔴 **Problemas Comuns:**
1. "Table not found" - como diagnosticar e corrigir
2. NULL aparecendo como 0 - solução
3. Query muito lenta (timeout) - otimizar
4. Relacionamentos quebrados - validar
5. Medidas DAX com valores errados - debugar
6. E mais 4 problemas...

⚡ **Performance Tips:**
1. Otimização de Query SQL (before/after)
2. Estratégia de tamanho de tabela
3. Caching e materialização
4. Otimização de joins
5. Incrementalização (Upsert)
6. Compactação de arquivos


**Quando usar:** Quando algo der errado ou quiser otimizar performance

---

## 🎯 ROTEIRO RECOMENDADO


Dia 1 - Entender (2h)
└─ Ler: Analytics_Engineer_Fabric_Guia_Completo.md (Fase 1-4)
└─ Ler: Diagrama das 7 fases (acima)

Dia 1 - Executar (4h)
└─ Executar: Fabric_Analytics_Engine_Codigo_Completo.py
└─ Ver tabelas serem criadas no Lakehouse
└─ Validar dados com queries SQL simples

Dia 2 - Power BI (4h)
└─ Conectar Fabric ao Power BI Desktop
└─ Importar tabelas gold_*
└─ Criar relacionamentos
└─ Copiar medidas DAX de: Guia_Pratico_SQL_DAX_Dashboards.md
└─ Criar 4 dashboards principais

Dia 2 - Troubleshooting (1h)
└─ Ler: FAQ do Troubleshooting_Performance_Tips.md
└─ Resolver qualquer problema encontrado
```

---

```
Rápido (2h)
└─ Executar: Fabric_Analytics_Engine_Codigo_Completo.py
└─ Verificar estrutura no Lakehouse
└─ Ir direto para o Power BI

Power BI (2h)
└─ Conectar + Criar modelo
└─ Copiar DAX measures
└─ Criar dashboards
```


```
Preparação (1h)
└─ Baixar dataset: Brazilian E-Commerce (Kaggle)
└─ Upload CSV no Lakehouse
└─ Adaptar path no código Python

Executar (4-6h)
└─ Executar: Fabric_Analytics_Engine_Codigo_Completo.py
└─ Adaptar queries SQL se necessário
└─ Testar qualidade de dados

Power BI (4h)
└─ Conectar + Modelar
└─ Criar dashboards
└─ Formatação visual

Otimizar (2h)
└─ Ler: Performance_Tips do Troubleshooting
└─ Adicionar índices
└─ Testar speed
```

---

## 📊 ESTRUTURA FINAL DE ARQUIVOS

Após completar tudo, seu Lakehouse terá:

```
Lakehouse/
│
├── Raw Layer (dados brutos - imutável)
│   ├── raw_orders           (1,000 - 10M registros)
│   ├── raw_customers        (100 - 1M registros)
│   ├── raw_products         (50 - 100K registros)
│   ├── raw_order_items      (5,000 - 50M registros)
│   └── raw_reviews          (1,000 - 10M registros)
│
├── Silver Layer (transformado)
│   ├── silver_orders                 (validado, limpo)
│   ├── silver_customer_metrics       (agregado por cliente)
│   ├── silver_order_items_enriched   (com product details)
│   └── silver_seller_metrics         (performance por seller)
│
└── Gold Layer (pronto para BI - Star Schema)
    ├── dim_date              (1,095 dias)
    ├── dim_customer          (segmentação RFM)
    ├── dim_product           (análise por categoria)
    ├── dim_seller            (performance metrics)
    └── fact_sales            (tabela de fatos central)
```

**Tamanho típico:**
- Raw: 100MB - 1GB (dependendo do dataset)
- Silver: 10MB - 100MB (70% comprimido)
- Gold: 1MB - 10MB (80-90% comprimido)

---

## 💻 TECNOLOGIAS UTILIZADAS

| Componente | Tecnologia | Funcionalidade |
|-----------|-----------|---|
| **Data Lake** | Fabric Lakehouse | Armazenamento |
| **Processamento** | Apache Spark + SQL | Transformação |
| **Linguagem** | Python + Spark SQL | Código |
| **BI** | Power BI Desktop | Dashboards |
| **Versionamento** | Delta Lake | Histórico de dados |
| **Modelagem** | Star Schema | Dimensões + Fatos |

---

## 📈 MÉTRICAS QUE VOCÊ CRIARÁ

Ao final, seus dashboards terão:

**Revenue Metrics:**
- Total Revenue (YTD, MTD)
- Revenue Growth (YoY, MoM)
- Revenue by Category, Product, Region

**Customer Metrics:**
- Total Customers, New Customers
- Lifetime Value (LTV)
- Customer Lifetime Value (CLV)
- Churn Rate, At Risk Customers
- RFM Segmentation

**Product Metrics:**
- Top Products, Bottom Products
- Product Category Performance
- Average Rating by Product
- Inventory Turnover

**Operational Metrics:**
- Order Completion Rate
- Average Order Value
- Customer Acquisition Cost
- Profit Margin by Segment

---
Se ficar preso em algo:

1. **Procure em:** Troubleshooting_Performance_Tips.md
2. **Se não encontrar:**
   - Busque no Stack Overflow: `[fabric]`, `[spark-sql]`, `[power-bi]`
   - Consulte docs: Microsoft Fabric Docs (learn.microsoft.com/en-us/fabric)


**Próximo passo:** Abra o arquivo **`Analytics_Engineer_Fabric_Guia_Completo.md`** e comece pela Fase 1!

---

## 🎓 Aprendizado Esperado

Após completar este projeto, você será capaz de:

✅ Criar pipelines ETL completos  
✅ Estruturar dados em Star Schema  
✅ Escrever SQL otimizado com índices  
✅ Criar medidas DAX avançadas  
✅ Desner dashboards profissionais  
✅ Monitorar qualidade de dados  
✅ Otimizar performance de queries  
✅ Troubleshoot problemas comuns  
✅ Documentar projetos de dados  
✅ Apresentar insights para stakeholders  

---

Contato e feedback

anunciacao.datanalysis@gmail.com

---

### Índice Rápido dos Arquivos

| Arquivo | Tamanho | Tipo | Tempo |
|---------|---------|------|-------|
| Analytics_Engineer_Fabric_Guia_Completo.md | ~15KB | Teórico | 2-3h |
| Fabric_Analytics_Engine_Codigo_Completo.py | ~8KB | Código | 3-4h |
| Guia_Pratico_SQL_DAX_Dashboards.md | ~20KB | Receitas | Referência |
| Troubleshooting_Performance_Tips.md | ~12KB | FAQ | Referência |

**Total: ~4 arquivos, ~55KB, 5-8 horas de aprendizado prático**

---

*Criado em Janeiro 2025 - Atualizado para Microsoft Fabric 2025*
