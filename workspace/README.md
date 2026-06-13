# lab_datacollect

Laboratório prático de **Engenharia de Dados** desenvolvido como trabalho de pós-graduação. O projeto demonstra padrões de ingestão de dados de um banco transacional PostgreSQL para um Data Lake no MinIO (Object Storage compatível com S3), utilizando dois paradigmas distintos: **Apache Spark** e **Python Nativo**.

---

## Visão Geral da Arquitetura

```
PostgreSQL (Origem Transacional)
         │
         ├──► Apache Spark (JDBC) ──► MinIO / S3 (Camada RAW)
         │         └── Formatos: Parquet, Avro, JSONL
         │
         └──► Python Nativo (psycopg2 + cursor) ──► MinIO / S3 (Camada RAW)
                   └── Formatos: CSV, Parquet, JSONL, Avro
```

A infraestrutura local é orquestrada via containers Docker (PostgreSQL + MinIO + Spark).

---

## Estrutura do Repositório

```
workspace/
├── ddl_cenarios_data_collect.sql   # DDL com os 5 cenários de tabelas de origem
├── gerardo_dados_origem.py          # Script de geração de massa de dados (Faker)
│
├── spark_jobs/
│   ├── README.md                    # Guia: Spark vs. Python Nativo
│   ├── spark_job_parquet.ipynb      # Ingestão com Spark → Parquet (Snappy)
│   ├── spark_job_avro.ipynb         # Ingestão com Spark → Avro
│   └── spark_job_jsonl.ipynb        # Ingestão com Spark → JSON Lines
│
├── python_native/
│   ├── README.md                    # Guia: Cursores Nativos vs. Pandas
│   ├── python_native_csv.ipynb      # Ingestão nativa → CSV (StringIO buffer)
│   ├── python_native_parquet.ipynb  # Ingestão nativa → Parquet (PyArrow)
│   ├── python_native_jsonl.ipynb    # Ingestão nativa → JSON Lines
│   └── python_native_avro.ipynb     # Ingestão nativa → Avro (fastavro)
│
└── teste_service/
    ├── README.md                    # Guia de validação de infraestrutura
    ├── teste-postgres.ipynb         # Teste de conectividade ao PostgreSQL
    ├── teste_minio.ipynb            # Teste de acesso ao MinIO (S3)
    └── teste-spark.ipynb            # Teste de inicialização da SparkSession
```

---

## Os 5 Cenários de Extração

O arquivo [`ddl_cenarios_data_collect.sql`](ddl_cenarios_data_collect.sql) cria as seguintes tabelas, cada uma representando um padrão real de extração incremental:

| # | Tabela | Domínio | Estratégia de Extração |
|---|--------|---------|------------------------|
| 1 | `pedidos` / `itens_pedido` | E-commerce | Incremental por `data_ultima_atualizacao` (Trigger de UPDATE) |
| 2 | `eventos_web` | Telemetria / Web | Incremental por `timestamp_evento` (Insert-Only, JSONB) |
| 3 | `transacoes_financeiras` | Financeiro / Ledger | Incremental por `data_transacao` (Insert-Only, alta precisão) |
| 4 | `produtos_catalogo` | Catálogo | Incremental por `data_ultima_atualizacao` + Soft Delete (`fl_excluido`) |
| 5 | `fornecedores_legado` | Legado / Opaque | Full Load diário (sem controle de atualização) |

---

## Geração de Massa de Dados

O script [`gerardo_dados_origem.py`](gerardo_dados_origem.py) utiliza a biblioteca **Faker** para popular as tabelas com dados sintéticos em escala configurável:

| Tabela | Volume Padrão |
|--------|---------------|
| Pedidos | 10.000 (≈ 30.000 itens) |
| Eventos Web | 50.000 |
| Ledger Financeiro | 20.000 |
| Produtos Catálogo | 2.000 |
| Fornecedores Legado | 5.000 |

O script também simula comportamentos reais: **reajuste de preço em 5%** dos produtos (aciona o trigger de UPDATE) e **soft delete em 2%** (marcação de itens fora de linha).

Para executar, configure as variáveis de conexão no início do arquivo e rode:

```bash
python gerardo_dados_origem.py
```

---

## Pré-requisitos e Configuração

### 1. Validar a Infraestrutura

Antes de executar qualquer pipeline, rode os notebooks de teste na ordem:

1. [`teste_service/teste-postgres.ipynb`](teste_service/teste-postgres.ipynb) — valida conexão TCP e autenticação no PostgreSQL
2. [`teste_service/teste_minio.ipynb`](teste_service/teste_minio.ipynb) — valida acesso HTTP e credenciais do MinIO
3. [`teste_service/teste-spark.ipynb`](teste_service/teste-spark.ipynb) — valida inicialização da JVM e SparkSession

### 2. Criar o Schema e Popular os Dados

```bash
# 1. Aplicar o DDL no PostgreSQL
psql -h localhost -p 5442 -U postgres -f ddl_cenarios_data_collect.sql

# 2. Gerar a massa de dados
python gerardo_dados_origem.py
```

### 3. Executar os Pipelines

Abra os notebooks de [`spark_jobs/`](spark_jobs/) ou [`python_native/`](python_native/) no Jupyter e execute célula por célula com `Shift + Enter`.

---

## Formatos de Arquivo Cobertos

| Formato | Orientação | Tipo | Melhor Ferramenta | Caso de Uso |
|---------|-----------|------|-------------------|-------------|
| **CSV** | Linha | Texto | Python Nativo | Exportação para analistas / Excel / BI |
| **JSON Lines** | Linha | Texto Semi-estruturado | Python Nativo ou Spark | Ingestão bruta de eventos |
| **Avro** | Linha | Binário (Schema) | Python Nativo ou Spark | Streaming / Kafka / integração com contratos |
| **Parquet** | Coluna | Binário | Apache Spark | Camadas Silver/Gold de Data Lakehouses |

---

## Tecnologias Utilizadas

- **Python 3.x** — `psycopg2`, `boto3`, `faker`, `fastavro`, `pyarrow`
- **Apache Spark 3.5.0** — via PySpark + conectores JDBC (PostgreSQL) e S3A (MinIO)
- **PostgreSQL** — banco de dados transacional de origem (porta `5442` local)
- **MinIO** — Object Storage S3-compatível (porta `9000` local), bucket `raw`
- **Jupyter Notebooks** — ambiente interativo de execução e documentação

---

## Referências de Arquitetura

- [Spark vs. Python Nativo — Quando usar cada um](/workspace/spark_jobs/README.md)
- [Cursores Nativos vs. Pandas em Pipelines de Ingestão](/workspace/python_native/README.md)
- [Validação de Infraestrutura Local](/workspace/teste_service/README.md)
