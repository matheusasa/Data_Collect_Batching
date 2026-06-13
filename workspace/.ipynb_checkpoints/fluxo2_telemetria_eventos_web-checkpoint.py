# =============================================================================
# Universidade Mackenzie - MBA Engenharia de Dados
# Disciplina: Data Collect and Storage
# Prof. Filipe Quintieri Lima
#
# Aluno: Matheus Alves da Silva
# RA: 10752559
#
# Caso de Uso 1 - Fluxo 2: Telemetria e Eventos Web (Schema Drift)
# Estratégia: Incremental (Insert-Only)
# Formato de Saída: JSONL (JSON Lines)
# Destino: MinIO (camada Bronze)
# =============================================================================

import json
import os
from datetime import date, datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

METADATA_PREFIX = "bronze/telemetria/metadata/"
WATERMARK_DEFAULT = os.getenv("ULTIMA_EXECUCAO", "2000-01-01 00:00:00")

DATA_INGESTAO = date.today().strftime("%Y-%m-%d")
TIMESTAMP_INGESTAO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

OUTPUT_PATH = (
    f"s3a://{MINIO_BUCKET}/bronze/telemetria/eventos_web/"
    f"data_ingestao={DATA_INGESTAO}/eventos.jsonl"
)


# =============================================================================
# METADATA / WATERMARK
# =============================================================================

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ler_watermark(s3_client) -> str:
    """Lista os arquivos de metadata no MinIO e retorna o timestamp
    do registro mais recente com status 'success'."""
    try:
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=METADATA_PREFIX)
        objetos = response.get("Contents", [])
        if not objetos:
            print("[INFO] Nenhum metadata encontrado. Usando valor padrão.")
            return WATERMARK_DEFAULT

        objetos_ordenados = sorted(objetos, key=lambda o: o["LastModified"], reverse=True)

        for obj in objetos_ordenados:
            body = s3_client.get_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
            data = json.loads(body["Body"].read().decode("utf-8"))
            registros = data if isinstance(data, list) else [data]
            for registro in registros:
                if registro.get("status") == "success":
                    ts = registro["timestamp"]
                    print(f"[INFO] Watermark encontrado em '{obj['Key']}': {ts}")
                    return ts

        print("[WARN] Nenhum metadata com status 'success'. Usando valor padrão.")
    except ClientError as e:
        print(f"[WARN] Erro ao listar metadata: {e}. Usando valor padrão.")
    return WATERMARK_DEFAULT


def upload_metadata(s3_client, registro: dict):
    """Sobe um registro de metadata como JSON no MinIO."""
    key = f"{METADATA_PREFIX}{TIMESTAMP_INGESTAO}.json"
    payload = json.dumps(registro).encode("utf-8")
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )
    print(f"[OK] Metadata salvo: s3://{MINIO_BUCKET}/{key}")


# =============================================================================
# INICIALIZAÇÃO DO SPARK
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Cria e configura a SparkSession com suporte ao MinIO via S3A
    e ao driver JDBC do PostgreSQL.

    spark.jars.packages baixa os JARs do Maven na primeira execução e
    armazena o cache em workspace/.ivy2 (volume persistente no Docker).
    """
    spark = (
        SparkSession.builder
        .appName("Fluxo2_Telemetria_EventosWeb")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# EXTRAÇÃO - TABELA: eventos_web
# Schema Drift: extraímos o payload como texto puro (STRING) para não tentar
# inferir as colunas JSONB, que podem variar a cada evento.
# =============================================================================

def extrair_eventos(spark: SparkSession, ultima_execucao: str):
    """
    Lê eventos_web do PostgreSQL via JDBC.
    A coluna 'payload' (JSONB) é mantida como texto bruto — o Spark não
    tenta inferir seu esquema nesta camada, respeitando o princípio de
    cópia fiel da Bronze e evitando falhas por schema drift.
    """
    query = (
        f"(SELECT id, data_criacao, CAST(payload AS TEXT) AS payload "
        f"FROM eventos_web WHERE data_criacao > '{ultima_execucao}') AS t"
    )

    print(f"[INFO] Extraindo eventos_web com data_criacao > {ultima_execucao}")

    df = spark.read.jdbc(
        url=PG_JDBC_URL,
        table=query,
        properties={
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        },
    )

    count = df.count()
    print(f"[INFO] {count} evento(s) extraído(s) de 'eventos_web'.")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    s3 = get_minio_client()
    ultima_execucao = ler_watermark(s3)

    print("=" * 60)
    print("FLUXO 2 - Telemetria / Eventos Web (Insert-Only / Schema Drift)")
    print(f"Data de ingestão  : {DATA_INGESTAO}")
    print(f"Watermark utilizado: {ultima_execucao}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        df_eventos = extrair_eventos(spark, ultima_execucao)

        if df_eventos.rdd.isEmpty():
            print("[INFO] Nenhum evento novo. Nenhum arquivo gerado.")
        else:
            (
                df_eventos
                .coalesce(1)
                .write
                .mode("overwrite")
                .json(OUTPUT_PATH)
            )
            print(f"[OK] Arquivo JSONL gravado em: {OUTPUT_PATH}")

        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "success"})
        print("FLUXO 2 concluído com sucesso.")

    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {e}")
        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "error", "mensagem": str(e)})

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
