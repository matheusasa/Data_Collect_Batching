# =============================================================================
# Universidade Mackenzie - MBA Engenharia de Dados
# Disciplina: Data Collect and Storage
# Prof. Filipe Quintieri Lima
#
# Aluno: Matheus Alves da Silva
# RA: 10752559
#
# Caso de Uso 1 - Fluxo 5: Sistema ERP Legado Opaco
# Estratégia: Full Load (Carga Total) Diária
# Formato de Saída: Parquet
# Destino: MinIO (camada Bronze)
# =============================================================================

import io
import json
import os
from datetime import date, datetime

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

METADATA_PREFIX    = "bronze/erp_legado/metadata/"

DATA_INGESTAO      = date.today().strftime("%Y-%m-%d")
TIMESTAMP_INGESTAO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def get_pg_connection():
    """Retorna uma conexão com o PostgreSQL."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )


def get_minio_client():
    """Retorna um client S3 apontando para o MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )


def upload_metadata(s3_client, registro: dict):
    key = f"{METADATA_PREFIX}{TIMESTAMP_INGESTAO}.json"
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(registro).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[OK] Metadata salvo: s3://{MINIO_BUCKET}/{key}")


def upload_df_as_parquet(s3_client, df: pd.DataFrame, s3_key: str):
    """
    Serializa um DataFrame como Parquet em memória e faz upload para o MinIO.
    O formato Parquet oferece compressão nativa e preserva a tipagem colunar,
    reduzindo o custo de armazenamento das cópias completas diárias.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    print(f"[OK] Upload realizado: s3://{MINIO_BUCKET}/{s3_key}")


# =============================================================================
# EXTRAÇÃO - TABELA: fornecedores_legado
# Full Load: SELECT * sem filtros de data
# =============================================================================

def extrair_fornecedores_full(conn) -> pd.DataFrame:
    """
    Realiza a carga completa (Full Load) da tabela fornecedores_legado.

    Justificativa: O sistema ERP legado não registra datas de atualização
    e realiza exclusões físicas sem logs de auditoria. Por isso, não é possível
    identificar quais registros foram alterados ou removidos desde a última
    execução. A estratégia correta é copiar o snapshot completo diariamente,
    particionado por data_ingestao, para que comparações futuras (diff entre
    snapshots) possam detectar as mudanças na camada Silver.
    """
    query = "SELECT * FROM fornecedores_legado"
    print("[INFO] Executando Full Load de 'fornecedores_legado'...")
    df = pd.read_sql(query, conn)
    print(f"[INFO] {len(df)} registro(s) extraído(s).")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    s3 = get_minio_client()

    print("=" * 60)
    print("FLUXO 5 - ERP Legado Opaco (Full Load Diário)")
    print(f"Data de ingestão (snapshot): {DATA_INGESTAO}")
    print("=" * 60)

    conn = None
    try:
        conn = get_pg_connection()
        df_fornecedores = extrair_fornecedores_full(conn)

        if df_fornecedores.empty:
            print("[WARN] A tabela fornecedores_legado está vazia. Arquivo não gerado.")
        else:
            s3_key = (
                f"bronze/erp_legado/fornecedores_snapshot/"
                f"data_ingestao={DATA_INGESTAO}/fornecedores_full.parquet"
            )
            upload_df_as_parquet(s3, df_fornecedores, s3_key)
            print(f"[INFO] Snapshot disponível em: s3://{MINIO_BUCKET}/{s3_key}")

        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "success"})
        print("FLUXO 5 concluído com sucesso.")

    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {e}")
        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "error", "mensagem": str(e)})

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
