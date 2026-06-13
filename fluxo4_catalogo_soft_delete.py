# =============================================================================
# Universidade Mackenzie - MBA Engenharia de Dados
# Disciplina: Data Collect and Storage
# Prof. Filipe Quintieri Lima
#
# Aluno: Matheus Alves da Silva
# RA: 10752559
#
# Caso de Uso 1 - Fluxo 4: Catálogo de Produtos (Soft Delete e Updates)
# Estratégia: Incremental (Captura de Updates)
# Formato de Saída: CSV
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

METADATA_PREFIX   = "bronze/catalogo/metadata/"
WATERMARK_DEFAULT = os.getenv("ULTIMA_EXECUCAO", "2000-01-01 00:00:00")

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


def ler_watermark(s3_client) -> str:
    """Lista os arquivos de metadata e retorna o timestamp do mais recente com status 'success'."""
    try:
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=METADATA_PREFIX)
        objetos = response.get("Contents", [])
        if not objetos:
            print("[INFO] Nenhum metadata encontrado. Usando valor padrão.")
            return WATERMARK_DEFAULT

        for obj in sorted(objetos, key=lambda o: o["LastModified"], reverse=True):
            body = s3_client.get_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
            data = json.loads(body["Body"].read().decode("utf-8"))
            for registro in (data if isinstance(data, list) else [data]):
                if registro.get("status") == "success":
                    print(f"[INFO] Watermark encontrado em '{obj['Key']}': {registro['timestamp']}")
                    return registro["timestamp"]

        print("[WARN] Nenhum metadata com status 'success'. Usando valor padrão.")
    except ClientError as e:
        print(f"[WARN] Erro ao listar metadata: {e}. Usando valor padrão.")
    return WATERMARK_DEFAULT


def upload_metadata(s3_client, registro: dict):
    key = f"{METADATA_PREFIX}{TIMESTAMP_INGESTAO}.json"
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(registro).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[OK] Metadata salvo: s3://{MINIO_BUCKET}/{key}")


def upload_df_as_csv(s3_client, df: pd.DataFrame, s3_key: str):
    """Serializa um DataFrame como CSV em memória e faz upload para o MinIO."""
    buffer = io.StringIO()
    # Delimitado por vírgula (padrão), com cabeçalho
    df.to_csv(buffer, index=False, header=True, sep=",")
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )
    print(f"[OK] Upload realizado: s3://{MINIO_BUCKET}/{s3_key}")


# =============================================================================
# EXTRAÇÃO - TABELA: produtos_catalogo
# =============================================================================

def extrair_delta_produtos(conn, ultima_execucao: str) -> pd.DataFrame:
    """
    Extrai o delta de produtos alterados ou marcados como excluídos
    desde a última execução.

    IMPORTANTE: A coluna 'fl_excluido' (soft delete) é levada integralmente
    para o MinIO, sem qualquer filtro. Produtos inativos devem ser preservados
    na Bronze para que a camada Silver possa aplicar a lógica de descarte
    adequada, mantendo o histórico de exclusões lógicas.
    """
    query = """
        SELECT *
        FROM produtos_catalogo
        WHERE data_ultima_atualizacao > %(ultima_execucao)s
    """
    print(f"[INFO] Extraindo delta de produtos_catalogo com data_ultima_atualizacao > {ultima_execucao}")
    df = pd.read_sql(query, conn, params={"ultima_execucao": ultima_execucao})
    print(f"[INFO] {len(df)} produto(s) no delta extraído.")

    # Verificação informativa: quantos registros são soft-deleted neste delta
    if "fl_excluido" in df.columns:
        n_excluidos = df["fl_excluido"].sum() if df["fl_excluido"].dtype == bool else \
                      (df["fl_excluido"] == True).sum()
        print(f"[INFO] Destes, {n_excluidos} possuem fl_excluido=True (soft delete).")

    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    s3 = get_minio_client()
    ultima_execucao = ler_watermark(s3)

    print("=" * 60)
    print("FLUXO 4 - Catálogo de Produtos (Soft Delete / Incremental)")
    print(f"Data de ingestão   : {DATA_INGESTAO}")
    print(f"Watermark utilizado: {ultima_execucao}")
    print("=" * 60)

    conn = None
    try:
        conn = get_pg_connection()
        df_delta = extrair_delta_produtos(conn, ultima_execucao)

        if df_delta.empty:
            print("[INFO] Nenhum produto alterado. Nenhum arquivo gerado.")
        else:
            s3_key = (
                f"bronze/catalogo/produtos/"
                f"data_ingestao={DATA_INGESTAO}/delta_produtos.csv"
            )
            upload_df_as_csv(s3, df_delta, s3_key)

        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "success"})
        print("FLUXO 4 concluído com sucesso.")

    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {e}")
        upload_metadata(s3, {"timestamp": TIMESTAMP_INGESTAO, "status": "error", "mensagem": str(e)})

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
