# =============================================================================
# Universidade Mackenzie - MBA Engenharia de Dados
# Disciplina: Data Collect and Storage
# Prof. Filipe Quintieri Lima
#
# Aluno: Matheus Alves da Silva
# RA: 10752559
#
# Caso de Uso 1 - Fluxo 1: Sistema Transacional Pai-Filho
# Estratégia: Incremental (High-Watermark)
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

METADATA_PREFIX = "bronze/transacional/metadata/"
WATERMARK_DEFAULT = os.getenv("ULTIMA_EXECUCAO", "2000-01-01 00:00:00")

DATA_INGESTAO = date.today().strftime("%Y-%m-%d")
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

def ler_watermark(s3_client) -> str:
    """Lista os arquivos de metadata no MinIO e retorna o timestamp
    do registro mais recente com status 'success'. Usa o valor padrão
    caso nenhum seja encontrado."""
    try:
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=METADATA_PREFIX)
        objetos = response.get("Contents", [])
        if not objetos:
            print("[INFO] Nenhum metadata encontrado. Usando valor padrão.")
            return WATERMARK_DEFAULT

        # Ordena do mais recente para o mais antigo pela data de modificação
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


def upload_df_as_csv(s3_client, df: pd.DataFrame, s3_key: str):
    """Serializa um DataFrame como CSV em memória e faz upload para o MinIO."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )
    print(f"[OK] Upload realizado: s3://{MINIO_BUCKET}/{s3_key}")


def upload_df_as_json(s3_client, df: pd.DataFrame, s3_key: str):
    """Serializa um DataFrame como JSON em memória e faz upload para o MinIO."""
    buffer = io.StringIO()
    df.to_json(buffer, orient="records", date_format="iso")
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="application/json"
    )
    print(f"[OK] Upload realizado: s3://{MINIO_BUCKET}/{s3_key}")

# =============================================================================
# EXTRAÇÃO - TABELA PAI: pedidos
# Usa data_ultima_atualizacao para capturar inserções e modificações
# =============================================================================

def extrair_pedidos(conn, ultima_execucao: str) -> pd.DataFrame:
    """
    Extrai registros da tabela pedidos modificados ou inseridos
    após a última execução (High-Watermark).
    """
    query = """
        SELECT *
        FROM pedidos
        WHERE data_ultima_atualizacao > %(ultima_execucao)s
    """
    print(f"[INFO] Extraindo pedidos com data_ultima_atualizacao > {ultima_execucao}")
    df = pd.read_sql(query, conn, params={"ultima_execucao": ultima_execucao})
    print(f"[INFO] {len(df)} registro(s) extraído(s) de 'pedidos'.")
    return df


# =============================================================================
# EXTRAÇÃO - TABELA FILHA: itens_pedido
# Itens são imutáveis após a compra; filtra por data_ultima_atualizacao
# =============================================================================

def extrair_itens_pedido(conn, ultima_execucao: str) -> pd.DataFrame:
    """
    Extrai registros da tabela itens_pedido criados após a última execução.
    Como os itens são imutáveis, basta filtrar pela data de criação.
    """
    query = """
        SELECT *
        FROM itens_pedido
        INNER JOIN pedidos ON itens_pedido.pedido_id = pedidos.id
        WHERE pedidos.data_ultima_atualizacao > %(ultima_execucao)s
    """
    print(f"[INFO] Extraindo itens_pedido com data_ultima_atualizacao > {ultima_execucao}")
    df = pd.read_sql(query, conn, params={"ultima_execucao": ultima_execucao})
    print(f"[INFO] {len(df)} registro(s) extraído(s) de 'itens_pedido'.")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    s3 = get_minio_client()
    ultima_execucao = ler_watermark(s3)

    print("=" * 60)
    print("FLUXO 1 - Transacional Pai-Filho (High-Watermark)")
    print(f"Data de ingestão : {DATA_INGESTAO}")
    print(f"Watermark utilizado: {ultima_execucao}")
    print("=" * 60)

    conn = None
    try:
        conn = get_pg_connection()
        # --- Extrai pedidos ---
        df_pedidos = extrair_pedidos(conn, ultima_execucao)
        if not df_pedidos.empty:
            key_pedidos = (
                f"bronze/transacional/pedidos/"
                f"data_ingestao={DATA_INGESTAO}/pedidos.csv"
            )
            upload_df_as_csv(s3, df_pedidos, key_pedidos)
        else:
            print("[INFO] Nenhum pedido novo. Nenhum arquivo gerado.")

        # --- Extrai itens_pedido ---
        df_itens = extrair_itens_pedido(conn, ultima_execucao)
        if not df_itens.empty:
            key_itens = (
                f"bronze/transacional/itens_pedido/"
                f"data_ingestao={DATA_INGESTAO}/itens.csv"
            )
            upload_df_as_csv(s3, df_itens, key_itens)
        else:
            print("[INFO] Nenhum item novo. Nenhum arquivo gerado.")

        metadata = pd.DataFrame([{"timestamp": TIMESTAMP_INGESTAO, "status": "success"}])
        upload_df_as_json(s3, metadata, f"bronze/transacional/metadata/{TIMESTAMP_INGESTAO}.json")
        print("FLUXO 1 concluído com sucesso.")

    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {e}")
        metadata = pd.DataFrame([{"timestamp": TIMESTAMP_INGESTAO, "status": "error", "mensagem": str(e)}])
        upload_df_as_json(s3, metadata, f"bronze/transacional/metadata/{TIMESTAMP_INGESTAO}.json")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
