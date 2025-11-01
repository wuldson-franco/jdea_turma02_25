import boto3
import pandas as pd
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook
from botocore.exceptions import ClientError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_parquet_from_minio(bucket_name, key, endpoint_url, access_key, secret_key):
    # Lê um arquivo Parquet do MinIO e retorna um DataFrame.
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        response = minio_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        logger.info(f"Arquivo {key} lido com sucesso do bucket {bucket_name}")
        return df
    except ClientError as e:
        logger.error(f"Erro ao ler arquivo {key} do bucket {bucket_name}: {e}")
        raise

def generate_indicators(df_vendas, df_itens, df_produtos, df_clientes):
    # Gera indicadores de consumo analítico.
    logger.info("Gerando indicadores de consumo...")

    df_merge = df_itens.merge(df_produtos, on="produtoid", how="left") \
                       .merge(df_vendas, on="vendaid", how="left") \
                       .merge(df_clientes, on="clienteid", how="left")

    # Produto mais vendido
    produto_mais_vendido = df_merge.groupby("produto")["quantidade"].sum().sort_values(ascending=False).head(1)

    # Cliente que mais comprou
    cliente_mais_comprou = df_merge.groupby("cliente")["valortotal"].sum().sort_values(ascending=False).head(1)

    # Período (mês) de maior venda
    df_merge["data"] = pd.to_datetime(df_merge["data"], errors="coerce")
    periodo_maior_venda = (
        df_merge.groupby(df_merge["data"].dt.to_period("M"))["valortotal"]
        .sum()
        .sort_values(ascending=False)
        .head(1)
    )

    indicadores_df = pd.DataFrame({
        "produto_mais_vendido": produto_mais_vendido.index,
        "qtd_vendida": produto_mais_vendido.values,
        "cliente_mais_comprou": cliente_mais_comprou.index,
        "valor_total_comprado": cliente_mais_comprou.values,
        "periodo_maior_venda": [str(periodo_maior_venda.index[0])],
        "valor_periodo": [float(periodo_maior_venda.values[0])]
    })

    logger.info("Indicadores gerados com sucesso!")
    return indicadores_df

def process_consumer_layer(bucket_silver, bucket_gold, endpoint_url, access_key, secret_key):
    # Processa dados da camada Silver, gera indicadores e salva no Gold e MariaDB.
    logger.info("Iniciando processamento da camada Consumer (Gold Layer)")

    # Conectar ao MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Cria o bucket gold se não existir
    try:
        minio_client.head_bucket(Bucket=bucket_gold)
        logger.info(f"O bucket '{bucket_gold}' já existe.")
    except ClientError as e:
        if e.response['Error']['Code'] in ['404', 'NoSuchBucket']:
            minio_client.create_bucket(Bucket=bucket_gold)
            logger.info(f"Bucket '{bucket_gold}' criado com sucesso.")
        else:
            raise e

    # Lê arquivos Parquet da camada Silver
    df_clientes = read_parquet_from_minio(bucket_silver, "Clientes/data_silver.parquet", endpoint_url, access_key, secret_key)
    df_produtos = read_parquet_from_minio(bucket_silver, "Produtos/data_silver.parquet", endpoint_url, access_key, secret_key)
    df_vendas = read_parquet_from_minio(bucket_silver, "Vendas/data_silver.parquet", endpoint_url, access_key, secret_key)
    df_itens = read_parquet_from_minio(bucket_silver, "ItensVendas/data_silver.parquet", endpoint_url, access_key, secret_key)

    # Gera indicadores
    indicadores_df = generate_indicators(df_vendas, df_itens, df_produtos, df_clientes)

    # Salva indicadores no bucket Gold
    output_buffer = io.BytesIO()
    indicadores_df.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)

    gold_key = "indicadores/indicadores_vendas.parquet"
    minio_client.put_object(Bucket=bucket_gold, Key=gold_key, Body=output_buffer.getvalue())
    logger.info(f"Indicadores salvos com sucesso no bucket Gold: {gold_key}")

    # Insere os dados no MariaDB
    try:
        mysql_hook = MySqlHook(mysql_conn_id="mariadb_local")
        connection = mysql_hook.get_conn()

        with connection.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS indicadores_vendas (
                produto_mais_vendido VARCHAR(255),
                qtd_vendida FLOAT,
                cliente_mais_comprou VARCHAR(255),
                valor_total_comprado FLOAT,
                periodo_maior_venda VARCHAR(20),
                valor_periodo FLOAT
            )
            """
            cursor.execute(create_table_sql)
            connection.commit()
            logger.info("Tabela 'indicadores_vendas' criada/verificada com sucesso.")

            # Inserir os dados
            for _, row in indicadores_df.iterrows():
                insert_sql = """
                INSERT INTO indicadores_vendas 
                (produto_mais_vendido, qtd_vendida, cliente_mais_comprou, valor_total_comprado, periodo_maior_venda, valor_periodo)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, tuple(row))
            connection.commit()
            logger.info("Dados inseridos com sucesso na tabela indicadores_vendas.")

    except Exception as e:
        logger.error(f"Erro ao salvar dados no MariaDB: {e}")
        raise
    finally:
        if connection:
            connection.close()
            logger.info("Conexão com o MariaDB encerrada.")