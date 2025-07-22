from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

from tasks.extracao_g_sheets_dk import google_sheet_to_minio_etl

# Argumentos iniciais.
default_args = {
    'owner': 'Wuldson',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Parametro para dags
@dag(
    dag_id='dag_main',
    default_args=default_args,
    description='Dag curso JDEA',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['extração', 'etl', 'raw'],
)


def main_dag():
    google_sheets = ['Clientes', 'Vendedores', 'Produtos', 'Vendas', 'ItensVendas']  # Altere para o nome das abas da sua planilha
    bucket_name = 'raw'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '1XMrFMPptCKfy6u2tsP0FuAgPlbJ-anA0caH2e6LL5F0'

    with TaskGroup("group_task_sheets", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as group_task_sheets:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheets_name}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    # Definindo ordem das tarefas
    group_task_sheets


main_dag_instance = main_dag()