from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Funções de exemplo para as tasks
def extrair_dados():
    print("Extraindo dados do SEI ou de base de processos...")

def transformar_dados():
    print("Transformando dados: limpeza, parsing de texto, etc.")

def carregar_dados():
    print("Carregando dados no banco ou data lake...")

default_args = {
    'owner': 'anatel_ted',
    'depends_on_past': False,
    'email': ['seu_email@dominio.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'exemplo_pado_etl',
    default_args=default_args,
    description='DAG de exemplo para pipeline de dados PADO Anatel',
    schedule_interval='@daily',  # Executa diariamente
    start_date=days_ago(1),
    catchup=False,
    tags=['pado', 'etl', 'exemplo'],
) as dag:

    extrair = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados
    )

    transformar = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )

    carregar = PythonOperator(
        task_id='carregar_dados',
        python_callable=carregar_dados
    )

    # Definindo ordem das tarefas
    extrair >> transformar >> carregar
