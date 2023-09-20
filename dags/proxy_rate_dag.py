from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os
import requests


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def download_arquivo():
    # Diretório do download do arquivo
    download_directory = 'csv_tratados'

    # URL do download do arquivo. Esse link é do site https://www.frbsf.org/economic-research/indicators-data/proxy-funds-rate/
    url = 'https://www.frbsf.org/wp-content/uploads/sites/4/proxy-funds-rate-data.xlsx?202305052'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'close'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Obtém o nome do arquivo sem a parte da URL após o "?" -> 'proxy-funds-rate-data.xlsx'
        nome_arquivo = os.path.basename(url).split("?")[0]
        nome_arquivo = os.path.join(download_directory, nome_arquivo)
        with open(nome_arquivo, 'wb') as f:
            f.write(response.content)
            print("Arquivo baixado com sucesso.")


with DAG(
    default_args=default_args,
    dag_id='proxy_rate',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    download_arquivo_task = PythonOperator(
        task_id='download_arquivo',
        python_callable=download_arquivo
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> download_arquivo_task >> close_task
