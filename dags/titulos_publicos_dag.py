from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os
import requests
from time import sleep


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def download_arquivo(url_list):
    # Diretório do download do arquivo
    download_directory = 'csv_tratados'

    for url in url_list:
        response = requests.get(url)
        if response.status_code == 200:
            nome_arquivo = os.path.join(
                download_directory, os.path.basename(url))
            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)
                print("Arquivo baixado com sucesso.")
        sleep(5)  # Intervalo entre downloads


with DAG(
    default_args=default_args,
    dag_id='titulos_publicos',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    # Lista com as URLs dos downloads dos arquivos. Esses links são do site https://www.tesourodireto.com.br/titulos/historico-de-precos-e-taxas.htm
    url_list = [
        'https://cdn.tesouro.gov.br/sistemas-internos/apex/producao/sistemas/sistd/2023/NTN-B_2023.xls',
        'https://cdn.tesouro.gov.br/sistemas-internos/apex/producao/sistemas/sistd/2023/NTN-B_Principal_2023.xls',
        'https://cdn.tesouro.gov.br/sistemas-internos/apex/producao/sistemas/sistd/2023/LTN_2023.xls',
    ]

    download_arquivo_task = PythonOperator(
        task_id='download_arquivo',
        python_callable=download_arquivo,
        op_args=[url_list]
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> download_arquivo_task >> close_task
