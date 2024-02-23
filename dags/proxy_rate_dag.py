from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import re
import requests


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def download_arquivo():
    # Diretório do download do arquivo
    download_directory = 'csv_tratados'

    # Site do FED San Francisco
    url = 'https://www.frbsf.org/research-and-insights/data-and-indicators/proxy-funds-rate/'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'close'
    }

    # Solicitando o HTTP GET do site do FED San Francisco
    response = requests.get(url, headers=headers)

    # HTML da página
    soup = BeautifulSoup(response.text, 'html.parser')

    # Usando o regex para encontrar o link da planilha
    link_pattern = re.compile(r'/wp-content/uploads/proxy-funds-rate-data\.xlsx\?(\d+)')
    link = soup.find('a', href=link_pattern)

    # Selecionando o href -> '/wp-content/uploads/proxy-funds-rate-data.xlsx?20240105'
    href = link.get('href')

    # URL do download da planilha do Proxy Rate
    url_planilha = f'https://www.frbsf.org/{href}'

    # Fazendo o donwload da planilha do Proxy Rate
    response_planilha = requests.get(url_planilha, headers=headers)

    if response_planilha.status_code == 200:
        nome_arquivo = os.path.basename(url_planilha).split("?")[0]  # Obtém o nome do arquivo sem a parte da URL após o "?" -> 'proxy-funds-rate-data.xlsx'
        nome_arquivo = os.path.join(download_directory, nome_arquivo)
        with open(nome_arquivo, 'wb') as f:
            f.write(response_planilha.content)
            print("Arquivo baixado com sucesso.")

    else:
        print('Arquivo não encontrado.')


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
