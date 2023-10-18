from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import os
import re


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def baixar_arquivo():
    # Diretório do download do arquivo
    download_directory = 'csv_tratados'

    # URL
    url = 'https://www.fecomercio.com.br/pesquisas/indice/peic'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'close'
    }

    # Fazendo a requisição HTTP para obter o conteúdo da página
    response = requests.get(url, headers=headers)

    # Criando o objeto BeautifulSoup para analisar o conteúdo HTML da página
    soup = BeautifulSoup(response.content, 'html.parser')
    # Tabela excel da pesquisa PEIC
    planilha = soup.find_all('a', class_='download')
    # Extraindo apenas o link de download da planilha excel que está dentro de 'href' -> 'https://www.fecomercio.com.br/upload/file/2023/10/06/peic_link_download_202309.xlsx'
    link_planilha = planilha[0].get('href')
    
    # Requisição para o link de download da planilha
    response = requests.get(link_planilha, headers=headers)

    # Fazendo o download do arquivo
    if response.status_code == 200:
        nome_arquivo = os.path.basename(link_planilha)
        
        # Renomeando o arquivo de 'peic_link_download_202309.xlsx' para 'peic_202309.xlsx'
        novo_nome = re.sub(r'peic_link_download_', 'peic_', nome_arquivo)
        novo_caminho = os.path.join(download_directory, novo_nome)

        with open(novo_caminho, 'wb') as f:
            f.write(response.content)
        

with DAG(
    default_args=default_args,
    dag_id='peic',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    baixar_arquivo_task = PythonOperator(
        task_id='baixar_arquivo',
        python_callable=baixar_arquivo
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> baixar_arquivo_task >> close_task
