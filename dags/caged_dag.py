from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import os
from time import sleep


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def init(ti):
    url = 'http://pdet.mte.gov.br/novo-caged'
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Procurando o texto com a data de lançamento do Novo Caged
    data = soup.find('h2', class_='outstanding-title').text

    return data


def formatando_data(ti):
    data = ti.xcom_pull(task_ids='init')
    # Selecionando apenas a data com regex
    padrao = r"\w+ - (.*)"
    data_completa = re.findall(padrao, data)[0]

    # Selecionando apenas o mês e o ano separadamente
    padrao = r'(\w+)'  # Separa a string em ['Abril', 'de', '2023']
    mes = re.findall(padrao, data_completa)[0]
    ano = re.findall(padrao, data_completa)[2]

    # Dicionário de correspondência entre nomes dos meses e os seus números
    meses_para_numeros = {
        'Janeiro': '01',
        'Fevereiro': '02',
        'Março': '03',
        'Abril': '04',
        'Maio': '05',
        'Junho': '06',
        'Julho': '07',
        'Agosto': '08',
        'Setembro': '09',
        'Outubro': '10',
        'Novembro': '11',
        'Dezembro': '12'
    }

    # Obter o número correspondente usando o dicionário
    numero_mes = meses_para_numeros[mes]

    return ano, numero_mes


def download_arquivo(ti):
    ano = ti.xcom_pull(task_ids='formatando_data')[0]
    numero_mes = ti.xcom_pull(task_ids='formatando_data')[1]

    caminho_arquivo = 'csv_tratados/3-tabelas.xlsx'

    url_download = f'http://pdet.mte.gov.br/images/Novo_CAGED/{ano}/{ano}{numero_mes}/3-tabelas.xlsx'
    response = requests.get(url_download)

    if response.status_code == 200:
        with open(caminho_arquivo, 'wb') as f:
            f.write(response.content)
        print("Arquivo baixado com sucesso.")

        # Novo nome do arquivo
        novo_nome_arquivo = f'{ano}{numero_mes}_caged.xlsx'
        novo_caminho_arquivo = os.path.join('csv_tratados', novo_nome_arquivo)

        # Renomeando o arquivo
        os.rename(caminho_arquivo, novo_caminho_arquivo)
        print("Arquivo renomeado com sucesso.")

    else:
        print("Erro ao baixar o arquivo.")


with DAG(
    default_args=default_args,
    dag_id='caged_dag',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = PythonOperator(
        task_id='init',
        python_callable=init
    )

    formatando_data_task = PythonOperator(
        task_id='formatando_data',
        python_callable=formatando_data
    )

    download_arquivo_task = PythonOperator(
        task_id='download_arquivo',
        python_callable=download_arquivo
    )

init_task >> formatando_data_task >> download_arquivo_task
