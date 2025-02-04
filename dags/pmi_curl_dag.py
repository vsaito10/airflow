from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from curl_cffi import requests
import pandas as pd


"""
URLs:
PMI Serviços - https://br.investing.com/economic-calendar/services-pmi-1062
PMI Industrial - https://br.investing.com/economic-calendar/manufacturing-pmi-829
PMI ISM Não-Manufatura - https://br.investing.com/economic-calendar/ism-non-manufacturing-pmi-176
PMI ISM Industrial - https://br.investing.com/economic-calendar/ism-manufacturing-pmi-173
PMI Industrial China - https://br.investing.com/economic-calendar/chinese-manufacturing-pmi-594
PMI Servicos China - https://br.investing.com/economic-calendar/chinese-non-manufacturing-pmi-831
"""

default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def pmi_table(num_tipo_pmi):
    # Configuração dos headers para a requisição HTTP
    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://br.investing.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'sec-ch-ua-mobile': '?0',
    }

    # Fazendo a requisição GET para a URL do PMI
    response = requests.get(f'https://sbcharts.investing.com/events_charts/eu/{num_tipo_pmi}.json', headers=headers)
    print(response.status_code)
    data = response.json()

    # Criando um DataFrame a partir dos dados JSON
    df = pd.DataFrame(data['attr'])
    # Convertendo a coluna 'timestamp' de milissegundos para datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    # Formatando a data para o formato 'YYYY-MM-DD'
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    # Convertendo a coluna 'timestamp' para o tipo datetime 
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d')
    # Selecionando as colunas principais
    df = df[['timestamp', 'actual']]
    # Definindo a coluna 'timestamp' como índice do DataFrame
    df = df.set_index('timestamp')

    # Mapeando os tipos de PMI
    dict_tipos_pmi = {
        '829': 'pmi_industrial',
        '1062': 'pmi_servicos',
        '173': 'pmi_industrial_ism',
        '176': 'pmi_ism_nao_manufatura',
        '594': 'china_pmi_industrial',
        '831': 'china_pmi_nao_manufatura'
    }

    # Selecionando qual é a string do PMI correspondente ao 'num_tipo_pmi'
    tipo_pmi = dict_tipos_pmi.get(num_tipo_pmi, 'tipo_pmi_desconhecido')

    # Salvando em um arquivo csv
    df.to_csv(f'csv_tratados/{tipo_pmi}.csv', sep=';')


with DAG(
    default_args=default_args,
    dag_id='pmi_teste',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    web_scraping_data_task = PythonOperator(
        task_id='web_scraping_data',
        python_callable=pmi_table,
        op_kwargs={'num_tipo_pmi':'173'}
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> web_scraping_data_task >> close_task