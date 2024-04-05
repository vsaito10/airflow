from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from time import sleep
import os
import requests


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def download_planilhas():
    # Diretório do download do arquivo
    download_directory = 'csv_tratados'

    options = Options()
    options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor='http://172.19.0.3:4444',
        options=options
    )

    driver.get('https://www.tesourodireto.com.br/titulos/historico-de-precos-e-taxas.htm')
    sleep(1)

    # Clicando no botão de aceitar cookies
    try:
        botao_cookie = driver.find_element(By.XPATH, '//*[@id="onetrust-accept-btn-handler"]')
        botao_cookie.click()
    except:
        pass

    # Obtendo o HTML 
    html = driver.page_source

    # Analisando o HTML com BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Encontrando todos os elementos <a> dentro da div com a classe 'td-download-docs-box'
    links = soup.find_all('a', class_='td-download-docs-card anual')

    # Iterando sobre os links encontrados e obtenha o atributo 'href' de cada um
    lst_links = []
    for link in links:
        href = link['href']
        lst_links.append(href)

    # Não preciso dos dois primeiros links da lista ('valor-nominal-de-ntn-b' e 'valor-nominal-de-ntn-c')
    lst_links = lst_links[2:]

    # Iterando sobre os links das planilhas p/ fazer o seu download
    for link in lst_links:
        response = requests.get(link)
        if response.status_code == 200:
            nome_arquivo = os.path.join(download_directory, os.path.basename(link))
            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)
                sleep(5)

    driver.quit()


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

    download_planilhas_task = PythonOperator(
        task_id='download_arquivo',
        python_callable=download_planilhas
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> download_planilhas_task >> close_task
