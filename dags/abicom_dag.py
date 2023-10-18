from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup
import re
from time import sleep


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def web_scraping_imagem():
    options = Options()
    options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor='http://172.20.0.3:4444',
        options=options
    )

    # Site Abicom
    url = 'https://abicom.com.br/categoria/ppi/'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Acessando o site da Abicom
    driver.get(url)
    sleep(2)

    # Imagem do relatório da Abicom
    imagem = driver.find_element(
        By.XPATH, f'//*[@id="page"]/article/div/div/div/div/div/div[1]/a/img')

    # Data de quando a imagem foi publicada
    data = driver.find_element(
        By.XPATH, f'//*[@id="page"]/article/div/div/div/div/div/div[1]/a/div/h5').text
    # Selecionando apenas a data da string ('PPI - 16/10/2023' -> '16/10/2023')
    padrao = r'\d{2}/\d{2}/\d{4}'
    correspondencia = re.findall(padrao, data)
    # Transformando o formato da data de '16/10/2023' para '20231016'
    data_original = correspondencia[0]
    data_formatada = datetime.strptime(
        data_original, "%d/%m/%Y").strftime("%Y%m%d")

    # Tirando o screenshot da imagem
    arquivo_imagem = f'abicom_imagem/{data_formatada}_abicom.png'
    imagem.screenshot(arquivo_imagem)

    # Fechando o driver
    driver.quit()


with DAG(
    default_args=default_args,
    dag_id='abicom',
    start_date=datetime(2023, 8, 1),
    schedule_interval='00 10 * * *',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    web_scraping_imagem_task = PythonOperator(
        task_id='web_scraping_imagem',
        python_callable=web_scraping_imagem
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> web_scraping_imagem_task >> close_task
