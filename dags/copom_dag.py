from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
from time import sleep
import os
import requests
import shutil


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def baixar_arquivo(url_path):
    # Diretório do download do arquivo 
    download_directory = 'copom_pdf'

    options = Options()
    options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor='http://172.20.0.3:4444',
        options=options
    )

    driver.get(url_path)
    sleep(1)

    # Clicando no botão de cookie
    botao_cookie = driver.find_element(By.XPATH, '/html/body/app-root/bcb-cookies/div/div/div/div/button[2]').click()
    sleep(1)

    # Botão de download
    botao_download = driver.find_element(By.XPATH, '//*[@id="publicacao"]/div[1]/div/div/div/div[1]/div[2]/download/div/div/a')

    # Eu não clico no botão, porque eu não consigo redirecionar o download do arquivo para a pasta que eu quero
    # Por isso, eu seleciono o link do download do arquivo que está dentro do 'href'
    link_planilha = botao_download.get_attribute('href')            

    # Fazendo a requisição HTTP para obter o conteúdo da página
    response = requests.get(link_planilha, stream=True)

    if response.status_code == 200:
        nome_arquivo = os.path.basename(link_planilha)
        nome_arquivo = os.path.join(download_directory, nome_arquivo)
        with open(nome_arquivo, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
            print(f"Arquivo '{nome_arquivo}' baixado com sucesso.")
    else:
        print(f"Não foi possível fazer o download do arquivo. Status de resposta: {response.status_code}")
    
    sleep(2)            

    # Fechando o driver
    driver.quit()


with DAG(
    default_args=default_args,
    dag_id='copom',
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
        python_callable=baixar_arquivo,
        op_kwargs={
            'url_path':'https://www.bcb.gov.br/en/publications/copomminutes/20092023'
        }
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> baixar_arquivo_task >> close_task   
