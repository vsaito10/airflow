from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def init():
    options = Options()
    # Executar em modo headless (sem abrir o navegador)
    options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor='http://172.18.0.3:4444',
        options=options
    )
    driver.get("http://www.google.com")
    print('Entrou no site!')
    driver.quit()


with DAG(
    default_args=default_args,
    dag_id='teste_selenium',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_task = PythonOperator(
        task_id='init',
        python_callable=init
    )


init_task
