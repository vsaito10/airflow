from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from time import sleep
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import re


default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def web_scraping_table():
    options = Options()
    options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor='http://172.18.0.3:4444',
        options=options
    )

    driver.get(
        'https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-tipo-de-participante-ptBR.asp'
    )
    sleep(2)

    # Data de divulgação
    data_divulgacao = driver.find_element(
        By.XPATH, '//*[@id="divContainerIframeBmf"]/div[1]/div/form/div/div[3]/p').text
    # Selecionando a data na string 'Atualizado em: 04/09/2023'
    padrao_data = r'\d{2}/\d{2}/\d{4}'
    data_divulgacao = re.findall(padrao_data, data_divulgacao)[0]
    # Transformando em datetime
    data_divulgacao = pd.to_datetime(data_divulgacao)

    # As tabelas do site estão dentro desse XPATH
    tabelas = driver.find_elements(
        By.XPATH, '//*[@id="divContainerIframeBmf"]/div[2]/div'
    )

    for tabela in tabelas:  
        # Títulos das tabelas p/ descobrir em qual posição o 'MERCADO FUTURO DE DÓLAR' está 
        lst_titulo = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{i}]/caption').text 
            for i in range(1, 35)
        ]
        # Esse é o número do XPATH que é dinâmico no site da B3
        posicao_dol_xpath = lst_titulo.index('MERCADO FUTURO DE DÓLAR') + 1     
        print(f'A posição do XPATH do dólar futuro é de {posicao_dol_xpath}')

        # Nome das colunas
        lst_colunas = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{posicao_dol_xpath}]/thead/tr[1]/th[{i}]').text
            for i in [2, 3]
        ]

        # Nome das linhas da tabela
        # 'Bancos' e 'DTVM'S e Corretoras de Valores'
        lst_players = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{posicao_dol_xpath}]/tbody/tr[{i}]/td[1]').text
            for i in [2, 3]
        ]                                 

        # 'Investidor Institucional', 'Investidores Não Residentes', 'Pessoa Jurídica Não Financeira' e 'Pessoa Física'
        lst_players_2 = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{posicao_dol_xpath}]/tbody/tr[{i}]/td[1]/strong').text
            for i in [4, 6, 8, 9]
        ]                                 
        
        # Juntando as listas dos players
        lst_player_final = lst_players + lst_players_2

        # Números da tabela
        lst_num_compra = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{posicao_dol_xpath}]/tbody/tr[{i}]/td[2]').text
            for i in [2, 3, 4, 6, 8, 9] 
        ]              

        lst_num_venda = [
            tabela.find_element(By.XPATH, f'//*[@id="divContainerIframeBmf"]/div[2]/div/table[{posicao_dol_xpath}]/tbody/tr[{i}]/td[4]').text
            for i in [2, 3, 4, 6, 8, 9] 
        ]

    # Criando um dicionário com os dados da coluna
    data = {col: [] for col in lst_colunas}

    # Adicionando listas vazias para cada coluna
    for col in lst_colunas:
        data[col] = []

    # Preenchendo as linhas com os números
    data['Compra'] = lst_num_compra
    data['Venda'] = lst_num_venda

    # Criando o DataFrame
    df = pd.DataFrame(data, index=lst_player_final)

    # Substituindo os pontos por vírgulas em todo o DataFrame
    df = df.apply(lambda x: x.str.replace('.', ''))

    # Convertendo os valores para números (float)
    df = df.apply(pd.to_numeric, errors='ignore')

    # Calculando a soma das colunas 'Compra' e 'Venda'
    soma_compra = df['Compra'].sum()
    soma_venda = df['Venda'].sum()

    # Adicionando essa nova linha (soma das colunas) no df
    linha_total = pd.DataFrame(
        {'Compra': [soma_compra], 'Venda': [soma_venda]}, index=['Total'])
    df = pd.concat([df, linha_total])

    # Renomeando o index do df com a data de divulgação
    df = df.rename_axis(data_divulgacao.date())

    # Transformando em um arquivo csv
    df.to_csv('csv_tratados/contratos_aberto_dolar.csv', sep=';', index=True)

    # Fechando o driver
    driver.quit()


with DAG(
    default_args=default_args,
    dag_id='contratos_abertos_b3_v2',
    start_date=datetime(2023, 8, 1),
    schedule_interval='30 8 * * *',
    catchup=False
) as dag:

    init_task = DummyOperator(
        task_id='init',
        dag=dag,
    )

    web_scraping_table_task = PythonOperator(
        task_id='web_scraping_table',
        python_callable=web_scraping_table
    )

    close_task = DummyOperator(
        task_id='close',
        dag=dag,
    )

    init_task >> web_scraping_table_task >> close_task
