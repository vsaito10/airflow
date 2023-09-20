from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

default_args = {
    'owner': 'vitor',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def init(ti):
    url = "https://tr.ons.org.br/Content/Get/SituacaoDosReservatorios"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.5",
        "Connection": "keep-alive",
        "Origin": "https://www.ons.org.br",
        "Referer": "https://www.ons.org.br/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "sec-ch-ua": "^\^Not/A",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\^Windows^^"
    }

    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text)

    return data


def extract(ti):
    data = ti.xcom_pull(task_ids='init')
    # Selecionando as regiões
    regiao_norte = data[1]['Subsistema']
    regiao_nordeste = data[3]['Subsistema']
    regiao_sul = data[7]['Subsistema']
    regiao_sudeste = data[19]['Subsistema']

    # Selecionando as porcentagens dos níveis dos reservatórios
    nivel_norte = data[1]['SubsistemaValorUtil']
    nivel_nordeste = data[3]['SubsistemaValorUtil']
    nivel_sul = data[7]['SubsistemaValorUtil']
    nivel_sudeste = data[19]['SubsistemaValorUtil']

    # Selecionando a data da última atualização
    data_atualizacao = data[0]['Data']

    return {'regiao_sudeste': regiao_sudeste, 'regiao_sul': regiao_sul,
            'regiao_nordeste': regiao_nordeste, 'regiao_norte': regiao_norte,
            'nivel_sudeste': nivel_sudeste, 'nivel_sul': nivel_sul,
            'nivel_nordeste': nivel_nordeste, 'nivel_norte': nivel_norte,
            'data_atualizacao': data_atualizacao}


def transform(ti):
    regiao_sudeste = ti.xcom_pull(task_ids='extract')['regiao_sudeste']
    regiao_sul = ti.xcom_pull(task_ids='extract')['regiao_sul']
    regiao_nordeste = ti.xcom_pull(task_ids='extract')['regiao_nordeste']
    regiao_norte = ti.xcom_pull(task_ids='extract')['regiao_norte']
    nivel_sudeste = ti.xcom_pull(task_ids='extract')['nivel_sudeste']
    nivel_sul = ti.xcom_pull(task_ids='extract')['nivel_sul']
    nivel_nordeste = ti.xcom_pull(task_ids='extract')['nivel_nordeste']
    nivel_norte = ti.xcom_pull(task_ids='extract')['nivel_norte']
    data_atualizacao = ti.xcom_pull(task_ids='extract')['data_atualizacao']

    # Transformando em datetime a data de atualização
    data_atualizacao = pd.to_datetime(data_atualizacao)

    # Juntando os dados em duas listas
    regioes = [
        regiao_sudeste,
        regiao_sul,
        regiao_nordeste,
        regiao_norte
    ]

    niveis_reservatorios = [
        nivel_sudeste,
        nivel_sul,
        nivel_nordeste,
        nivel_norte
    ]

    # Juntando as duas listas
    dados_reservatorios = dict(zip(regioes, niveis_reservatorios))
    # Transformando em df
    df_reservatorios = pd.DataFrame(
        dados_reservatorios, index=[data_atualizacao]
    )
    # Retirando o horário deixando apenas a data
    df_reservatorios.index = df_reservatorios.index.date
    # Formatando a data de 'ano-mes-dia' para 'dia-mes-ano'
    df_reservatorios.index = df_reservatorios.index.map(
        lambda x: x.strftime('%d/%m/%Y')
    )
    # Retirando os espaços em branco nos nomes da coluna e colocando em letra minúscula
    df_reservatorios.columns = df_reservatorios.columns.str.replace(
        ' ', '').str.lower()
    # Transformando os números em porcentagem
    df_reservatorios = df_reservatorios.applymap(
        lambda x: f"{x:.2f}%"
    )

    return df_reservatorios


def load(ti):
    df_reservatorios = ti.xcom_pull(task_ids='transform')
    # Atualizando o arquivo csv com os novos dados (todo dia esse site é atualizado)
    try:
        nivel_reservatorios_final = pd.read_csv(
            'csv_tratados/nivel_reservatorios.csv',
            sep=';',
            index_col='Unnamed: 0'
        )
    except FileNotFoundError:
        print("O arquivo csv não foi encontrado.")

    # Concatenando o DataFrame existente com os novos dados
    nivel_reservatorios_final = pd.concat([
        nivel_reservatorios_final,
        df_reservatorios
    ])

    nivel_reservatorios_final.to_csv(
        'csv_tratados/nivel_reservatorios.csv',
        sep=';'
    )


with DAG(
    default_args=default_args,
    dag_id='nivel_reservatorios',
    start_date=datetime(2023, 8, 1),
    schedule_interval="0 11 * * *",
    catchup=False
) as dag:

    init_task = PythonOperator(
        task_id='init',
        python_callable=init
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    init_task >> extract_task >> transform_task >> load_task
