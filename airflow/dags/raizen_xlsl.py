import sys
import subprocess
import numpy as np
import pandas as pd
import locale
import logging
import logging.config
import requests

from os import makedirs, path
from unicodedata import normalize

locale.setlocale(locale.LC_ALL, "pt_BR.utf8")

sys.path.append("/opt/airflow")
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def normalize_col(df: pd.DataFrame) -> pd.DataFrame:
    # obs: NFKD — Formato de Normalização de Compatibilidade de Decomposição
    return [
        normalize("NFKD", col).encode("ASCII", "ignore").decode("ASCII").lower()
        for col in df.columns
    ]


def troca_pos_df(df: pd.DataFrame) -> pd.DataFrame:
    
    index = -1
    TC = (len(df.columns) - 1) * -1
    for row_index, row in df.iterrows():
        index = index + 13 if index < TC else index
        dados = row.values.flatten().tolist()
        dados = np.roll(dados, index)
        df.iloc[row_index, :] = dados
        index = -1 if index < TC else index - 1
    return df


def normalize_data(df: pd.DataFrame, features: list, values: list) -> pd.DataFrame:
    
    df_features = df[features]
    df_values = troca_pos_df(df[values])
    return pd.concat([df_features, df_values], axis=1)


def normalize_vendas_combustiveis_m3(df: pd.DataFrame, features: list, values: list) -> pd.DataFrame:
    
    df.columns = normalize_col(df)
    df = normalize_data(df, features, values)
    df = df.melt(id_vars=features)
    df = df.loc[df["variable"] != "total"]
    df["created_at"] = pd.Timestamp.today()
    df["year_month"] = pd.to_datetime(df["ano"].astype(str) + "-" + df["variable"], format="%Y-%b")
    df["product"], df["unit"] = (
        df["combustivel"].str.split("(").str[0].str.strip(),
        df["combustivel"]
        .str.split("(")
        .str[1]
        .replace(to_replace="\\)", value="", regex=True)
        .str.strip(),
    )
    df = df.drop(labels=["variable", "regiao", "ano", "combustivel"], axis=1)
    df.rename(columns={"estado": "uf", "value": "volume"}, inplace=True)
    df.fillna(0, inplace=True)
    return df


def gera_parquet(xls_convertido, sheet, features, values, dest_path, dest_filename):
    logging.info("lendo planilha convertida por aba")
    df = pd.read_excel(xls_convertido, sheet_name=sheet)
    df = normalize_vendas_combustiveis_m3(df, features, values)
    makedirs(dest_path, exist_ok=True)
    logging.info(f"numero de linhas {df.count()}")
    df.to_parquet(dest_path + dest_filename)
    logging.info(f"gravou parquet  {dest_path + dest_filename}")


def convert_xls_libre(path: str, out_dir: str) -> None:

    logging.info("executa linha de comando libreoffice")
    makedirs(out_dir, exist_ok=True)
    linha_command = f"libreoffice --headless --convert-to xls --outdir {out_dir} {path}"
    process = subprocess.Popen(linha_command.split(), stdout=subprocess.PIPE)
    output, error_conv = process.communicate()
    if error_conv:
        logging.error(f"Erro convertendo file {path}")
        raise
    logging.info(f"Gerado arquivo convertido {path}")


def salva_file(file: bytes, target_file: str):

    try:
         with open(target_file, "wb") as f:
            f.write(file)
    except Exception as teste_w:
        logging.error(f"erro ao gravar arquivo do xls carregado - {teste_w}")
        raise



def download_vendas_combustiveis_m3(url, xls_web):
    logger = logging.getLogger(__name__)
    filename = url.split("/")[-1]
    filepath = "/".join([xls_web, filename])

    logger.info("cria diretorio para xls")
    makedirs(xls_web, exist_ok=True)
    logging.info("request e gravação xls")
    web_file = requests.get(url)
    salva_file(web_file.content, filepath )


default_args = {
    "owner": "Claudio cesar",
    "start_date": days_ago(1),
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="download_vendas_combustiveis_m3",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@once",
    description="Download vendas-combustiveis-m3",
    catchup=False,
) as dag:
    #  Tarefa inicial - Download vendas-combustiveis-m3 origem github
    task_download_vendas_combustiveis_m3 = PythonOperator(
        task_id="download_vendas_combustiveis_m3",
        python_callable=download_vendas_combustiveis_m3,
        dag=dag,
        op_kwargs={
            "url": "https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls",
            "xls_web": "/home/airflow/xls_web",
        },
    )

    # Tarefa conversao  - Conversão xls usando o Libreoffice
    task_convert_xls_libre = PythonOperator(
        task_id="convert_xls_libre",
        python_callable=convert_xls_libre,
        dag=dag,
        op_kwargs={
            "path": "/home/airflow/xls_web/vendas-combustiveis-m3.xls",
            "out_dir": "/home/airflow/convertido/",
        },
    )

    # Tarefa geracao parquet sales oil derivative fuels
    task_gera_parquet_sales_oil_derivative_fuels = PythonOperator(
        task_id="gera_parquet_sales_oil_derivative_fuels",
        python_callable=gera_parquet,
        dag=dag,
        op_kwargs={
            "xls_convertido": "/home/airflow/convertido/vendas-combustiveis-m3.xls",
            "sheet": "DPCache_m3",
            "dest_path": "/home/airflow/parquet/",
            "dest_filename": "sales_oil_derivative_fuels.parquet",
            "features": ["combustivel", "ano", "regiao", "estado"],
            "values": ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez","total",],
        },
    )

    # Tarefa geracao parquet sales diesel
    task_gera_parquet_sales_diesel = PythonOperator(
        task_id="gera_parquet_sales_diesel",
        python_callable=gera_parquet,
        dag=dag,
        op_kwargs={
            "xls_convertido": "/home/airflow/convertido/vendas-combustiveis-m3.xls",
            "sheet": "DPCache_m3_2",
            "dest_path": "/home/airflow/parquet/",
            "dest_filename": "sales_diesel.parquet",
            "features": ["combustivel", "ano", "regiao", "estado"],
            "values": ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez","total",],
        },
    )

    (
        task_download_vendas_combustiveis_m3
        >> task_convert_xls_libre
        >> task_gera_parquet_sales_oil_derivative_fuels
        >> task_gera_parquet_sales_diesel
    )
