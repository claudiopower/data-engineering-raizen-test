## Todo o teste foi resolvido utilizando o docker com imagem do Airflow
## Airflow deploy

git clone https://github.com/claudiopower/data-engineering-raizen-test.git

cd data-engineering-raizen-test/airflow
 
docker-compose up -d
 
------------------------- SAIDA --------------------------------
airflow-worker
 
/home/airflow/parquet/sales_diesel.parquet
/home/airflow/parquet/sales_oil_derivative_fuels.parquet
 
no diretorio raiz esta o arquivo Jupyter Notebook: teste_parquet.ipynb que foi usado para validar os dados

também existe uma imagem do Dag executado com sucesso.

evidencia execução Airflow.jpg