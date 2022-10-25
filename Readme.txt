## Airflow deploy

git clone https://github.com/claudiopower/data-engineering-raizen-test.git

cd data-engineering-raizen-test/airflow

docker-compose up airflow-init
 
docker-compose up -d
 
------------------------- SAIDA --------------------------------
 airflow-worker
 
/home/airflow/parquet/sales_diesel.parquet
/home/airflow/parquet/sales_oil_derivative_fuels.parquet
 
