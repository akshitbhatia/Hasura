docker build . --tag hasura-img:0.0.1
mkdir -p ./logs ./plugins
docker-compose up airflow-init
docker-compose up