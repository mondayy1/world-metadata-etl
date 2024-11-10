# world-metadata-etl
ETL pipeline to load country metadata from https://restcountries.com/ into a data warehouse

## Command Lines

1. clone repo
``` bash
$ git clone git@github.com:mondayy1/world-metadata-etl.git
```

2. get into dir
``` bash
$ cd airflow-setup
```

3. download airflow 2.9.1 yml
``` bash
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
```

4. image pull and up
``` bash
$ docker compose -f docker-compose.yaml pull
$ docker compose -f docker-compose.yaml up
```

5. trigger dag
``` bash
$ docker exec -it airflow-setup-airflow-webserver-1 airflow dags trigger world_metadata 
```
