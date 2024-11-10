from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info("Extract Started!")
    f = requests.get('https://restcountries.com/v3/all')
    return (f.json())


@task
def transform(json):
    logging.info("Transform Started!")
    records = []
    for element in json:
        country = element['name']['official'].replace('\'', '')
        population = element['population']
        area = element['area']
        records.append([country, population, area])
    return records


@task
def load(records):
    logging.info("Load started!")
    cur = get_Redshift_connection()
    try:
        sql = """
        DROP TABLE IF EXISTS hojoong310.world_information;
        CREATE TABLE hojoong310.world_information (
            country varchar(100),
            population bigint,
            area float
        );
        """
        cur.execute(sql)

        for r in records:
            country = r[0]
            population = r[1]
            area = r[2]
            print(country, "-", population, '-', area)
            sql = "INSERT INTO hojoong310.world_information VALUES ('{c}', '{p}', '{a}')".format(c=country, p=population, a=area)
            cur.execute(sql)
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("Load Complete!")


with DAG(
    dag_id = 'world_metadata',
    start_date = datetime(2024,11,1),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6', #Every Sat(6) at 06:30(UTC)
) as dag:
    
    extract >> transform >> load