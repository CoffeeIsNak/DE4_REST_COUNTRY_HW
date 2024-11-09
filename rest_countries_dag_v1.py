from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from datetime import datetime
import logging

# Redshift로 연결하는 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    logging.info("Redshift connect success")
    return conn.cursor()

# Full Refresh를 위해 테이블을 비우는 함수.
def _refresh_table(cur, schema, table):
    cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    country varchar(50),
    population bigint,
    area bigint
);""")


# extract task - api에서 json들의 list를 받아옴
@task
def extract():
    http = HttpHook(http_conn_id='rest_country_api_conn', method='GET')
    endpoint = Variable.get('rest_counry_api_')

    response = http.run(endpoint, timeout=30)
    logging.info("api works well")
    data = response.json()
    logging.info("extract done")
    return data


# json에서 원하는 정보만 가져와서 records 리스트에 담아 반환
@task
def transform(data):
    logging.info("transform start")
    records = []

    for country in data:
        records.append([country['name']['official'], country['population'], country['area']])
    
    logging.info("transform done")
    return records


# 실제 테이블에 적재
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 테이블 삭제 후 재생성
        _refresh_table(cur, schema, table)

        # 데이터 적재
        for r in records:
            sql = f"INSERT INTO {schema}.{table} (country, population, area) VALUES (%s, %s, %s);"
            cur.execute(sql, (r[0], r[1], r[2]))

        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")


with DAG(
    dag_id = 'RestCountriesDag_v1',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:

    load(transform(extract()))