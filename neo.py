from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import os
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bank_data_load',
    default_args=default_args,
    description='ETL process for loading bank data from CSV to PostgreSQL',
    schedule_interval=None,
    catchup=False,
)

# Path to your CSV files
csv_path = '/opt/airflow/dags/'

def log_start(**kwargs):
    # Logging start of the process
    print("Logging start of the ETL process")
    return {"message": "ETL process started", "timestamp": datetime.now()}

def log_end(**kwargs):
    # Logging end of the process
    print("Logging end of the ETL process")
    return {"message": "ETL process finished", "timestamp": datetime.now()}

def load_csv_to_postgres(table_name, **kwargs):
    # Load CSV data into PostgreSQL table
	
    # Параметры подключения
    db_user = 'postgres'
    db_password = '12345678'
    db_host = 'postgres'
    db_port = '5433'
    db_name = 'postgres'
    
    # Формирование строки подключения
    conn_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Создание подключения
    engine = create_engine(conn_str)
    
    file_path = os.path.join(csv_path, f"{table_name}.csv")
    df = pd.read_csv(file_path)
    df.to_sql(table_name, con=engine, schema='DS', if_exists='replace', index=False)

    return f"Loaded {len(df)} rows into {table_name}"

with dag:
    t1 = PythonOperator(
        task_id='start_load_process',
        python_callable=log_start,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='load_md_account_d',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'MD_ACCOUNT_D'},
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='load_md_currency_d',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'MD_CURRENCY_D'},
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id='load_md_exchange_rate_d',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'MD_EXCHANGE_RATE_D'},
        provide_context=True,
    )

    t5 = PythonOperator(
        task_id='load_md_ledger_account_s',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'MD_LEDGER_ACCOUNT_S'},
        provide_context=True,
    )

    t6 = PythonOperator(
        task_id='load_ft_posting_f',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'FT_POSTING_F'},
        provide_context=True,
    )

    t7 = PythonOperator(
        task_id='end_load_process',
        python_callable=log_end,
        provide_context=True,
    )

    t1 >> [t2, t3, t4, t5, t6] >> t7

