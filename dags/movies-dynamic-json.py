from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator, 
        PythonVirtualenvOperator, 
        BranchPythonOperator
        )

with DAG(
    'movies-dynamic-json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016,1,1),
    catchup=True,
    tags=["movies","dynamic","json"],
) as dag:
    def re_parti(ds_nodash):
        from spark_repart.re_part import re_part
        re_part(ds_nodash)

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/data/movie/repartition/load_dt={ds_nodash}'
        if os.path.exists(path):
            return join_df.task_id
        else:
            return re_partition.task_id

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    get_data = BashOperator(
            task_id = "get.data",
            bash_command="""
                echo "get.data"
            """
            )
    pars_parq = BashOperator(
            task_id = "parsing.parquet",
            bash_command="""
                echo "parsing.parquet"
            """
            )
    select_parq = BashOperator(
            task_id = "select.parquet",
            bash_command="""
                echo "select.parquet"
            """
            )

    start >> get_data >> pars_parq >> select_parq >> end
