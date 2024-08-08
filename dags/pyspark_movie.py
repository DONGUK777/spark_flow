from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from pprint import pprint
from airflow.utils.task_group import TaskGroup

with DAG(
    'pyspark_movie' ,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past':False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_tasks= 3,
    max_active_runs= 1,
    description='hello world DAG',
    # schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2025, 3, 3),
    catchup=True,
    tags=['spark', 'movie', 'ant'],
) as dag:
  # t1, t2 and t3 are examples of tasks created by instantiating operators


    def repartition(ds_nodash):
        print("*" * 33)
        print(ds_nodash)
        print("*" * 33)

    re_task = PythonVirtualenvOperator(
        task_id = "re.partition",
        python_callable=repartition,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/DONGUK777/spark_flow.git@0.2.0/airflowdag"],
    )
    

    join_df = BashOperator(
        task_id='join.df',
        bash_command='''
            echo "spark-submit..."
            echo "{{ds_nodash}}"
        ''',
    )

    agg_df = BashOperator(
        task_id='agg.df',
        bash_command='''
            echo "spark-submit..."
            echo "{{ds_nodash}}"
        ''',
    )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    #flow
    start >> re_task >> join_df >> agg_df >> end
