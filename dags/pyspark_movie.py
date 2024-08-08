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
    end_date=datetime(2015, 1, 3),
    catchup=True,
    tags=['spark', 'movie', 'ant'],
) as dag:
  # t1, t2 and t3 are examples of tasks created by instantiating operators


    def re_partition(ds_nodash):
        from airflow_spark.re import re_partition
        df_row_cnt, read_path, write_path= re_partition(ds_nodash)
        print(f'df_row_cnt:{df_row_cnt}')
        print(f'read_path:{read_path}')
        print(f'write_path:{write_path}')

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/data/movie_data/repartition/load_dt={ds_nodash}'
        # 이미 파일이 있다면 join_df로
        if os.path.exists(path):
            return join_df.task_id
        # 파일이 없다면 re_partition로
        else:
            return re_partition.task_id


    re_task = PythonOperator(
        task_id = "re.partition",
        python_callable=re_partition,
        trigger_rule="one_success"
    )
    

##    join_df = BashOperator(
#        task_id='join.df',
#        bash_command='''
#            $SPARK_HOME/bin/spark-submit /home/tommy/code/spyspark/simpleApp.py {{ds_nodash}}
#        '''
#    )

    join_df = BashOperator(
        task_id='join.df',
        bash_command='''
            $SPARK_HOME/bin/spark-submit /home/tommy/code/airflow_spark/py/movie_join_df.py {{ds_nodash}}
        '''
    )

    agg_df = BashOperator(
        task_id='agg.df',
        bash_command='''
            $SPARK_HOME/bin/spark-submit /home/tommy/code/airflow_spark/py/movie_sum_df.py {{ds_nodash}}
        '''
    )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    #flow
    start >> re_task >> join_df >> agg_df >> end
