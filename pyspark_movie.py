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
##    def get_data(ds_nodash):
#        from mov.api.call import save2df
#        df = save2df(ds_nodash)
#        print(df.head(5))

    def fun_multi_y(ds_nodash, url_param):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)
        
        print(df[['movieCd', 'movieNm']].head(5))

        for k, v in url_param.items():
            df[k] = v

        #p_cols = list(url_param.keys()).insert(0, 'load_dt')
        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet',
                partition_cols=p_cols
                # partition_cols=['load_dt', 'movieKey']
        )


    def repartition(ds_nodash):
        print("*" * 33)
        print(ds_nodash)
        print("*" * 33)

    re_task = PythonVirtualenvOperator(
        task_id = "re.partition",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
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

        task_id='echo.task',
        bash_command="echo 'task'"
    )


#    task_err = BashOperator(
#        task_id="err.report",
#        bash_command="""
#        """,
#        trigger_rule="one_failed"
#    )


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    get_start = EmptyOperator(
            task_id='get.start',
            trigger_rule="all_done"
    )
    
#    task_get = PythonVirtualenvOperator(
#            task_id='get_data',
#            python_callable=get_data,
#            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
#            system_site_packages=False,
#            #venv_cache_path="/home/tommy/tmp/airflow_venv/get_data"
#        )

    with TaskGroup('processing_tasks', dag=dag) as process_group:
        multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"multiMovieYn": "Y"}}
        )
        multi_n = PythonVirtualenvOperator(
            task_id='multi_n',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"multiMovieYn": "N"}}
        ) 
        nation_k = PythonVirtualenvOperator(
            task_id='nation_k',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"repNationCd": "K"}}
        )
        nation_f = PythonVirtualenvOperator(
            task_id='nation_f',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"repNationCd": "F"}}
        )
    
    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"
    )
    
    # task_start >> branch_op >> rm_dir 
    # rm_dir >> [task_get, multi_y, multi_n, nation_k, nation_f]
    # task_start >> task_join >> task_save
    # branch_op >> [task_get, multi_y, multi_n, nation_k, nation_f]
    # branch_op >> echo_task >> task_save
    
    # [task_get, multi_y, multi_n, nation_k, nation_f] >> task_save >> task_end
    
    task_start >> [branch_op, throw_err]
    branch_op >> [rm_dir, echo_task]
    branch_op >> get_start
    rm_dir >> get_start
    throw_err >> task_save
    get_start >> process_group
    # get_start >> [task_get, multi_y, multi_n, nation_k, nation_f]
    process_group >> get_end >> task_save >> task_end
    # [task_get, multi_y, multi_n, nation_k, nation_f] >> get_end >> task_save >> task_end
   
