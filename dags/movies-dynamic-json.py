from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, size, count
import os
import requests
import json
import time
from tqdm import tqdm

# 환경 변수에서 API 키를 가져옴
API_KEY = os.getenv('MOVIE_API_KEY')

# 데이터 저장을 위한 함수
def save_json(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# API 호출 함수
def req(url):
    r = requests.get(url)
    return r.json()

# 영화 데이터를 수집하고 JSON으로 저장하는 함수
def fetch_and_save_movies(ds_nodash, **kwargs):
    year = int(ds_nodash[:4])  # 실행 날짜에서 연도 추출
    file_path = f'data/movies/year={year}/data.json'

    url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}&itemPerPage=10"

    all_data = []
    for page in tqdm(range(1, 11)):  # 페이지 1~10에 대해서만 루프 실행
        time.sleep(1)  # API 호출 간에 딜레이를 줌
        r = req(url_base + f"&curPage={page}")
        d = r.get('movieListResult', {}).get('movieList', [])
        all_data.extend(d)

    save_json(all_data, file_path)
    return True

# PySpark 작업을 실행하는 함수
def parse_and_process_data(ds_nodash, **kwargs):
    # Spark 세션 생성
    spark = SparkSession.builder.appName("MoviesDataProcessing").getOrCreate()

    # JSON 파일 경로 설정
    json_file_path = f'/home/tommy/code/moviedata/data/movies/year={ds_nodash[:4]}/data.json'

    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON file not found at {json_file_path}")

    # JSON 파일을 읽어 DataFrame으로 변환
    jdf = spark.read.option("multiline", "true").json(json_file_path)

    # companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
    ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))


    # 회사 및 감독 정보 펼치기
    edf = ccdf.withColumn("company", explode_outer("companys"))
    eedf = edf.withColumn("director", explode_outer("directors"))

    # 결과를 Parquet 형식으로 저장
    output_path = f'/home/tommy/code/moviedata/processed/movies/year={ds_nodash[:4]}/data.parquet'
    eedf.write.mode("overwrite").parquet(output_path)

    return True

# 감독 및 회사별 집계 함수
def select_and_aggregate_data(ds_nodash, **kwargs):
    # Spark 세션 초기화
    spark = SparkSession.builder \
        .appName("Movie Data Aggregation") \
        .getOrCreate()

    # Parquet 파일 경로
    year = ds_nodash[:4]  # 실행 날짜에서 연도 추출
    parquet_file_path = f'/home/tommy/code/moviedata/processed/movies/year={year}/data.parquet'
    director_output_path = f'/home/tommy/code/moviedata/processed/movies/year={year}/director_aggregated_data.parquet'
    company_output_path = f'/home/tommy/code/moviedata/processed/movies/year={year}/company_aggregated_data.parquet'

    # Parquet 파일 읽기
    df = spark.read.parquet(parquet_file_path)

    # 감독 별 영화 수 집계
    director_agg_df = df.groupBy("director").agg(count("movieCd").alias("movie_count"))

    # 회사 별 영화 수 집계
    company_agg_df = df.groupBy("company").agg(count("movieCd").alias("movie_count"))

    # 결과 저장
    director_agg_df.write.mode('overwrite').parquet(director_output_path)
    company_agg_df.write.mode('overwrite').parquet(company_output_path)
    
    print("Company Aggregated Data:")
    company_agg_df.show()
    director_agg_df.show()
    return True

# DAG 정의
with DAG(
    'movies_dynamic_json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='A DAG to fetch and process movie data',
    schedule_interval="@once",  # 한 번만 실행되도록 설정
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016, 1, 1),
    catchup=True,
    tags=["movies", "dynamic", "json"],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    get_data = PythonOperator(
        task_id='get.data',
        python_callable=fetch_and_save_movies,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    pars_parq = PythonOperator(
        task_id='parsing.parquet',
        python_callable=parse_and_process_data,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    select_parq = PythonOperator(
        task_id='select.parquet',
        python_callable=select_and_aggregate_data,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    # 태스크 간의 의존성 설정
    start >> get_data >> pars_parq >> select_parq >> end
