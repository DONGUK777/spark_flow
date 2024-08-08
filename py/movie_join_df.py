from pyspark.sql import SparkSession
import sys
LOAD_DT = sys.argv[1]

spark = SparkSession.builder.appName("Join_df").getOrCreate()

df1 = spark.read.parquet(f"/home/tommy/data/movie_data/repartition/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("one_day")

df2 = spark.sql(f"""
SELECT
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    -- multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM one_day
WHERE multiMovieYn IS NULL
""")

df2.createOrReplaceTempView("multi_null")

df3 = spark.sql(f"""
SELECT
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    -- repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM one_day
WHERE repNationCd IS NULL
""")

df3.createOrReplaceTempView("nation_null")

df_j = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.salesAmt, n.salesAmt) AS salesAmt, -- 매출액
    COALESCE(m.audiCnt, n.audiCnt) AS audiCnt, -- 관객수
    COALESCE(m.showCnt, n.showCnt) AS showCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM multi_null m FULL OUTER JOIN nation_null n
ON m.movieCd = n.movieCd""")

df_j.createOrReplaceTempView("join_df")

df_j.write.mode('append').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/tommy/data/movie_data/hive")
spark.stop()
