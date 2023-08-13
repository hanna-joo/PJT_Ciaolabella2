from pyspark.sql.functions import col, when, floor, window, concat, lit
from pyspark.sql.functions import from_json, dayofmonth, hour, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


spark_conf = SparkConf().set("spark.redis.host", "218.154.53.236").set("spark.redis.port", "7302").set("spark.redis.auth", "xpxmfltm1019")
sc = SparkContext(conf = spark_conf)
spark = SparkSession.builder.getOrCreate()


# kafka에서 'log_nolabel' 토픽 스트림 가져오기
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "218.154.53.236:9091, 218.154.53.236:9092, 218.154.53.236:9093, 218.154.53.236:9094") \
    .option("subscribe", "log_nolabel") \
    .option('startingOffsets', 'latest')\
    .load()
#df.selectExpr("CAST(key AS STRING)")


# 필요한 schema 지정
search_schema = StructType([
    StructField('messageType', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('fields', StructType([
        StructField('event', StringType(), True),
        StructField('topic', StringType(), True),
        StructField('key', StringType(), True),
        StructField('fields', StructType([
            StructField('member_id', StringType(), True),
            StructField('user_gender', StringType(), True),
            StructField('user_age', StringType(), True),
            StructField('user_region', StringType(), True),
            StructField('search_word', StringType(), True),
            StructField('searchclick_time', StringType(), True)
        ]), True)
    ]), True),
])


# 스트림데이터(dataframe) 가공
# Row(timestamp, 성별, 나이대, 검색어) 형태로
df = df.select(col('value').cast(StringType()))\
    .withColumn('value', from_json(col('value'), search_schema))\
    .filter(col('value.fields.key')=='search')\
    .select(
        col('value.timestamp').alias('ts'),
        when(col('value.fields.fields.user_gender')=='none','un').otherwise(col('value.fields.fields.user_gender')).alias('gender'),
        when(col('value.fields.fields.user_age')=='none', 99).when(col('value.fields.fields.user_age')>=60, 60).otherwise(10*floor(col('value.fields.fields.user_age')/10)).alias('age'),
        col('value.fields.fields.search_word').alias('word')
    )


# window 지정 후 count
# withWatermark(기준 컬럼, 허용 지연 시간)
# window(기준시간, window크기, 다음 window 얼마 후 열리는지, window시작시각)
window_df = df.withWatermark("ts", "30 seconds") \
    .groupBy(
        window(df.ts, "1440 minutes"),
        df.gender, df.age, df.word) \
    .count()


# sink to redis
def foreach_batch_function(df, epoch_id):
    df.write\
    .format("org.apache.spark.sql.redis")\
    .mode("append")\
    .option("table", "search")\
    .option("key.column", "key")\
    .option("encode", "utf-8")\
    .save()

query = window_df.withColumn('key', concat(regexp_replace(col('window').cast(StringType())[2:10], '-', ''), lit(':'), col('gender'), lit(':'), col('age').cast(StringType()), lit(':'), col('word')))\
       .drop('window', 'gender', 'age', 'word')\
       .writeStream.outputMode("update").foreachBatch(foreach_batch_function).trigger(processingTime='1 minutes').start()
query.awaitTermination()