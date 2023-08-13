from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import datetime
import time
import os
# spark_version = '3.2.2'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--master yarn --packages org.apache.kafka:kafka_2.12:3.2.0, org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3'
# Kafka Schema
kafka_fields = StructType([
    StructField('messageType', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('fields', StructType([
        StructField('event', StringType(), True),
        StructField('topic', StringType(), True),
        StructField('key', StringType(), True),
        StructField("fields", StructType([
            StructField("user_age",StringType(),True),
            StructField("user_gender",StringType(),True),
            StructField("user_region",StringType(),True),

            StructField("save_point",IntegerType(),True),
            StructField("login_time",StringType(),True),
            StructField("logout_time",StringType(),True),
            StructField("searchclick_location",StringType(),True),
            
            StructField("selected_menu",StringType(),True),
            StructField("search_word",StringType(),True),
            StructField("product_name",StringType(),True),
        ]), True)
    ]), True),
])



# Schema (최종 
# Session 생성
spark = SparkSession.builder.appName('ciao_logs').getOrCreate()

# Kafka 컨슈밍 객체 생성
start_offset= """{"log_ecopoint":{"0":-1,"1":-1},"log_inout":{"0":-1,"1":-1},"log_lesswaste:{"0":-1},"log_menuclick":{"0":-1},"log_nolabel":{"0":-1,"1":-1}}"""
kafka_stream_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "218.154.53.236:9091, 218.154.53.236:9092, 218.154.53.236:9093,218.154.53.236:9094")\
    .option('subscribe','log_inout, log_menuclick, log_ecopoint, log_lesswaste,log_nolabel')\
    .option('startingOffsets',"earliest")\
    .load()
# , log_menuclick, log_ecopoint, log_lesswaste,log_nolabel
# root
#  |-- key: binary (nullable = true) 
#  |-- value: binary (nullable = true) 이거 살리고
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true) 이거 살리고
#  |-- timestampType: integer (nullable = true)

select_key_stream = kafka_stream_df.select(col("timestamp"), col('key').cast(StringType()),col('value').cast(StringType()))\
    .withColumn("kafka_datas", from_json(col("value"), kafka_fields))\
    .withColumn("user_ages", when(col("kafka_datas.fields.fields.user_age")=="none", -1).otherwise(col("kafka_datas.fields.fields.user_age").cast(IntegerType())))\
    .select(
        "timestamp",
        hour("timestamp").alias("hours"),
        "key",
        when((col('user_ages'))>=60, 60).otherwise(10*floor(col('user_ages')/10)).alias('user_age'),
        "kafka_datas.fields.fields.user_gender", 
        "kafka_datas.fields.fields.user_region", 
        "kafka_datas.fields.fields.save_point", 
        "kafka_datas.fields.fields.selected_menu", 
        "kafka_datas.fields.fields.search_word", 
        "kafka_datas.fields.fields.product_name"
    )
    # .orderBy(col("time"), col("user_age"))
select_key_stream.printSchema()

# root
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestamp2: integer (nullable = true)
#  |-- key: string (nullable = true)
#  |-- user_age: long (nullable = true)
#  |-- user_gender: string (nullable = true)
#  |-- user_region: string (nullable = true)
#  |-- save_point: integer (nullable = true)
#  |-- selected_menu: string (nullable = true)
#  |-- search_word: string (nullable = true)
#  |-- product_name: string (nullable = true)

window_df = select_key_stream.groupBy(window("timestamp", "60 minutes").alias("window_time"),"hours", "user_age","user_region", "user_gender")\
    .agg(
        sum(when(col("key")=="login",1).otherwise(0)).alias("login"),
         sum(when(col("key")=="logout",1).otherwise(0)).alias("logout"),
         sum(when(col("selected_menu")=="nolabel",1).otherwise(0)).alias("menu_nolabel"),
         sum(when(col("selected_menu")=="ecopoint1",1).otherwise(0)).alias("menu_eco1"),
         sum(when(col("selected_menu")=="ecopoint2",1).otherwise(0)).alias("menu_eco2"),
         sum(when(col("selected_menu")=="lesswaste",1).otherwise(0)).alias("menu_lesswaste"),
         sum(when(col("key")=="1",1).otherwise(0)).alias("ecopoint1_click"),
         sum(when(col("key")=="2",1).otherwise(0)).alias("ecopoint2_click"),
         sum(when(col("key")=="lesswaste",1).otherwise(0)).alias("lesswaste_click"),
         sum(when(col("key")=="search",1).otherwise(0)).alias("nolabel_search"),
         sum(when(col("key")=="click",1).otherwise(0)).alias("nolabel_click"),
         sum(when(col("key")== "1", col("save_point")).otherwise(0)).alias("save_ecopoint1"),
         sum(when(col("key")== "2", col("save_point")).otherwise(0)).alias("save_ecopoint2"),
         )\
    .orderBy("window_time", "hours","user_age","user_gender", "user_region")

def foreach_batch_function(df, epoch_id):
    df.write\
    .format("org.apache.spark.sql.redis")\
    .mode("append")\
    .option("table", "common")\
    .option("key.column", "key")\
    .option("encode", "utf-8")\
    .save()

query = window_df.withColumn('key', concat(regexp_replace(col('window_time').cast(StringType())[2:10], '-', ''), lit(':'), col('user_gender'), lit(':'), col('user_age').cast(StringType()), lit(':'), col('user_region')))\
        .drop('window_time','user_age', 'user_region','user_gender')\
       .writeStream.outputMode("update").foreachBatch(foreach_batch_function).trigger(processingTime='5 minutes').option('truncate', 'false').start()
query.awaitTermination()