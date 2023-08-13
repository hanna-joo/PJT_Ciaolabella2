import time
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json, struct, col, when, floor, date_format, count, min, lit

click_schema = StructType([
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
						StructField('product_name', StringType(), True),
						StructField('product_volume', StringType(), True),
						StructField('product_unitprice', StringType(), True),
            StructField('productclick_time', TimestampType(), True)
        ]), True)
    ]), True),
])

utc_unix_time = time.time()
start_uut_ms = (utc_unix_time - 29 * 60 * 60) * 1000
end_uut_ms = (utc_unix_time - 5 * 60 * 60) * 1000

kafka_df = spark\
    .read\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "218.154.53.236:9091, 218.154.53.236:9092, 218.154.53.236:9093, 218.154.53.236:9094")\
    .option("assign", """{"log_nolabel":[1]}""")\
    .option("startingOffsetsByTimestamp", """{{"log_nolabel":{{"1": {}}}}}""".format(start_uut_ms, start_uut_ms))\
    .option("endingOffsetsByTimestamp", """{{"log_nolabel":{{"1": {}}}}}""".format(end_uut_ms, end_uut_ms))\
    .load()\
		.select(col('key').cast(StringType()), col('value').cast(StringType()))\
    .withColumn("value", when(col('key') == 'click', 
								from_json(col("value"), click_schema)))\
		.select(col('value.fields.fields.*'))\
		.withColumn('price', col('product_unitprice').cast(FloatType()))\
		.withColumn('date', date_format('productclick_time', 'yyyyMMdd'))\
		.withColumn('user_age', when(col('user_age').cast(IntegerType())>=60, 60).otherwise(10*floor(col('user_age')/10)))\
		.drop('member_id', 'user_region', 'productclick_time', 'product_unitprice')\
		.na.fill(99, ['user_age'])\
		.groupBy('date', 'product_name', 'product_volume', 'user_age', 'user_gender')\
		.agg(min('price').alias('price_min'), count("*").alias('count'))\
		.orderBy('date', 'product_name', 'product_volume', 'user_age', 'user_gender')\

kafka_df.select(to_json(struct('*')).alias('value'))\
		.withColumn('partition', lit(0))\
		.write\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 
						"218.154.53.236:9091, 218.154.53.236:9092, 218.154.53.236:9093, 218.154.53.236:9094")\
		.option("topic", "log_aggregation")\
    .save()