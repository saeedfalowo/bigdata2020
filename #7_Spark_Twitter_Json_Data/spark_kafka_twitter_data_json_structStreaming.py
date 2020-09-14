import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import re

spark = SparkSession.builder\
    .appName("SparkStructuredStreaming")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#sc = SparkContext(appName="SparkStructuredStreaming")
#sc = spark.sparkContext
#sqlContext = SQLContext(sc)

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "zoo-lion") \
    .load() \
    .select("value") \
    .selectExpr("CAST(value AS STRING)") #\
    #.select(F.from_json(F.col("value").cast("string"),schema))


def extract_json_row(row):
    
    row_dict = row.asDict()
    row_val_json = json.dumps(row_dict['value'])
    
    #twitter_df = spark.read.load(sc.parallelize([row_val_json]))
    #twitter_df.show()
    #print(row_dict['value'])    
    print("\n\nHellow human\n\n")

def extract_json_df(df, epochId):
    schema = StructType([
        StructField("created_at", StringType()),
        StructField("name", StringType()),
        StructField("screen_name", StringType()),
        StructField("full_text", StringType()),
        StructField("retweet_cnt", StringType()),
        StructField("favorite_cnt", StringType()),
        StructField("hash_tags", StringType())
    ])
    
    twitter_df = df.withColumn("value", F.from_json("value", schema))\
        .select(F.col('value.*'))
    
    twitter_df.show()
    
#query = df.writeStream.format('console').start()
#query = df.writeStream.format('console').foreach(extract_json_row).start()
query = df.writeStream.format('console').foreachBatch(extract_json_df).start()
query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 spark_kafka_twitter_data_json_structStreaming.py localhost:9093

"""
REF:
https://docs.databricks.com/spark/latest/structured-streaming/examples.html
https://docs.databricks.com/spark/latest/structured-streaming/foreach.html
https://stackoverflow.com/questions/57706678/make-json-in-sparks-structured-streaming-accessible-in-python-pyspark-as-data
"""