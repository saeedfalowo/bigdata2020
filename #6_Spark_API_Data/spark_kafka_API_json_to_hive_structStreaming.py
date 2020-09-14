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
    .appName("SparkToHiveStructuredStreaming")\
    .config("spark.sql.warehouse.dir","/usr/hive/warehouse")\
    .config("spark.sql.catalogImplementation","hive")\
    .config("hive.metastore.uris","thrift://localhost:9083")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sql("DROP TABLE IF EXISTS Businesses_data_struct")
cnt = 0

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
    print(row_val_json)
    #twitter_df = spark.read.load(sc.parallelize([row_val_json]))
    #twitter_df.show()
    #print(row_dict['value'])    
    print("\n\nHellow human\n\n")

def extract_json_df(df, epochId):
    
    global cnt
    row_list = df.select('value').collect()
    if row_list:
        #print("Not Empty")
        row_json = row_list[0].asDict()["value"]
        row_dict = json.loads(row_json)
        row_dict_keys_list = list(row_dict.keys())
        #print(row_dict_keys_list)
        
        schema_list = []
        [schema_list.append(StructField(key, StringType())) for key in row_dict_keys_list]
        schema = StructType(schema_list)
        
        #print(schema)
        
        yelp_df = df.withColumn("value", F.from_json("value", schema)).select(F.col('value.*'))
        #yelp_df.show()
        
        interested_cols = ['name', 'alias', 'categories', 'location', 'price', 'rating', 'review_count', 'transactions']
        
        #check_cols = all(col in yelp_df.columns for col in interested_cols)
        check_cols = all(col in row_dict_keys_list for col in interested_cols)
        if check_cols:
            Businesses_data_df = yelp_df.select(interested_cols)
            Businesses_data_df.show()
            
            # append to df to existing table if it
            Businesses_data_df.registerTempTable("Businesses_data_struct")
            print("cnt: ", cnt)
            if cnt < 1:
                Businesses_data_df.write.mode("overwrite")\
                    .saveAsTable("yelp_data.Businesses_data_struct")
                cnt+=1
            else:
                Businesses_data_df.write.mode("append")\
                    .saveAsTable("yelp_data.Businesses_data_struct")

                # saves df as Parquet by default 
        
        
    else:
        print("Is Empty")
    
#query = df.writeStream.format('console').start()
#query = df.writeStream.format('console').foreach(extract_json_row).start()
query = df.writeStream.format('console').foreachBatch(extract_json_df).start()
query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 spark_kafka_API_json_to_hive_structStreaming.py localhost:9093

"""
REF:
https://docs.databricks.com/spark/latest/structured-streaming/examples.html
https://docs.databricks.com/spark/latest/structured-streaming/foreach.html
https://stackoverflow.com/questions/57706678/make-json-in-sparks-structured-streaming-accessible-in-python-pyspark-as-data
"""

# interested_cols = ['name', 'alias', 'categories', 'location', 'price', 'rating', 'review_count', 'transactions']
