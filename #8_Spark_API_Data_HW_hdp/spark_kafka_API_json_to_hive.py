import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import re

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    sc = SparkContext(appName="SparkToHiveStreaming")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5) # 5 seconds window

    spark = SparkSession.builder\
        .appName("SparkToHiveStreaming")\
        .config("spark.sql.warehouse.dir","/usr/hive/warehouse")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris","thrift://sandbox-hdp.hortonworks.com:9083")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("DROP TABLE IF EXISTS Businesses_data_all")
    spark.sql("DROP TABLE IF EXISTS Businesses_data")

    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})

    data = kvs.map(lambda x: x[1])

    cnt_all = 0
    cnt = 0
    def readRdd4rmKafkaStream(readRDD):
        global cnt, cnt_all
        if not readRDD.isEmpty():
            # Put RDD into a dataframe
            df = sqlContext.read.json(readRDD)
            df.registerTempTable("Businesses_data_all")

            interested_cols = ['name', 'alias', 'categories', 'location', 'price', 'rating', 'review_count', 'transactions']

            #if 'price' in df.columns:
            check_cols = all(col in df.columns for col in interested_cols)

            #print(check_cols)
            if check_cols:
                Businesses_data_df = sqlContext.sql(
                    "SELECT "+
                    ",".join(interested_cols)+
                    " FROM Businesses_data_all WHERE price IS NOT NULL")
                Businesses_data_df.show()

                # append to df to existing table if it
                Businesses_data_df.registerTempTable("Businesses_data")
                if cnt < 1:
                    df.write.mode("overwrite")\
                        .saveAsTable("yelp_data.Businesses_data")
                    cnt+=1
                else:
                    df.write.mode("append")\
                        .saveAsTable("yelp_data.Businesses_data")

                    # saves df as Parquet by default


    data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()

    # spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 spark_kafka_API_json_to_hive.py sandbox-hdp.hortonworks.com:6667 zoo-lion-2
