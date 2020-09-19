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
    sc = SparkContext(appName="SparkStreaming")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5) # 5 seconds window

    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})

    data = kvs.map(lambda x: x[1])

    def readRdd4rmKafkaStream(readRDD):
        if not readRDD.isEmpty():
            # Put RDD into a dataframe
            df = sqlContext.read.json(readRDD)
            df.registerTempTable("Businesses_data")
            interested_cols = ['name', 'alias', 'categories', 'location', 'price', 'rating', 'review_count', 'transactions']

            #if 'price' in df.columns:
            check_cols = all(col in df.columns for col in interested_cols)

            #print(check_cols)
            if check_cols:
                Businesses_data_df = sqlContext.sql(
                    "SELECT "+
                    ",".join(interested_cols)+
                    " FROM Businesses_data WHERE price IS NOT NULL")
                Businesses_data_df.show()

    data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()

    # spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 spark_kafka_API_json.py sandbox-hdp.hortonworks.com:6667 zoo-lion-2
    
