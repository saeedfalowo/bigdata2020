import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

if __name__ == "__main__":
    sc = SparkContext(appName="SparkStreaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5) # 5 seconds window
    
    broker, topic = sys.argv[1:]
    """kvs = KafkaUtils.createStream(ssc,
                                  broker,
                                  "raw-event-streaming-consumer",
                                  {topic:1})"""
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})
    
    #lines = sc.textFile("file:///home/saeed/Documents/spark_project//src/Shakespeare.txt")
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" "))\
                        .map(lambda word: (word, 1))\
                        .reduceByKey(lambda a, b: a+b)
    
    counts.pprint()
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()