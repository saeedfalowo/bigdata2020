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
    ssc = StreamingContext(sc, 10) # 5 seconds window
        
    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})
    
    data = kvs.map(lambda x: x[1])
    
    def readRdd4rmKafkaStream(readRDD):
        if not readRDD.isEmpty():
            
            #print(type(readRDD.collect()[0]))
            
            # Convert json data to dict
            data_dict = json.loads(readRDD.collect()[0])
            #data_dict = data_dict.decode('utf-8')
            #print(data_dict["album"]["id"].decode('utf-8'))

            album_artists = data_dict["album"]["artists"]
            album_artists_list = [artist["name"] for artist in album_artists]

            track_artists = data_dict["artists"]
            track_artists_list = [artist["name"] for artist in album_artists]
            
            filtered_data = {
                "album_id": data_dict["album"]["id"],
                "album_artists": album_artists_list,
                "album_name": data_dict["album"]["name"],
                "album_total_tracks": data_dict["album"]["total_tracks"],
                "album_release_date": data_dict["album"]["release_date"],

                "track_name": data_dict["name"],
                "track_id": data_dict["id"],
                "track_artists": track_artists_list,
                "track_duration": data_dict["duration_ms"],
                "track_number": data_dict["track_number"],
                "track_popularity": data_dict["popularity"],
                "track_explicity": data_dict["explicit"]
            }
            
            json_rdd = sc.parallelize([json.dumps(filtered_data)])
            # Put RDD into a dataframe
            #df = sqlContext.read.json(readRDD)
            df = sqlContext.read.json(json_rdd)
            df.show()
    
    data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    #filtered_data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()
    
# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ~/Documents/spark_project/src/spotify_api/spark_kafka_spotifyAPI_json.py localhost:9093 spotify-topic
