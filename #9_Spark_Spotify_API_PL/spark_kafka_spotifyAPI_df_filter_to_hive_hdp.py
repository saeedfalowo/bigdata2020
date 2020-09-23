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

    #spark.sql("DROP TABLE IF EXISTS spotify_db.music_hive_table")
    spark.sql("DROP TABLE IF EXISTS music_hive_table")

    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})

    data = kvs.map(lambda x: x[1])

    id = 1
    cnt = 0
    def readRdd4rmKafkaStream(readRDD):
        global cnt, id
        if not readRDD.isEmpty():
            # Put RDD into a dataframe
            df = sqlContext.read.json(readRDD)

            all_detail_df = df.select(
                # TRACK INFO DETAIL
                F.col("disc_number").alias("track_disc_num"),
                F.col("duration_ms").alias("track_duration_ms"),
                F.col("explicit").alias("track_is_explicit"),
                F.col("external_ids").alias("track_ext_id"),
                F.col("href").alias("track_href"),
                F.col("id").alias("track_id"),
                F.col("name").alias("track_name"),
                F.col("popularity").alias("track_popularity"),
                F.col("track_number"),
                F.col("uri").alias("track_uri"),

                # ALBUM INFO DETAIL
                #F.explode(F.array("album")).alias("album"),
                #F.col("album.album_type").alias("album_type"),
                F.col("album.id").alias("album_id"),
                F.col("album.name").alias("album_name"),
                F.col("album.release_date").alias("album_release"),
                F.col("album.total_tracks").alias("album_total_tracks"),

                # ARTIST INFO DETAIL
                F.col("artists.id").alias("artists_id"),
                F.col("artists.name").alias("artists_name"),
                F.col("artists.uri").alias("artists_uri")

            )
            #all_detail_df = all_detail_df.withColumn("id",F.lit(id))
            all_detail_df = all_detail_df.select(F.lit(id).alias("id"), "*")
            id += 1
            all_detail_df.show()
            all_detail_df.registerTempTable("music_hive_table")

            if cnt < 1:
                 all_detail_df.write.mode("overwrite")\
                     .saveAsTable("spotify_db.music_hive_table")
                 cnt+=1
            else:
                 all_detail_df.write.mode("append")\
                     .saveAsTable("spotify_db.music_hive_table")

                 # saves df as Parquet by default


    data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 spark_kafka_spotifyAPI_df_filter_to_hive_hdp.py sandbox-hdp.hortonworks.com:6667 spotify-topic-2
