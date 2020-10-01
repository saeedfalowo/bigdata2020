import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import re

#reload(sys)
#sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    sc = SparkContext(appName="SparkToHiveStreaming")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5) # 5 seconds window

    spark = SparkSession.builder\
        .appName("SparkToHiveStreaming")\
        .config("spark.sql.warehouse.dir","/usr/hive/warehouse")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris","thrift://localhost:9083")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("DROP TABLE IF EXISTS plants_data_table")

    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})

    data = kvs.map(lambda x: x[1])
    
    def extract_df_cols(df):
        data_cols = ['common_name', 'scientific_name', 'image_url', 'family_common_name',
                     'vegetable', 'main_species']
        df_data = df.select(data_cols)
        df_data = df_data.select('*', F.col("image_url").alias("plant_image_url")).drop('image_url')
        #df_data.show()
        #print('\n')

        df_all = df_data.select('*').drop('main_species')

        # EXPLODE MAIN_SPECIES COLUMN
        df_main_species = df_data.select('main_species.*')      
        data_main_species_cols = ['rank', 'genus', 'family', 'duration', 'edible_part', 'edible',
                    'images', 'common_names', 'distribution', 'flower', 'foliage', 'fruit_or_seed',
                    'specifications', 'growth']
        cols_df = df_main_species.columns
        actual_cols = []
        [actual_cols.append(cols_df[i]) for i in range(len(cols_df)) if cols_df[i] in data_main_species_cols]

        df_main_species = df_main_species.select(actual_cols)
        #df_main_species.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species)
        df_all = df_all.drop('images','common_names','distribution','flower',
                             'foliage','fruit_or_seed','specifications','growth')


        # EXPLODE MAIN_SPECIES_IMAGES COLUMN
        df_main_species_imgs = df_main_species.select('images.*')        
        data_main_species_images_cols = ['flower', 'leaf', 'habit', 'fruit']
        df_main_species_imgs = df_main_species_imgs.select(data_main_species_images_cols)        
        #df_main_species_imgs.show()
        #print('\n')


        # EXTRACT FRUIT IMAGES
        try:
            df_main_species_imgs_fruit = df_main_species_imgs.select(F.col('fruit.image_url').alias('fruit_image_url'))
        except Exception:
            df_main_species_imgs_fruit = df_main_species_imgs.select(F.lit('null').alias("fruit_image_url"))
        #df_main_species_imgs_fruit.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_imgs_fruit)


        # EXTRACT FLOWER IMAGES
        try:
            df_main_species_imgs_flower = df_main_species_imgs.select(F.col('flower.image_url').alias('flower_image_url'))  
        except Exception:
            df_main_species_imgs_flower = df_main_species_imgs.select(F.lit('null').alias("flower_image_url"))
        #df_main_species_imgs_flower.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_imgs_flower)


        # EXTRACT HABIT IMAGES
        try:
            df_main_species_imgs_habit = df_main_species_imgs.select(F.col('habit.image_url').alias('habit_image_url'))  
        except Exception:
            df_main_species_imgs_habit = df_main_species_imgs.select(F.lit('null').alias("habit_image_url"))
        #df_main_species_imgs_habit.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_imgs_habit)


        # EXTRACT LEAF IMAGES
        try:
            df_main_species_imgs_leaf = df_main_species_imgs.select(F.col('leaf.image_url').alias('leaf_image_url'))   
        except Exception:
            df_main_species_imgs_leaf = df_main_species_imgs.select(F.lit('null').alias("leaf_image_url"))
        #df_main_species_imgs_leaf.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_imgs_leaf)


        # EXPLODE MAIN_SPECIES_COMMON_NAMES
        df_main_species_common_names = df_main_species.select('common_names.*')
        if 'en' in df_main_species_common_names.columns:
            data_main_species_common_names = ['en']
            df_main_species_common_names = df_main_species_common_names.select(data_main_species_common_names) 
        elif 'eng' in df_main_species_common_names.columns:
            #data_main_species_common_names = ['eng']
            df_main_species_common_names = df_main_species_common_names.select(F.col('eng').alias('en'))
        else:
            df_main_species_common_names = df_main_species_common_names.select(F.lit('null').alias("en")) 

        #df_main_species_common_names.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_common_names)


        # EXPLODE MAIN_SPECIES_DISTRIBUTION
        if 'distribution' in df_main_species.columns:
            df_main_species_distribution = df_main_species.select('distribution.*') 
            data_main_species_distribution = ['native','introduced']
            #df_main_species_distribution = df_main_species_distribution.select(data_main_species_distribution)
            try:
                df_main_species_distribution = df_main_species_distribution.select(data_main_species_distribution)    
            except Exception:
                df_main_species_distribution = df_main_species_distribution.select('native',F.lit(F.array('null')).alias("introduced"))
        else:
            df_main_species_distribution = df_main_species.select(F.lit('[null]').alias("native"),F.lit('[null]').alias("introduced"))
        #df_main_species_distribution.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_distribution)


        # EXPLODE MAIN_SPECIES_FLOWER
        df_main_species_flower = df_main_species.select('flower.*') 
        data_main_species_flower = [F.col('color').alias('flower_color'),
                                    F.col('conspicuous').alias('flower_conspicuous')]
        df_main_species_flower = df_main_species_flower.select(data_main_species_flower)    
        #df_main_species_flower.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_flower)


        # EXPLODE MAIN_SPECIES_FOLIAGE
        df_main_species_foliage = df_main_species.select('foliage.*') 
        data_main_species_foliage = [F.col('color').alias('foliage_color')]
        df_main_species_foliage = df_main_species_foliage.select(data_main_species_foliage)    
        #df_main_species_foliage.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_foliage)


        # EXPLODE MAIN_SPECIES_FRUIT_OR_SEED
        df_main_species_fruit_or_seed = df_main_species.select('fruit_or_seed.*') 
        data_main_species_fruit_or_seed = [F.col('color').alias('fruit_or_seed_color'),
                                           'seed_persistence']
        df_main_species_fruit_or_seed = df_main_species_fruit_or_seed.select(data_main_species_fruit_or_seed)    
        #df_main_species_fruit_or_seed.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_fruit_or_seed)


        # EXPLODE MAIN_SPECIES_SPECIFICATIONS
        df_main_species_specifications = df_main_species.select('specifications.*') 
        data_main_species_specifications = ['ligneous_type', 'growth_form', 'growth_habit', 'growth_rate',
                                            F.col('average_height.cm').alias('average_height_cm'),
                                            F.col('maximum_height.cm').alias('maximum_height_cm'), 'nitrogen_fixation',
                                            'shape_and_orientation', 'toxicity']
        df_main_species_specifications = df_main_species_specifications.select(data_main_species_specifications)
        #df_main_species_specifications.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_specifications)


        # EXPLODE MAIN_SPECIES_GROWTH
        df_main_species_growth = df_main_species.select('growth.*') 
        data_main_species_growth = ['description', 'sowing', 'days_to_harvest', 'row_spacing',
                                    'ph_maximum', 'ph_minimum', 'atmospheric_humidity',
                                    'growth_months', 'bloom_months', 'fruit_months',
                                    F.col('minimum_precipitation.mm').alias('minimum_precipitation_mm'),
                                    F.col('maximum_precipitation.mm').alias('maximum_precipitation_mm'),
                                    F.col('minimum_root_depth.cm').alias('minimum_root_depth_cm'),
                                    F.col('minimum_temperature.deg_c').alias('minimum_temperature_deg_c'),
                                    F.col('maximum_temperature.deg_c').alias('maximum_temperature_deg_c'),
                                    'soil_nutriments', 'soil_salinity', 'soil_texture', 'soil_humidity']
        df_main_species_growth = df_main_species_growth.select(data_main_species_growth)
        #df_main_species_growth.show()
        #print('\n')

        df_all = df_all.crossJoin(df_main_species_growth)
        
        return df_all

    id = 1
    cnt = 0
    
    def readRdd4rmKafkaStream(readRDD):
        global id, cnt
        
        if not readRDD.isEmpty():
            
            # Put RDD into a dataframe
            df = sqlContext.read.json(readRDD)
            
            try:
                df_all = extract_df_cols(df)                
                df_all = df_all.select(F.lit(id).alias("id"), "*")

                if id == 1:
                    df_all.write.mode("overwrite")\
                        .saveAsTable("plants_db.plants_data_table")
                else:
                    df_all.write.mode("append")\
                        .saveAsTable("plants_db.plants_data_table")

                df_all.show()

                id += 1
            except Exception:
                cnt += 1
                print(cnt, ": Skipping...")
    
    data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    #filtered_data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()
    
# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark_kafka_Trefle_API_json_to_hive.py localhost:9093 plants-topic