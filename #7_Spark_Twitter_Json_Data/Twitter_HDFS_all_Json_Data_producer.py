from kafka import KafkaProducer
from time import sleep
from hdfs import InsecureClient
import json

HDFS_HOSTNAME = "master"
HDFS_PORT = '50070'
HDFS_CONNECTION_str = 'http://' + HDFS_HOSTNAME + ':' + HDFS_PORT

hdfs_client = InsecureClient(HDFS_CONNECTION_str)
file_path = "/home/saeed/flume/tweets/FlumeData.1599698795303"

with hdfs_client.read(file_path) as read_hdfs:
    content = read_hdfs.read()

twitter_data_json = content.decode('utf-8')
twitter_data_json = twitter_data_json.split('\n')

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         api_version=(0,10,1))

for msg in twitter_data_json:
    #msg = json.loads(msg)
    #producer.send('zoo-lion',json.dumps(msg).encode('utf-8'))
    producer.send('zoo-lion',msg.encode('utf-8'))
    sleep(5)