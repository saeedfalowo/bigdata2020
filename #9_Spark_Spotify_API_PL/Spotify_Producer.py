from kafka import KafkaProducer
from time import sleep
import json

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         api_version=(0,10,1))

f = open("spotify_data.json","r")
spotify_json_data = f.read()
spotify_data_dict_list = json.loads(spotify_json_data)

for response in spotify_data_dict_list:
    for track in response["tracks"]["items"]:
        producer.send('spotify-topic',json.dumps(track).encode('utf-8'))
        sleep(5)