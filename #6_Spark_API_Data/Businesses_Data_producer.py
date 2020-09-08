from kafka import KafkaProducer
from time import sleep
import json

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         api_version=(0,10,1))

f = open('Businesses_London_0-1000_offset.json','r')
json_data = f.read()
response_list = json.loads(json_data)

businesses_data_list = []
for response_data in response_list:
    for business_data in response_data["businesses"]:
        businesses_data_list.append(business_data)

for msg in businesses_data_list:
    producer.send('zoo-lion',json.dumps(msg).encode('utf-8'))
    #producer.send('zoo-lion',msg.encode('utf-8'))
    sleep(5)