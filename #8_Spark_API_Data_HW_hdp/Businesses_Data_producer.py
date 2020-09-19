from kafka import KafkaProducer
from time import sleep
import json

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                         api_version=(0,10,1))

f = open('Businesses_London_0-1000_offset.json','r')
json_data = f.read()
response_list = json.loads(json_data)

businesses_data_list = []
for response_data in response_list:
    for business_data in response_data["businesses"]:
        businesses_data_list.append(business_data)

for msg in businesses_data_list:
    producer.send('zoo-lion-2',json.dumps(msg).encode('utf-8'))
    print("msg sent: ", msg)
    sleep(5)
