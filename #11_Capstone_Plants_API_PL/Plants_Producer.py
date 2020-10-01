from kafka import KafkaProducer
from time import sleep
import json
import requests

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         api_version=(0,10,1))

f = open('creds.json','r')
creds_json = f.read()
creds_dict = json.loads(creds_json)

trefle_access_token = creds_dict['access_token']
trefle_API_HOST = 'https://trefle.io'
query = '/api/v1/plants'
trefle_auth = '?token='+trefle_access_token
pagination = '&page='
page = 1
cnt = 1;
while page:
    
    #try:
    response = requests.get(trefle_API_HOST+query+trefle_auth+pagination+str(page))
    page += 1
    plants_data_dict = response.json()

    for plant_result in plants_data_dict['data']:
        plant_detail_url = plant_result['links']['plant']

        detail_response = requests.get(trefle_API_HOST+plant_detail_url+trefle_auth)
        print(cnt,": ",trefle_API_HOST+plant_detail_url+trefle_auth)
        plant_detail_data_dict = detail_response.json()
        plant_detail_json = json.dumps(plant_detail_data_dict['data'])

        producer.send('plants-topic',plant_detail_json.encode('utf-8'))
        sleep(10)
        cnt+=1
            
    #except Exception:
        #print('Finished Streaming API data!')
        #break