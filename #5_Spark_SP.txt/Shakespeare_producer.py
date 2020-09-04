from kafka import KafkaProducer
from time import sleep

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         api_version=(0,10,1))

f = open('Shakespeare.txt','r')
text = f.read()
msgs_2_send = text.split('\n')

for msg in msgs_2_send:
    #producer.send('zoo-lion',json.dumps(msg).encode('utf-8'))
    producer.send('zoo-lion',msg.encode('utf-8'))
    sleep(5)
    print("msg sent: "+ msg)