from kafka import KafkaProducer
import json
from sodapy import Socrata
import time


# Configure Kafka Producer
topic_name='get-data'
producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = lambda x:json.dumps(x).encode('utf-8') 
    )


# Configure get data
with open('.env', 'r') as f:
    dotenv = json.loads(f.read())
offset = dotenv['offset']
limit = dotenv['limit']
client = Socrata("data.melbourne.vic.gov.au", None)
    
while True:
    # Get data
    while True:
        try:
            response = client.get("b2ak-trbp", limit=limit, offset=offset)
            break
        except:
            print("sleep")
            time.sleep(5)
    
    # response = client.get("b2ak-trbp", limit=limit, offset=offset)
    # Update offset
    offset += len(response)
    dotenv['offset'] = offset
    with open('.env', 'w') as f:
        json.dump(dotenv, f)

    if len(response) == 0:
        print("Newest data")
        time.sleep(3600)
        continue

    # Produce message
    message = producer.send(topic_name, value = response).get()
    print("Produce message:", end = " ")
    print ("%s:%d:%d" % (message.topic, message.partition,message.offset))
    

    