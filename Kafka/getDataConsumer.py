from fileinput import filename
from kafka import KafkaConsumer
import json
import boto3

def write_s3(value, offset):
    s3 = boto3.resource('s3')
    filename = 'Data/countPedestrian/'+str(offset)+'.json'
    s3object = s3.Object("vund23-deemo", filename)
    s3object.put(Body=(bytes(json.dumps(value, indent=4).encode('UTF-8'))))
    

topic_name = 'get-data'
consumer = KafkaConsumer(
    topic_name, bootstrap_servers = ['localhost : 9092'],
    value_deserializer = lambda x : json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
    )
# consumer = KafkaConsumer(
#     topic_name, bootstrap_servers = ['localhost : 9092'],
#     value_deserializer = lambda x : json.loads(x.decode('utf-8'))
#     )

for message in consumer:
    print("Consume message:", end = " ")
    print ("%s:%d:%d" % (message.topic, message.partition,message.offset))
    write_s3(message.value, str("%s:%d:%d" %(message.topic, message.partition,
                                        message.offset)))