import boto3
from sodapy import Socrata
import json

def writeJson(filepath, data):
    with open(filepath, 'a') as f:
        json.dump(data,f)

filepath = 'countPedestrian.json'
offset = 0
limit =1000000
client = Socrata("data.melbourne.vic.gov.au", None)
while True:
    print("offset: ", offset)
    response = client.get("b2ak-trbp", limit=limit, offset=offset)
    print(len(response))
    if len(response) == 0:
        break
    
    writeJson(filepath, response)
    offset += len(response)
