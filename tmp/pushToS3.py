import boto3
from nbformat import write

def writeS3(local_path, s3_path, bucket):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(Filename = local_path, Bucket = bucket, Key = s3_path)

s3_path = 'SourceCode/Spark.py'
local_path = './SourceCode/Spark.py'
bucket = 'vund23-deemo'
writeS3(local_path, s3_path, bucket)





