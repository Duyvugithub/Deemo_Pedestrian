import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

s3 = boto3.client('s3')
bucket = 'vund23-deemo'
filepath='Data/countPedestrian/test.json'

result = s3.get_object(Bucket=bucket, Key=filepath) 
# text = result["Body"].read()
# print(text) 



spark = SparkSession\
    .builder\
    .appName('App')\
    .getOrCreate()
    # .master('local[1]')\
    
sc = spark.sparkContext
sc.setLogLevel('ERROR')
count_schema = StructType([\
    StructField('date_time', StringType(), True),
    StructField('day', StringType(), True), 
    StructField('hourly_counts', IntegerType(), True),
    StructField('id', StringType(), True),
    StructField('mdate', IntegerType(), True),
    StructField('month', StringType(), True),
    StructField('sensor_id', StringType(), True),
    StructField('sensor_name', StringType(), True),
    StructField('time', IntegerType(), True),
    StructField('year', IntegerType(), True)
    ])
# Read count data
# count_df = spark.read.csv(filepath, header=True, schema=count_schema)
count_df = spark.read.json(result["Body"], multiLine=True, schema=count_schema)
# ----------------TRANSFORM PHASE ------------------
count_df = count_df.withColumn('date_time', to_timestamp('date_time', 'MMMM dd, yyyy hh:mm:ss a'))
count_df = count_df.withColumn("month",from_unixtime(unix_timestamp(col("month"),'MMMM'),'MM'))
print(count_df.schema)