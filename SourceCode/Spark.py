from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window
import time
import boto3

def write_S3(local_path, s3_path, bucket):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(Filename = local_path, Bucket = bucket, Key = s3_path)

def load_sensor_data(filepath):
    # ----------------EXTRACT PHASE ---------------`---
    # Create schema for sensor data
    sensor_schema = StructType([\
                           StructField('sensor_id', StringType(), True),
                           StructField('sensor_description', StringType(), True),
                           StructField('sensor_name', StringType(), True),
                           StructField('installation_date', StringType(), True),
                           StructField('status', StringType(), True),
                           StructField('note', StringType(), True),
                           StructField('direction_1', StringType(), True),
                           StructField('direction_2', StringType(), True),
                           StructField('latitude', StringType(), True),
                           StructField('longitude', StringType(), True),
                           StructField('location', StringType(), True),
                           ])
    # Read sensor data
    sensor_df = spark.read.csv(filepath, header=True, schema = sensor_schema)
    return sensor_df

def load_count_data(filepath):
    # ----------------EXTRACT PHASE ------------------
    # Create schema for Counting data
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
    count_df = spark.read.json(filepath, multiLine=True, schema=count_schema)
    # ----------------TRANSFORM PHASE ------------------
    count_df = count_df.withColumn('date_time', to_timestamp('date_time', 'MMMM dd, yyyy hh:mm:ss a'))
    count_df = count_df.withColumn("month",from_unixtime(unix_timestamp(col("month"),'MMMM'),'MM'))
    print(count_df.schema)
    return count_df

def findTop10PerDay(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Calculate Hourly_Count per Day
    sumPedestrians = count_df.groupBy(['year', 'month', 'mdate', 'sensor_id'])\
                        .agg(sum('hourly_counts').alias('hourly_counts'))\
                        .sort(col("year").asc(), col("month").asc(),
                              col("mdate").asc(),col("hourly_counts").desc())
    tmp = Window.partitionBy('year', 'month', 'mdate')\
            .orderBy(sumPedestrians['hourly_counts'].desc())
    top10PerDay = sumPedestrians.withColumn("rank",row_number().over(tmp))\
        .filter(col("rank") <= 10)\
        .orderBy(col("year").asc(), col("month").asc(), 
                col("mdate").asc(),col("hourly_counts").desc())

    # ----------------LOADING PHASE -------------------
    # Join result with sensor_df 
    fullTop10PerDay = top10PerDay.drop('rank')\
        .join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'), 
            top10PerDay.sensor_id == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    fullTop10PerDay.toPandas().to_csv('./Result/Spark_Top10PerDay.csv', index=False)
    s3_path = 'Result/SparkResult/Spark_Top10PerDay.csv'
    local_path = './Result/Spark_Top10PerDay.csv'
    bucket = 'vund23-deemo'
    write_S3(local_path, s3_path, bucket)

    print('Running Top 10 Per Day')
    print("--- %s seconds ---" % (time.time() - start_time))
    return fullTop10PerDay


def findTop10Permonth(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Calculate Hourly_Count per month
    sumPedestriansmonth = count_df.groupBy(['year', 'month', 'sensor_id'])\
                        .agg(sum('hourly_counts').alias('hourly_counts'))\
                        .sort(col("year").asc(), col("month").asc(),col("hourly_counts").desc())
    # Get top 10 value per month based on ParitionBy
    tmp = Window.partitionBy('year', 'month')\
            .orderBy(sumPedestriansmonth['hourly_counts'].desc())
    top10Permonth = sumPedestriansmonth.withColumn("rank",row_number().over(tmp))\
        .filter(col("rank") <= 10)\
        .orderBy(col("year").asc(), col("month").asc(), col("hourly_counts").desc())
 


    # ----------------LOADING PHASE -------------------
    # Join result with sensor_df 
    fullTop10Permonth = top10Permonth.drop('rank')\
        .join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
            top10Permonth.sensor_id == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    fullTop10Permonth.toPandas().to_csv('./Result/Spark_Top10Permonth.csv', index=False)
    s3_path = 'Result/SparkResult/Spark_Top10Permonth.csv'
    local_path = './Result/Spark_Top10Permonth.csv'
    bucket = 'vund23-deemo'
    write_S3(local_path, s3_path, bucket)

    print('Running Top 10 Per month')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostDecline(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Sum hourly_counts last 3 year
    countLast3year_df = count_df.select('year', 'sensor_id', 'hourly_counts')\
                                .filter(count_df.year.isin(2020,2021,2022))\
                                .groupBy('year', 'sensor_id')\
                                .agg(sum('hourly_counts').alias('hourly_counts'))

    # Hourly_count based on sensor_id per year
    year2020 = countLast3year_df.filter(countLast3year_df.year==2020)['sensor_id', 'hourly_counts']\
                                .withColumnRenamed('hourly_counts', 'Y2020')
    year2021 = countLast3year_df.filter(countLast3year_df.year==2021)['sensor_id', 'hourly_counts']\
                                .withColumnRenamed('hourly_counts', 'Y2021')
    year2022 = countLast3year_df.filter(countLast3year_df.year==2022)['sensor_id', 'hourly_counts']\
                                .withColumnRenamed('hourly_counts', 'Y2022')

    # Calculate change from this year to another year
    year2020to2022 = year2020.join(year2022, year2020.sensor_id == year2022.sensor_id)\
                            .select(year2020.sensor_id, (year2020.Y2020 - year2022.Y2022).alias('hourly_counts'))
    year2020to2021 = year2020.join(year2021, year2020.sensor_id == year2021.sensor_id)\
                            .select(year2020.sensor_id, (year2020.Y2020 - year2021.Y2021).alias('hourly_counts'))
    year2021to2022 = year2021.join(year2022, year2021.sensor_id == year2022.sensor_id)\
                            .select(year2021.sensor_id, (year2021.Y2021 - year2022.Y2022).alias('hourly_counts'))                                

    # Calculate location just have record in 2020 and 2021
    justIn2020_2021 = year2020to2021.join(year2020to2022, year2020to2021.sensor_id == year2020to2022.sensor_id, "leftanti")
    # Calculate location just have record in 2021 and 2022
    justIn2021_2022 = year2021to2022.join(year2020to2022, year2021to2022.sensor_id == year2020to2022.sensor_id, "leftanti")
    # Union result
    ChangeFrom2020To2022 = year2020to2022.union(justIn2020_2021).union(justIn2021_2022)
    # Find most Decline location
    ChangeFrom2020To2022.createOrReplaceTempView("DF")
    mostDeclide_df = spark.sql("select* from DF where hourly_counts == (select max(hourly_counts) from DF)")

    # ----------------LOADING PHASE -------------------
    mostDeclide_df = mostDeclide_df.join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
        mostDeclide_df.sensor_id == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    mostDeclide_df.toPandas().to_csv('./Result/Spark_MostDeclineLast2year.csv', index=False)
    s3_path = 'Result/SparkResult/Spark_MostDeclineLast2year.csv'
    local_path = './Result/Spark_MostDeclineLast2year.csv'
    bucket = 'vund23-deemo'
    write_S3(local_path, s3_path, bucket)

    print('Running Most Decline Last 2 year')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostGrowth(sensor_df, count_df, path):
    start_time = time.time()

    # Sum hourly_counts last 3 year
    countLast3year_df = count_df.select('year', 'sensor_id', 'hourly_counts')\
                                .filter(count_df.year.isin(2021,2022))\
                                .groupBy('year', 'sensor_id')\
                                .agg(sum('hourly_counts').alias('hourly_counts'))
    # Hourly_count based on sensor_id per year
    year2021 = countLast3year_df.filter(countLast3year_df.year==2021)['sensor_id', 'hourly_counts']\
                                .withColumnRenamed('hourly_counts', 'Y2021')
    year2022 = countLast3year_df.filter(countLast3year_df.year==2022)['sensor_id', 'hourly_counts']\
                                .withColumnRenamed('hourly_counts', 'Y2022')
    # Calculate change from this year to another year
    year2021to2022 = year2021.join(year2022, year2021.sensor_id == year2022.sensor_id)\
                            .select(year2021.sensor_id, (year2021.Y2021 - year2022.Y2022).alias('hourly_counts'))                                
    # Find most growth location
    year2021to2022.createOrReplaceTempView("Y2021to2022")
    mostGrowth_df = spark.sql("select * from Y2021to2022 \
        where hourly_counts == (select min(hourly_counts) from Y2021to2022)")


    # ----------------LOADING PHASE -------------------
    mostGrowth_df = mostGrowth_df.join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
        mostGrowth_df.sensor_id == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    mostGrowth_df.toPandas().to_csv('./Result/Spark_MostGrowthLastyear.csv', index=False)
    s3_path = 'Result/SparkResult/Spark_MostGrowthLastyear.csv'
    local_path = './Result/Spark_MostGrowthLastyear.csv'
    bucket = 'vund23-deemo'
    write_S3(local_path, s3_path, bucket)
    print('Running Most Growth last year')
    print("--- %s seconds ---" % (time.time() - start_time))

path = 's3n://vund23-deemo/'
# path = ''
filepath_sensor = path + 'Data/Pedestrian_Counting_System_-_Sensor_Locations.csv'
filepath_count = path + 'Data/countPedestrian/*.json'

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName('App')\
        .getOrCreate()
        # .master('local[1]')\
        
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    sensor_df = load_sensor_data(filepath_sensor)
    count_df = load_count_data(filepath_count)

    findTop10PerDay(sensor_df , count_df, path)
    findTop10Permonth(sensor_df , count_df, path)
    findMostDecline(sensor_df , count_df, path)
    findMostGrowth(sensor_df , count_df, path)

