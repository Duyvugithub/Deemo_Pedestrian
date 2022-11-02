from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window
import time

def load_sensor_data(filepath):
    # ----------------EXTRACT PHASE ------------------
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
                          StructField('ID', StringType(), True),
                          StructField('Date_Time', StringType(), True),
                          StructField('Year', IntegerType(), True),
                          StructField('Month', StringType(), True),
                          StructField('Mdate', IntegerType(), True),
                          StructField('Day', StringType(), True),
                          StructField('Time', IntegerType(), True),
                          StructField('Sensor_ID', StringType(), True),
                          StructField('Sensor_Name', StringType(), True),
                          StructField('Hourly_Counts', IntegerType(), True)
                          ])
    # Read count data
    count_df = spark.read.csv(filepath, header=True, schema=count_schema)
    # ----------------TRANSFORM PHASE ------------------
    count_df = count_df.withColumn('Date_Time', to_timestamp('Date_Time', 'MMMM dd, yyyy hh:mm:ss a'))
    count_df = count_df.withColumn("Month",from_unixtime(unix_timestamp(col("Month"),'MMMM'),'MM'))
    return count_df

def findTop10PerDay(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Calculate Hourly_Count per Day
    sumPedestrians = count_df.groupBy(['Year', 'Month', 'Mdate', 'Sensor_ID'])\
                        .agg(sum('Hourly_Counts').alias('Hourly_Counts'))\
                        .sort(col("Year").asc(), col("Month").asc(),
                              col("Mdate").asc(),col("Hourly_Counts").desc())
    tmp = Window.partitionBy('Year', 'Month', 'Mdate')\
            .orderBy(sumPedestrians['Hourly_Counts'].desc())
    top10PerDay = sumPedestrians.withColumn("rank",row_number().over(tmp))\
        .filter(col("rank") <= 10)\
        .orderBy(col("Year").asc(), col("Month").asc(), 
                col("Mdate").asc(),col("Hourly_Counts").desc())

    # ----------------LOADING PHASE -------------------
    # Join result with sensor_df 
    fullTop10PerDay = top10PerDay.drop('rank')\
        .join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'), 
            top10PerDay.Sensor_ID == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    fullTop10PerDay.toPandas().to_csv(path + './SparkResult/Spark_Top10PerDay.csv', index=False)
    print('Running Top 10 Per Day')
    print("--- %s seconds ---" % (time.time() - start_time))
    return fullTop10PerDay


def findTop10PerMonth(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Calculate Hourly_Count per Month
    sumPedestriansMonth = count_df.groupBy(['Year', 'Month', 'Sensor_ID'])\
                        .agg(sum('Hourly_Counts').alias('Hourly_Counts'))\
                        .sort(col("Year").asc(), col("Month").asc(),col("Hourly_Counts").desc())
    # Get top 10 value per month based on ParitionBy
    tmp = Window.partitionBy('Year', 'Month')\
            .orderBy(sumPedestriansMonth['Hourly_Counts'].desc())
    top10PerMonth = sumPedestriansMonth.withColumn("rank",row_number().over(tmp))\
        .filter(col("rank") <= 10)\
        .orderBy(col("Year").asc(), col("Month").asc(), col("Hourly_Counts").desc())
 


    # ----------------LOADING PHASE -------------------
    # Join result with sensor_df 
    fullTop10PerMonth = top10PerMonth.drop('rank')\
        .join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
            top10PerMonth.Sensor_ID == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    fullTop10PerMonth.toPandas().to_csv(path+'./SparkResult/Spark_Top10PerMonth.csv', index=False)
    print('Running Top 10 Per Month')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostDecline(sensor_df, count_df, path):
    start_time = time.time()
    # ---------- TRANSFORM PHASE -------------------
    # Sum Hourly_counts last 3 year
    countLast3year_df = count_df.select('Year', 'Sensor_ID', 'Hourly_Counts')\
                                .filter(count_df.Year.isin(2020,2021,2022))\
                                .groupBy('Year', 'Sensor_ID')\
                                .agg(sum('Hourly_Counts').alias('Hourly_Counts'))

    # Hourly_count based on Sensor_ID per year
    year2020 = countLast3year_df.filter(countLast3year_df.Year==2020)['Sensor_ID', 'Hourly_Counts']\
                                .withColumnRenamed('Hourly_Counts', 'Y2020')
    year2021 = countLast3year_df.filter(countLast3year_df.Year==2021)['Sensor_ID', 'Hourly_Counts']\
                                .withColumnRenamed('Hourly_Counts', 'Y2021')
    year2022 = countLast3year_df.filter(countLast3year_df.Year==2022)['Sensor_ID', 'Hourly_Counts']\
                                .withColumnRenamed('Hourly_Counts', 'Y2022')

    # Calculate change from this year to another year
    year2020to2022 = year2020.join(year2022, year2020.Sensor_ID == year2022.Sensor_ID)\
                            .select(year2020.Sensor_ID, (year2020.Y2020 - year2022.Y2022).alias('Hourly_Counts'))
    year2020to2021 = year2020.join(year2021, year2020.Sensor_ID == year2021.Sensor_ID)\
                            .select(year2020.Sensor_ID, (year2020.Y2020 - year2021.Y2021).alias('Hourly_Counts'))
    year2021to2022 = year2021.join(year2022, year2021.Sensor_ID == year2022.Sensor_ID)\
                            .select(year2021.Sensor_ID, (year2021.Y2021 - year2022.Y2022).alias('Hourly_Counts'))                                

    # Calculate location just have record in 2020 and 2021
    justIn2020_2021 = year2020to2021.join(year2020to2022, year2020to2021.Sensor_ID == year2020to2022.Sensor_ID, "leftanti")
    # Calculate location just have record in 2021 and 2022
    justIn2021_2022 = year2021to2022.join(year2020to2022, year2021to2022.Sensor_ID == year2020to2022.Sensor_ID, "leftanti")
    # Union result
    ChangeFrom2020To2022 = year2020to2022.union(justIn2020_2021).union(justIn2021_2022)
    # Find most Decline location
    ChangeFrom2020To2022.createOrReplaceTempView("DF")
    mostDeclide_df = spark.sql("select* from DF where Hourly_Counts == (select max(Hourly_Counts) from DF)")

    # ----------------LOADING PHASE -------------------
    mostDeclide_df = mostDeclide_df.join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
        mostDeclide_df.Sensor_ID == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    mostDeclide_df.toPandas().to_csv(path+'./SparkResult/Spark_MostDeclineLast2Year.csv', index=False)
    print('Running Most Decline Last 2 Year')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostGrowth(sensor_df, count_df, path):
    start_time = time.time()

    # Sum Hourly_counts last 3 year
    countLast3year_df = count_df.select('Year', 'Sensor_ID', 'Hourly_Counts')\
                                .filter(count_df.Year.isin(2021,2022))\
                                .groupBy('Year', 'Sensor_ID')\
                                .agg(sum('Hourly_Counts').alias('Hourly_Counts'))
    # Hourly_count based on Sensor_ID per year
    year2021 = countLast3year_df.filter(countLast3year_df.Year==2021)['Sensor_ID', 'Hourly_Counts']\
                                .withColumnRenamed('Hourly_Counts', 'Y2021')
    year2022 = countLast3year_df.filter(countLast3year_df.Year==2022)['Sensor_ID', 'Hourly_Counts']\
                                .withColumnRenamed('Hourly_Counts', 'Y2022')
    # Calculate change from this year to another year
    year2021to2022 = year2021.join(year2022, year2021.Sensor_ID == year2022.Sensor_ID)\
                            .select(year2021.Sensor_ID, (year2021.Y2021 - year2022.Y2022).alias('Hourly_Counts'))                                
    # Find most growth location
    year2021to2022.createOrReplaceTempView("Y2021to2022")
    mostGrowth_df = spark.sql("select * from Y2021to2022 \
        where Hourly_Counts == (select min(Hourly_Counts) from Y2021to2022)")


    # ----------------LOADING PHASE -------------------
    mostGrowth_df = mostGrowth_df.join(sensor_df.select('sensor_description', 'sensor_name', 'sensor_id'),
        mostGrowth_df.Sensor_ID == sensor_df.sensor_id, 'left')\
        .drop(sensor_df['sensor_id'])
    # Write to csv file
    mostGrowth_df.toPandas().to_csv(path+'./SparkResult/Spark_MostGrowthLastYear.csv', index=False)
    print('Running Most Growth last year')
    print("--- %s seconds ---" % (time.time() - start_time))

# path = 's3n://vund23-deemo/'
path = './'
filepath_sensor = path + 'Data/Pedestrian_Counting_System_-_Sensor_Locations.csv'
filepath_count = path + 'Data/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv'

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
    findTop10PerMonth(sensor_df , count_df, path)
    findMostDecline(sensor_df , count_df, path)
    findMostGrowth(sensor_df , count_df, path)

