from re import L
import pandas as pd

def load_sensor_data(filepath):
    sensor_df = pd.read_csv(filepath)
    return sensor_df

# path = 's3n://vund23-deemo/'
path = ''
filepath_sensor = path + 'Data/Pedestrian_Counting_System_-_Sensor_Locations.csv'
filepath_count = path + 'Data/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv'
sensor_df = load_sensor_data(filepath_sensor)
sensor_id = sensor_df['sensor_id'].tolist()
# Testing sensor_id in result exist 
def test_top10PerDay_ID():
    res = pd.read_csv(path + './Result/SparkResult/Spark_Top10PerDay.csv')
    res_sensor_id = res['Sensor_ID'].tolist()
    assert set(res_sensor_id).issubset(sensor_id) == True

def test_Top10PerMonth_ID():
    res = pd.read_csv(path + './Result/SparkResult/Spark_Top10PerMonth.csv')
    res_sensor_id = res['Sensor_ID'].tolist()
    assert set(res_sensor_id).issubset(sensor_id) == True

def test_MostDeclineLast2Year_ID():
    res = pd.read_csv(path+'./Result/SparkResult/Spark_MostDeclineLast2Year.csv')
    res_sensor_id = res['Sensor_ID'].tolist()
    assert set(res_sensor_id).issubset(sensor_id) == True

def test_MostGrowthLastYear_ID():
    res = pd.read_csv(path + './Result/SparkResult/Spark_MostGrowthLastYear.csv')
    res_sensor_id = res['Sensor_ID'].tolist()
    assert set(res_sensor_id).issubset(sensor_id) == True