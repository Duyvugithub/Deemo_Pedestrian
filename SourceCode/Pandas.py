import pandas as pd
from datetime import date
import datetime
import time

def load_sensor_data(filepath):
    sensor_df = pd.read_csv(filepath)
    return sensor_df

def load_count_data(filepath):
    count_df = pd.read_csv(filepath)
    count_df['Date_Time'] = pd.to_datetime(count_df['Date_Time'], format='%B %d, %Y %X %p')
    count_df['Date'] = count_df['Date_Time'].dt.date
    count_df['Month'] = pd.to_datetime(count_df['Month'], format='%B').dt.month
    return count_df

def findTop10PerDay(sensor_df , count_df, path):
    start_time = time.time()
    # ---------------TRANSFORM PHASE------------------
    # Sum number of Pedestrian
    sumPerDay = count_df.groupby(['Year', 'Month', 'Mdate', 'Sensor_ID'])\
        ['Hourly_Counts'].sum('Hourly_Counts').reset_index()
    # Sort value by total Hourly Counts per day
    sumPerDaySorted = sumPerDay.sort_values(['Year', 'Month', 'Mdate', 'Hourly_Counts'], 
                                            ascending = [True, True, True, False])
    # Get top 10 pedestrians location by day
    top10PerDay = sumPerDaySorted.groupby(['Year', 'Month', 'Mdate']).head(10)

    # ----------------LOADING PHASE-------------------
    tmp = top10PerDay.join(sensor_df[['sensor_description', 'sensor_name', 'sensor_id']]\
    .set_index('sensor_id'), 
                       on='Sensor_ID')
    fullTop10PerDay = tmp.sort_values(['Year', 'Month', 'Mdate', 'Hourly_Counts'],
                                    ascending = [True, True, True, False])
    fullTop10PerDay.to_csv(path+'PandasResult/Pandas_Top10PerDay.csv', index = False)
    print('Pandas Running Top 10 Per Day')
    print("--- %s seconds ---" % (time.time() - start_time))


def findTop10PerMonth(sensor_df , count_df, path):
    start_time = time.time()
    # ---------------TRANSFORM PHASE------------------
    # Sum number of Pedestrian
    sumPerMonth = count_df.groupby(['Year', 'Month', 'Sensor_ID'])['Hourly_Counts'].sum('Hourly_Counts').reset_index()
    # Sort value by total Hourly Counts per day
    sumPerMonthSorted = sumPerMonth.sort_values(['Year', 'Month', 'Hourly_Counts'], ascending = [True, True, False])
    # Get top 10 pedestrians location by day
    top10PerMonth = sumPerMonthSorted.groupby(['Year', 'Month']).head(10)

    # ----------------LOADING PHASE-------------------
    tmp = top10PerMonth.join(sensor_df[['sensor_description', 'sensor_name', 'sensor_id']]\
        .set_index('sensor_id'), on='Sensor_ID')
    fullTop10PerMonth = tmp.sort_values(['Year', 'Month', 'Hourly_Counts'],
                                    ascending = [True, True, False])
    fullTop10PerMonth.to_csv(path+'PandasResult/Pandas_Top10PerMonth.csv', index = False)
    print('Pandas Running Top 10 Per Month')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostDecline(sensor_df , count_df, path):
    start_time = time.time()
    # ---------------TRANSFORM PHASE------------------
    # Sum number Pedestrians in 3 year 2020, 2021, 2022
    countLast3year_df = count_df[count_df['Year'].isin([2020,2021,2022])]
    tmp = countLast3year_df.groupby(['Year', 'Sensor_ID'])[['Hourly_Counts']].sum().reset_index('Year')
    # Calculate Number of Pedestrians change from 2020 to 2022
    year2020 = tmp[tmp['Year'] == 2020]['Hourly_Counts']
    year2021 = tmp[tmp['Year'] == 2021]['Hourly_Counts']
    year2022 = tmp[tmp['Year'] == 2022]['Hourly_Counts']
    # Calculate Number of Pedestrians change from 2020 to 2022
    year2020to2022 = year2020-year2022
    year2020to2022_index = set(year2020to2022[year2020to2022.notnull()].index)
    year2020to2022 = year2020to2022[year2020to2022.notnull()]
    # Calculate Number of Pedestrians change from 2020 to 2021 and not in from 2020 to 2022
    year2020to2021 = year2020-year2021
    year2020to2021_index = set(year2020to2021[year2020to2021.notnull()].index)
    justIn20_21_index = list(set(year2020to2021_index) - set(year2020to2022_index))
    justIn20_21 = year2020to2021[justIn20_21_index]
    # Calculate Number of Pedestrians change from 2021 to 2022 and not in from 2020 to 2022
    year2021to2022 = year2021-year2022
    # Get list index of value just in 2021
    year2021to2022_index = set(year2021to2022[year2021to2022.notnull()].index)
    justIn21_22_index = list(set(year2021to2022_index) - set(year2020to2022_index))
    justIn21_22 = year2021to2022[justIn21_22_index]
    # Concat result
    ChangeFrom2020To2022 = pd.concat([year2020to2022, justIn20_21, justIn21_22])
    idx = ChangeFrom2020To2022.idxmax()
    mostDeclineDict = {
        'Sensor_ID': [idx],
        'Sum_Hourly_Count': [year2020[idx]- year2022[idx]]}
    mostDeclide_df = pd.DataFrame.from_dict(mostDeclineDict)


    # ----------------LOADING PHASE-------------------
    # Record result
    mostDeclide = mostDeclide_df.join(sensor_df[['sensor_description', 'sensor_name', 'sensor_id']]\
        .set_index('sensor_id'), on='Sensor_ID')

    mostDeclide.to_csv(path+'PandasResult/Pandas_MostDeclineLast2Year.csv', index = False)
    print('Spark Running Most Decline Last 2 Year')
    print("--- %s seconds ---" % (time.time() - start_time))

def findMostGrowth(sensor_df , count_df, path):
    start_time = time.time()
    # Sum number Pedestrians in 3 year 2020, 2021, 2022
    countLast3year_df = count_df[count_df['Year'].isin([2021,2022])]
    tmp = countLast3year_df.groupby(['Year', 'Sensor_ID'])[['Hourly_Counts']].sum().reset_index('Year')
    # Calculate Number of Pedestrians change from 2020 to 2022
    year2021 = tmp[tmp['Year'] == 2021]['Hourly_Counts']
    year2022 = tmp[tmp['Year'] == 2022]['Hourly_Counts']
    res = year2021 - year2022
    # Get index of location most growth last year
    idx = res.idxmin()
    mostGrowth = {'Sensor_ID': [idx],
                'Hourly_Count': [year2021[idx] - year2021[idx]]}
    mostGrowth_df = pd.DataFrame.from_dict(mostGrowth)
    # ----------------LOADING PHASE-------------------
    # Record result
    mostGrowth = mostGrowth_df.join(sensor_df[['sensor_description', 'sensor_name', 'sensor_id']]\
        .set_index('sensor_id'), on='Sensor_ID')

    mostGrowth.to_csv(path+'PandasResult/Pandas_MostGrowthLastYear.csv', index = False)
    print('Pandas Running Most Growth last year')
    print("--- %s seconds ---" % (time.time() - start_time))


path = 's3n://vund23-deemo/'
filepath_sensor = path + 'Data/Pedestrian_Counting_System_-_Sensor_Locations.csv'
filepath_count = path + 'Data/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv'

if __name__ == "__main__":
    sensor_df = load_sensor_data(filepath_sensor)
    count_df = load_count_data(filepath_count)

    findTop10PerDay(sensor_df , count_df, path)
    findTop10PerMonth(sensor_df , count_df, path)
    findMostDecline(sensor_df , count_df, path)
    findMostGrowth(sensor_df , count_df, path)