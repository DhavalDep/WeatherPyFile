#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 16:54:34 2024

@author: dhavaldepala
"""

from dotenv import load_dotenv
import os
import psycopg2 as psql
import pandas as pd
import requests


load_dotenv()
sql_user = os.getenv('sql_user')
sql_pass = os.getenv('sql_pass')
my_host = os.getenv('host')
api_key = os.getenv('api_key')


#Create Dataframe for the current weather data
columns = ['date','city','temperature','condition_text','condition_img','humidity', 'cloud', 'wind', 'precip']
current_df = pd.DataFrame(columns=columns)


#Gathering current weather data from API function and gather this in appropriate variables
def current_weather(city):
    url = f'https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}'
    response = requests.get(url)
    data = response.json()
    current_date = data['current']['last_updated']
    current_temp = data['current']['temp_c']
    current_condition_text = data['current']['condition']['text']
    current_condition_img = data['current']['condition']['icon']
    current_humidity = data['current']['humidity']
    current_cloud = data['current']['cloud']
    current_wind = data['current']['wind_mph']
    current_precip = data['current']['precip_mm']
    
    return current_date, current_temp, current_condition_text, current_condition_img, current_humidity, current_cloud, current_wind, current_precip

cities = ['london', 'manchester', 'birmingham','liverpool', 'leeds', 'nottingham', 'sheffield', 'cardiff', 'glasgow']
   
#Loop through the cities and gather the data using the current_weather function and put the data into the df 
for i in cities: 
    current_date, current_temp, current_condition_text, current_condition_img, current_humidity, current_cloud, current_wind, current_precip = current_weather(i)
    new_row = pd.DataFrame([{
    'date': current_date,
    'city': i,
    'temperature': current_temp,
    'condition_text': current_condition_text,
    'condition_img': current_condition_img,
    'humidity': current_humidity,
    'cloud':current_cloud,
    'wind': current_wind,
    'precip':current_precip
        }])
    
    current_df = pd.concat([current_df, new_row], ignore_index=True)
#List of cities that will be used (can always be extended easily)
cities = ['london', 'manchester', 'birmingham', 'liverpool', 'leeds', 'nottingham', 'sheffield', 'cardiff', 'glasgow']
#current_weather(city)
       
    
#Create second Dataframe for forecasted weather data
columns_2 = ['date','fc_date','city','fc_temperature_24hrs','fc_condition_text','fc_condition_img','hr0_temp',
    'hr1_temp',
    'hr2_temp',
    'hr3_temp',
    'hr4_temp',
    'hr5_temp',
    'hr6_temp',
    'hr7_temp',
    'hr8_temp',
    'hr9_temp',
    'hr10_temp',
    'hr11_temp',
    'hr12_temp',
    'hr13_temp',
    'hr14_temp',
    'hr15_temp',
    'hr16_temp',
    'hr17_temp',
    'hr18_temp',
    'hr19_temp',
    'hr20_temp',
    'hr21_temp',
    'hr22_temp',
    'hr23_temp' ]
full_forecast_df = pd.DataFrame(columns=columns_2)



#Function to Loop to find the closest hour to the current time (return which hour it is)
def find_fc_hour():
    url = f'https://api.weatherapi.com/v1/forecast.json?key={api_key}&q=london&days=2'
    response = requests.get(url)
    data = response.json()
    for i in range(0,24):
        loopdate = data['forecast']['forecastday'][0]['hour'][i]['time']
        currentdate = data['current']['last_updated']
        if loopdate.startswith(currentdate[:-2]):
            to_the_hour_date = data['forecast']['forecastday'][0]['hour'][i]['time']
            #print(data['forecast']['forecastday'][1]['hour'][i]['time'])
            break
    return i


#Gathering forecasted weather data from API function and gather this in appropriate variables
def forecast_weather(city):
    url = f'https://api.weatherapi.com/v1/forecast.json?key={api_key}&q={city}&days=2'
    response = requests.get(url)
    data = response.json()
    
    current_date = data['current']['last_updated']
    hour = find_fc_hour()
    fc_date = data['forecast']['forecastday'][1]['hour'][hour]['time']
    fc_temperature_24hrs = data['forecast']['forecastday'][1]['hour'][hour]['temp_c']
    fc_condition_text = data['forecast']['forecastday'][1]['hour'][hour]['condition']['text']
    fc_condition_img = data['forecast']['forecastday'][1]['hour'][hour]['condition']['icon']
    
    # Initialize an empty list to store hourly temperatures
    hourly_temps = []

    # Loop through the first day's hours and extract temperatures
    for hour_data in data['forecast']['forecastday'][0]['hour']:
        hourly_temps.append(hour_data['temp_c'])
        
    return current_date, fc_date, fc_temperature_24hrs, fc_condition_text, fc_condition_img, *hourly_temps


for city in cities:
    current_date, fc_date, fc_temperature_24hrs, fc_condition_text, fc_condition_img, *hourly_temps = forecast_weather(city)
    
    new_row = pd.DataFrame([{
        'date': current_date,
        'fc_date': fc_date,
        'city': city,
        'fc_temperature_24hrs': fc_temperature_24hrs,
        'fc_condition_text': fc_condition_text,
        'fc_condition_img': fc_condition_img,
        'hr0_temp': hourly_temps[0],
        'hr1_temp': hourly_temps[1],
        'hr2_temp': hourly_temps[2],
        'hr3_temp': hourly_temps[3],
        'hr4_temp': hourly_temps[4],
        'hr5_temp': hourly_temps[5],
        'hr6_temp': hourly_temps[6],
        'hr7_temp': hourly_temps[7],
        'hr8_temp': hourly_temps[8],
        'hr9_temp': hourly_temps[9],
        'hr10_temp': hourly_temps[10],
        'hr11_temp': hourly_temps[11],
        'hr12_temp': hourly_temps[12],
        'hr13_temp': hourly_temps[13],
        'hr14_temp': hourly_temps[14],
        'hr15_temp': hourly_temps[15],
        'hr16_temp': hourly_temps[16],
        'hr17_temp': hourly_temps[17],
        'hr18_temp': hourly_temps[18],
        'hr19_temp': hourly_temps[19],
        'hr20_temp': hourly_temps[20],
        'hr21_temp': hourly_temps[21],
        'hr22_temp': hourly_temps[22],
        'hr23_temp': hourly_temps[23]
    }])
    
    full_forecast_df = pd.concat([full_forecast_df, new_row], ignore_index=True)



    
#SQL PORTION    
#Establish a connection to the database
conn = psql.connect(database = "pagila",
                    user = sql_user,
                    password = sql_pass,
                    host = my_host,
                    port = 5432
                   )

#Creating the SQL table (if its not done already) for the current weather data
sql_create_table = """
    CREATE TABLE IF NOT EXISTS student.de10_dd_captest_current(
    date varchar(25),
    city varchar(25),
    temperature float,
    condition_text varchar(100),
    condition_img varchar(100),
    humidity float, 
    cloud float, 
    wind float, 
    precip float
    );
"""
cur = conn.cursor()
cur.execute(sql_create_table)
conn.commit()


#Inserting the data from the dataframe into the database
cur = conn.cursor()
sql_insert = """
    INSERT INTO student.de10_dd_captest_current(
    date,
    city,
    temperature,
    condition_text,
    condition_img,
    humidity, 
    cloud,
    wind, 
    precip
    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

for index, row in current_df.iterrows():
    cur.execute(sql_insert, (row['date'], row['city'], row['temperature'], row['condition_text'],row['condition_img'] ,row['humidity'], row['cloud'], row['wind'], row['precip']))
    #print(f"Inserted row {index + 1}")


conn.commit()


#Creating the SQL table (if its not done already) for the forecasted weather data
sql_create_table = """
    CREATE TABLE IF NOT EXISTS student.de10_dd_captest_full_forecast(
    date varchar(25),
    fc_date varchar(25),
    city varchar(25),
    fc_temperature float,
    fc_condition_text varchar(100),
    fc_condition_img varchar(100),
    hr0_temp float,
    hr1_temp float,
    hr2_temp float,
    hr3_temp float,
    hr4_temp float,
    hr5_temp float,
    hr6_temp float,
    hr7_temp float,
    hr8_temp float,
    hr9_temp float,
    hr10_temp float,
    hr11_temp float,
    hr12_temp float,
    hr13_temp float,
    hr14_temp float,
    hr15_temp float,
    hr16_temp float,
    hr17_temp float,
    hr18_temp float,
    hr19_temp float,
    hr20_temp float,
    hr21_temp float,
    hr22_temp float,
    hr23_temp float
    );
"""
cur = conn.cursor()
cur.execute(sql_create_table)
conn.commit()

#Inserting the data from the dataframe into the database
cur = conn.cursor()
sql_insert = """
    INSERT INTO student.de10_dd_captest_full_forecast(
    date,
    fc_date,
    city,
    fc_temperature,
    fc_condition_text,
    fc_condition_img,
    hr0_temp,
    hr1_temp,
    hr2_temp,
    hr3_temp,
    hr4_temp,
    hr5_temp,
    hr6_temp,
    hr7_temp,
    hr8_temp,
    hr9_temp,
    hr10_temp,
    hr11_temp,
    hr12_temp,
    hr13_temp,
    hr14_temp,
    hr15_temp,
    hr16_temp,
    hr17_temp,
    hr18_temp,
    hr19_temp,
    hr20_temp,
    hr21_temp,
    hr22_temp,
    hr23_temp
    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

for index, row in full_forecast_df.iterrows():
    cur.execute(sql_insert, (row['date'], row['fc_date'], row['city'], row['fc_temperature_24hrs'],
                             row['fc_condition_text'],row['fc_condition_img'],row['hr0_temp'],row['hr1_temp'],
                             row['hr2_temp'],row['hr3_temp'],row['hr4_temp'],row['hr5_temp'],row['hr6_temp'],
                             row['hr7_temp'],row['hr8_temp'],row['hr9_temp'],row['hr10_temp'],row['hr11_temp'],
                             row['hr12_temp'],row['hr13_temp'],row['hr14_temp'],row['hr15_temp'],row['hr16_temp'],
                             row['hr17_temp'],row['hr18_temp'],row['hr19_temp'],row['hr20_temp'],row['hr21_temp'],
                             row['hr22_temp'],row['hr23_temp']))
    #print(f"Inserted row {index + 1}")


conn.commit()