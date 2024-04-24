import requests_cache
import openmeteo_requests
import pandas as pd
from retry_requests import retry
import datetime
import numpy as np

# Get Pittsburgh's weather on a specific date. Could add a geocoding API call to extend to any city. Could also add an "end date" parameter to capture a range. I'm just going to call the function for each specific date in Eric's biking data.

def get_pgh_weather(date):
    '''
    (DateTime object) -> DataFrame
    Returns a DataFrame with the morning (6:00 - 8:00) and evening (5:00 - 7:00) WMO weather codes. 
    '''
    if not isinstance(date, datetime.date):
        raise ValueError('The "Date" argument must be a datetime object')

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Convert the date to a specified string format
    date_string = date.strftime('%Y-%m-%d')

    # API URL
    url='https://archive-api.open-meteo.com/v1/archive'
    # API parameters from https://open-meteo.com/en/docs/historical-weather-api
    params = {
        'latitude' : 40.44,
        'longitude' : -79.96,
        'start_date' : date_string,
        'end_date' : date_string,
        'hourly' : ['weather_code', 'wind_speed_10m', 'wind_direction_10m'],
        'temperature_unit' : 'fahrenheit',
        'wind_speed_unit' : 'mph',
        'precipitation_unit' : 'inch'
    } 

    # Get the API response
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process the hourly data
    hourly = response.Hourly()
    hourly_weather_code = hourly.Variables(0).ValuesAsNumpy().astype('int')
    hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(2).ValuesAsNumpy()
    
    hourly_data = {"datetime": pd.date_range(
    	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    	freq = pd.Timedelta(seconds = hourly.Interval()),
    	inclusive = "left"
    )}

    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    
    hourly_dataframe = (pd.DataFrame(data = hourly_data)
                        .iloc[[6, 7, 17, 18]]
                        .assign(destination = ['Work', 'Work', 'Home', 'Home'])
                        .assign(date = lambda x: x['datetime'].dt.date)
                        .drop('datetime', axis = 1)
                        .groupby(['date','destination'])[['weather_code', 'wind_speed_10m', 'wind_direction_10m']]
                        .agg({'weather_code': np.max, 'wind_speed_10m': np.mean, 'wind_direction_10m': np.mean})
                        .reset_index()
                       )

    return hourly_dataframe


def get_riding_days(start_date, end_date):
    '''
    (DateTime object) -> DataFrame
    Returns a DataFrame with the morning (6:00 - 8:00) and evening (5:00 - 7:00) WMO weather codes and temperatures. 
    '''
    if not isinstance(start_date, datetime.date):
        raise ValueError('The "Date" argument must be a datetime object')

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Convert the date to a specified string format
    start_date_string = start_date.strftime('%Y-%m-%d')
    end_date_string = end_date.strftime('%Y-%m-%d')

    
    # API URL
    url='https://archive-api.open-meteo.com/v1/archive'
    # API parameters from https://open-meteo.com/en/docs/historical-weather-api
    params = {
        'latitude' : 40.44,
        'longitude' : -79.96,
        'start_date' : start_date_string,
        'end_date' : end_date_string,
        'hourly': ['temperature_2m', 'weather_code'],
    	'timezone': 'America/New_York',
        'temperature_unit' : 'fahrenheit',
        'wind_speed_unit' : 'mph',
        'precipitation_unit' : 'inch'
    } 

    # Get the API response
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process the hourly data
    hourly = response.Hourly()
    hourly_temp = hourly.Variables(0).ValuesAsNumpy()
    weather_code = hourly.Variables(1).ValuesAsNumpy()
    
    
    hourly_data = {'datetime': pd.date_range(
    	start = pd.to_datetime(hourly.Time(), unit = 's', utc = True),
    	end = pd.to_datetime(hourly.TimeEnd(), unit = 's', utc = True),
    	freq = pd.Timedelta(seconds = hourly.Interval()),
    	inclusive = 'left'
    )}

    hourly_data['temp'] = hourly_temp
    hourly_data['weather_code'] = weather_code


    
    hourly_dataframe = (pd.DataFrame(data = hourly_data)
                        .loc[hourly_data['datetime'].hour.isin([6, 7, 17, 18])]
                        .assign(year = lambda x: x['datetime'].dt.year)
                       )
    
    return (hourly_dataframe)

















