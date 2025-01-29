"""
The purpose of this .py file is to establish connection with the MYSQL server and the air quality API.
Send a request, save the data as a pandas df, then insert it into database. 
"""
#DONE: change element(s) to pollutant(s)
#DONE: consider alternate api for more calls/ better data: https://aqicn.org/json-api/doc/
#TODO: consider openaq-quality-checks library for quality control of data

#Import dependencies
import os, sys, requests, json, csv, requests
import time
from datetime import datetime, date
from dotenv import load_dotenv
from openaq import OpenAQ, RateLimit as RateLimitError
from pandas import DataFrame
import pandas as pd
fromiso = datetime.fromisoformat

#Extract api keys and connection info
load_dotenv()

#pull secrets from environment 
OPENAQ_API_KEY = os.getenv('OPENAQ_API_KEY')
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_PORT = os.getenv('DB_PORT')
DB_REGION = os.getenv('DB_REGION')
DB_IAMUSER = os.getenv('DB_IAMUSER')

# init client (automatically pulls KEY from env)
api = OpenAQ()

# check rate limit and sleep if required
def check_rate_limit(response, to_print=True):
        if to_print:
                print(f'{response.headers.x_ratelimit_used} call(s) placed')
        #catch/limit rate limiting
        if response.headers.x_ratelimit_remaining == 0:
                if to_print:
                        print(f'\nRate limit reached. Sleeping {response.headers.x_ratelimit_reset} seconds...')
                rest = response.headers.x_ratelimit_reset
                time.sleep(rest)

#Get location info from location endpoint - taking location id as argument
def get_location_response(loc_id):
	try:
		loc_response = api.locations.get(loc_id)

		#pass loc response to check rate limit + sleep if nec.
		check_rate_limit(loc_response)
		return loc_response
	except RateLimitError:	#exception object from OpenAQ sdk
		time.sleep(30)

		#Call get_loc recursively with the same loc_id. This is only safe if the exception is rate limit, so it won't be inf. loop.
		return get_location_response(loc_id)
	except:
		return None
def location_res_to_dfs(loc_response):
	res = loc_response.results[0]

	loc_dict = {
		'id': res.id,
		'latitude': res.coordinates.latitude,
		'longitude': res.coordinates.longitude,
		'country_id': res.country.id,
		'locality': res.locality
	}

	countries_dict = {
		'id': res.country.id,
		'country_name': res.country.name
	}

	sensors_dict = {
		'id': [int(sensor.id) for sensor in res.sensors],
		'pollutant_id': [int(sensor.parameter.id) for sensor in res.sensors],
		'location_id': [int(res.id) for sensor in res.sensors]	#points to location_id for each sensor
		}

	#extract entire sensor parameter dict from the response for each sensor
	#keys of parameter are: id name  units displayName
	pollutants_dict = {
		'id': [sensor.parameter.id for sensor in res.sensors],	
		'name': [sensor.parameter.name for sensor in res.sensors],	
		'units': [sensor.parameter.units for sensor in res.sensors],	
		'display_name': [sensor.parameter.display_name for sensor in res.sensors]	
		}

	sensor_ids = [sensor.id for sensor in res.sensors]

	#convert location dict to df
	loc_df = DataFrame(loc_dict, index=[0])
	countries_df = DataFrame(countries_dict, index = [0])
	# sensors_df = DataFrame(sensors_dict, index = sensors_dict['id'])
	sensors_df = DataFrame(sensors_dict, index = range(len(sensors_dict['id'])))
	print('sensors before conv.', sensors_df.values)
	sensors_df = sensors_df.astype(int)	
	print('sensors after conv.', sensors_df.values)

	pollutants_df = DataFrame(pollutants_dict, index = pollutants_dict['id'])

	dfs = [loc_df, countries_df, sensors_df, pollutants_df]

	return sensor_ids, dfs

#date range defines how many days to get measurements from a sensor. limit is max # days. (1 measurement per day)
def get_sensor_aqi_resp(sensor_id, date_from, date_to, limit=365, page=1):
	"""
	Query each sensor at that location to get aggregated daily measurements with stat summaries
    each result in the results list will be the measurements data for one day

    res = api.measurements.list(sensor_id, rollup='daily', limit=1000, page=?)
        additional params: datetime_from, datetime_to
    res.results[0].value
    res.results[0].summary.min
                          .max
                          .sd
	"""

	#Prepare authorization for get request
	#TODO: remove manual date
	# date_from = '2025-01-10'
	params = {
		'datetime_from': date_from,
		'datetime_to': date_to,
		'limit': limit,
		'rollup': 'daily'	# aggregates measurements as daily avgs
	}

	# Define response as None before attempting to make a request
	response = None
	try:
		# send get request
		response = api.measurements.list(sensor_id, **params)
		
		# check rate limit + sleep if nec.
		check_rate_limit(response)

	# catch exception only if rate limit. otherwise, will raise
	except RateLimitError:
		# sleep 30s to back off the request rate limit
		time.sleep(30)

		# recurs. call func again with same request. Only works if issue is rate limit.
		get_sensor_aqi_resp(sensor_id, date_from, date_to, limit=limit, page=page)
	
	except TypeError as e:
		raise Exception(e)
	
	# returns None if request failed, handled downstream
	return response	


def sensor_res_to_df(response, location_id):	#select desired data to retain from entire json object. json object may contain mulitple days of sensor data for each sensor
	results = response.results
	found = len(response.results)

	#define datetime format string for aqi table
	fmt_str = '%Y-%m-%d %T'

	#extract all dates from each result entry in results json object
	data = {
		'datetime': [
			fromiso(result.period.datetime_to.local).strftime(fmt_str)
			for result in results],
		'location_id': [location_id] * len(results),
		'pollutant_id': [result.parameter.id for result in results],
		'value': [result.value for result in results],
		'min_val': [result.summary.min for result in results],
		'max_val': [result.summary.max for result in results],
		'sd': [result.summary.sd for result in results]
	}

	return DataFrame(data, index=range(found))

# Establish client connection with OpenAQ - air quality API
# def multi_aqi_request_to_df(sensor_ids: list[str], location_id: str, date_from, date_to: datetime) -> pd.DataFrame | None:
def multi_aqi_request_to_df(sensor_ids, location_id, date_from, date_to):
	
	#initiate dataframe to None
	aqi_cols = ['datetime', 'location_id', 'pollutant_id', 'value', 'min_val', 'max_val', 'sd']
	aqi_df = DataFrame(columns = aqi_cols)

	#loop over sensor ids, get sensor json response, then format it to extract needed parameters
	for sensor_id in sensor_ids:		
		#call get_info func
		res = get_sensor_aqi_resp(sensor_id, date_from, date_to)
		#catch None response, !200 status_code
		if res == None or not res.meta.found:
			#break out of current iteration of loop (current sensor_id)
			continue

		#extract desired data from json object
		aqi_df_temp = sensor_res_to_df(res, location_id)	
		
		#if aqi_df dataframe for responses not appended yet, set it to the dataframe with the dict data
		if aqi_df.empty or aqi_df.isna().all().all():
			aqi_df = aqi_df_temp

		#if it already exists, make a temp dataframe with current dict data, and concatenate the two. 
		else:
			aqi_df = pd.concat([aqi_df, aqi_df_temp], ignore_index=True)
	
	return aqi_df


