"""
The purpose of this .py file is to establish connection with the MYSQL server and the air quality API.
Send a request, save the data as a pandas df, then insert it into database. 
"""
#TODO: change element(s) to pollutant(s)
#TODO: consider alternate api for more calls/ better data: https://aqicn.org/json-api/doc/
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

#declare non-secret info
API_URL = 'https://api.openaq.org'

def check_rate_limit(response):
	
	# only reqs_remaining and reset_time needed for now
	#response from get_location_response returns 'LocationsResponse' object. Other func returns json.
	if type(response).__name__ == 'LocationsResponse':
		requests_remaining = response.headers.x_ratelimit_remaining
		reset_time = response.headers.x_ratelimit_reset

	#response from get_aqi_json with no results may return 'Response' object that is not subscriptable
	elif type(response).__name__ == 'Response':
		requests_remaining = response.headers.get('X-Ratelimit-Remaining')
		requests_remaining = response.headers.get('X-Ratelimit-Reset')
		
	else:
		#edge case, don't sleep and just return to script
		return

	#check if limit met, and if true, sleep for reset_time seconds. offset of 4 requests added for safety
	if int(requests_remaining) < 4:
		time.sleep(reset_time)

#Get location info from location endpoint - taking location id as argument
def get_location_response(loc_id):
	client = OpenAQ(api_key = OPENAQ_API_KEY)	

	#Apply exponential backup to resolve too many requests error
	try:
		loc_response = client.locations.get(loc_id)

		#pass json loc response to check rate limit + sleep if nec.
		check_rate_limit(loc_response)
		
		#convert response to json string
		json_loc_response = loc_response.json()

		#if request works, return json object
		return json.loads(json_loc_response)

	#In case there's an issue with the rate limit check, exception will be caught, any other exception will be raised
	except RateLimitError:	#exception object from OpenAQ sdk

		#sleep 30s to back off the request rate limit
		time.sleep(30)

		#Call get_loc recursively with the same loc_id. This is only safe if the exception is rate limit, so it won't be inf. loop.
		get_location_response(loc_id)

	#if try block didn't succeed, None response is handled in main()
	return None  

def location_json_to_dfs(json_loc_data):
	res = json_loc_data['results'][0]

	loc_dict = {
		'id': res['id'],
		'latitude': res['coordinates']['latitude'],
		'longitude': res['coordinates']['longitude'],
		'country_id': res['country']['id'],
		'locality': res['locality']
	}

	countries_dict = {
		'id': res['country']['id'],
		'country_name': res['country']['name']
	}

	sensors_dict = {
		'id': [sensor['id'] for sensor in res['sensors']],
		'element_id': [sensor['parameter']['id'] for sensor in res['sensors']],
		'location_id': [res['id'] for sensor in res['sensors']]	#points to location_id for each sensor
		}

	#extract entire sensor parameter dict from the response for each sensor
	#keys of parameter are: id name  units displayName
	elements_dict = [sensor['parameter'] for sensor in res['sensors']]	# -> list of dicts

	sensor_ids = [sensor['id'] for sensor in res['sensors']]

	#convert location dict to df
	loc_df = DataFrame(loc_dict, columns = range(len(loc_dict)))
	countries_df = DataFrame(countries_dict, columns = range(len(countries_dict)))
	sensors_df = DataFrame(sensors_dict, columns = range(len(sensors_dict)))
	elements_df = DataFrame(elements_dict)

	dfs = [loc_df, countries_df, sensors_df, elements_df]

	return sensor_ids, dfs

#date range defines how many days to get measurements from a sensor. limit is max # days. (1 measurement per day)
def get_sensor_aqi_json(sensor_id, date_from, date_to, limit=40, page=1):
	#Prepare URL endpoint
	MEASUREMENT_DAY_ENDPOINT = '/v3/sensors/{sensor_id}/measurements/daily'
	URL = API_URL + MEASUREMENT_DAY_ENDPOINT.format(sensor_id=sensor_id)

	#Prepare authorization for get request
	params = {
		'datetime_from': date_from,
		'datetime_to': date_to,
		'limit': limit,
		'page': page
	}
	headers = {
		'accept': 'application/json',
		'X-API-KEY': OPENAQ_API_KEY
		}

	#Define response as None before attempting to make a request
	response = None
	try:
		#send get request
		response = requests.get(URL, headers=headers, params=params)

		#pass response through rate limit checker, sleep if necessary. response must be passed, not json, because headers might not be in json object
		if response.ok:
			check_rate_limit(response)

	#catch exception only if rate limit. otherwise, will raise
	except RateLimitError:
		#sleep 30s to back off the request rate limit
		time.sleep(30)

		#recurs. call func again with same request. Only works if issue is rate limit.
		get_sensor_aqi_json(sensor_id, date_from, date_to, limit=40, page=1)
	
	except TypeError as e:
		raise Exception(e)
	
	#returns None if request failed, handled downstream
	return response	


def sensor_json_to_df(json_res, location_id):	#select desired data to retain from entire json object. json object may contain mulitple days of sensor data for each sensor
	results = json_res['results']

	#extract number of found results from metadata
	found = json_res.get('meta').get('found')

	#define datetime format string for aqi table
	fmt_str = '%Y-%m-%d %T'

	#extract all dates from each result entry in results json object
	data = {
		'datetime': [
			fromiso(result['period']['datetimeTo']['local']).strftime(fmt_str) 
			for result in results],
		'location_id': [location_id] * len(results),
		'element_id': [result['parameter']['id'] for result in results],
		'value': [result['value'] for result in results],
		'min_val': [result['summary']['min'] for result in results],
		'max_val': [result['summary']['max'] for result in results],
		'sd': [result['summary']['sd'] for result in results]
	}

	return DataFrame(data, index=range(found))

#Establish client connection with OpenAQ - air quality API
def multi_aqi_request_to_df(sensor_ids: list[str], location_id: str, date_from, date_to: datetime) -> pd.DataFrame | None:
	
	#initiate dataframe to None
	aqi_cols = ['datetime', 'location_id', 'element_id', 'value', 'min_val', 'max_val', 'sd']
	aqi_df = DataFrame(columns = aqi_cols)

	#loop over sensor ids, get sensor json response, then format it to extract needed parameters
	for sensor_id in sensor_ids:		
		#call get_info func
		res = get_sensor_aqi_json(str(sensor_id), date_from, date_to)
		#catch None response, !200 status_code
		if res == None or not res.ok:

			#break out of current iteration of loop (current sensor_id)
			continue

		#convert resonse to json format
		json_response = res.json()

		#if no results in response, proceed to next loop iter
		if json_response.get('meta').get('found') == 0:
			continue

		#extract desired data from json object
		aqi_df_temp = sensor_json_to_df(json_response, location_id)	
		
		#if aqi_df dataframe for responses not appended yet, set it to the dataframe with the dict data
		if aqi_df.empty or aqi_df.isna().all().all():
			aqi_df = aqi_df_temp

		#if it already exists, make a temp dataframe with current dict data, and concatenate the two. 
		else:
			aqi_df = pd.concat([aqi_df, aqi_df_temp], ignore_index=True)
	
	return aqi_df


