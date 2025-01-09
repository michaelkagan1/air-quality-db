"""
The purpose of this .py file is to establish connection with the MYSQL server and the air quality API.
Send a request, save the data as a pandas df, then insert it into database. 

done on 1/7:
	implemented RateLimitError instead of try/except blocks
	renamed objects/ variables/ functions to be clearer to understand
	implemented type hints
# DONE: Make clearer which service each is for using prefixes
# DONE: Build appropriate internal rate limiting so we avoid 429s
# DONE: Use rate limit header information to control requests. Create check_rate_limit func to see when limit is reached, and sleep for the time until reset
# DONE: Use dictionary literal syntax for clarity
# DONE: Use builtin formatting features
# DONE: Don't use locals!

1/8:
# TODO: apply check_rate_limit func to every api_call. Either in function or in main
# TODO: get rid of try/catch blocks
# TODO: resolve bug in get_aqi: premature return if a sensor_id is None or doesn't return a response
# Renamed functions for clearer role

1/9:
#TODO: resolve response handling from line 183 - response must be converted to .json() first. Handle case where there is no header (and doesn't need to sleep)

"""

#Import dependencies
import os, sys, requests, json, csv, requests
import time
from datetime import datetime, date
from dotenv import load_dotenv
import boto3
import mysql.connector as sqlconnector
import pymysql
from openaq import OpenAQ, RateLimit as RateLimitError
from pandas import DataFrame
import pandas as pd
fromisoformat = datetime.fromisoformat

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

#Define static column headers for database tables
aqi_cols = ['datetime', 'location_id', 'element_id', 'value', 'units', 'min_val', 'max_val', 'sd']
locations_cols = ['location_id', 'latitude', 'longitude', 'country_id', 'locality']
countries_cols = ['country_id', 'country_name']
elements_cols = ['element_id', 'element_name', 'units']
sensors_cols = ['sensor_id', 'element_id', 'location_id']



def get_token():	#obtain token
	client = boto3.client('rds')
	TOKEN = client.generate_db_auth_token(DB_HOSTNAME, DB_PORT, DB_IAMUSER, DB_REGION)
	if not TOKEN:
		raise Exception('Token request failed!')
	print(f'Token obtained... \n')
	return TOKEN


def connect_db():	#obtain connection
	TOKEN = get_token()
	config = {
		'host': DB_HOSTNAME,
		'port': DB_PORT,
		'user': DB_IAMUSER,
		'password': TOKEN,
		'auth_plugin': 'mysql_clear_password'
		}
	cnx = sqlconnector.connect(**config)

			#verify connection
	if cnx.is_connected():
		print('DB connection established...')
	else:
		raise Exception('DB connection failed')

	#set cursor to execute commands + queries in mysql server
	curs = cnx.cursor()

	#connect to aqi database
	curs.execute('USE aqi')

        #clear cursor result for future queries
	curs.fetchall()

	return cnx, curs	#returns cnx and curs, with cursor already "in" aqi db

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

	#check if limit met, and if true, sleep for reset_time seconds
	if requests_remaining < 2:
		time.sleep(reset_time)

#Get location info from location endpoint - taking location id as argument
def get_location_response(loc_id):
	client = OpenAQ(api_key = OPENAQ_API_KEY)	

	#define max number of retries and delay time in s

	loc_response = None

	#Apply exponential backup to resolve too many requests error
	try:
		loc_response = client.locations.get(loc_id)

		#pass json loc response to check rate limit + sleep if nec.
		check_rate_limit(loc_response)
		
		#convert response to 
		json_loc_response = loc_response.json()
		
		return json.loads(json_loc_response)	#if loc_response is None, None response is handled in main()

	#In case there's an issue with the rate limit check, exception will be caught, any other exception will be raised
	except RateLimitError:	#exception object from OpenAQ sdk

		#sleep 30s to back off the request rate limit
		time.sleep(30)

		#Call get_loc recursively with the same loc_id. This is only safe if the exception is rate limit, so it won't be inf. loop.
		get_location_response(loc_id)
		

def location_json_to_df(jloc_data):
	res = jloc_data['results'][0]

	loc = {
		'location_id': res['id'],
		'latitude': res['coordinates']['latitude'],
		'longitude': res['coordinates']['longitude'],
		'locality': res['locality'],
		'country_name': res['country']['name'],
		'country_id': res['country']['id']
	}

	sensors = {
		'sensor_id': [sensor['id'] for sensor in res['sensors']],
		'element_id': [sensor['parameter']['id'] for sensor in res['sensors']],
		'element_name': [sensor['parameter']['name'] for sensor in res['sensors']]
		}

	sensor_ids = [sensor['id'] for sensor in res['sensors']]

	return sensor_ids, sensors, loc

#date range defines how many days to get measurements from a sensor. limit is max # days. (1 measurement per day)
def get_sensor_aqi_json(sensor_id, date_from, date_to, limit=40, page=1):
	#set temporary dates for dev

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
		check_rate_limit(response)

	#catch exception only if rate limit. otherwise, will raise
	except RateLimitError:
		#sleep 30s to back off the request rate limit
		time.sleep(30)

		#recurs. call func again with same request. Only works if issue is rate limit.
		get_sensor_aqi_json(sensor_id, date_from, date_to, limit=40, page=1)
	
	#returns None if request failed, handled downstream
	return response	

def sensor_json_to_df(jres, location_id):	#select desired data to retain from entire json object
	data = {}

	try:
		results = jres['results']
	except:
		pdb.set_trace()
	# TODO: re-order id naming to avoid it in main ETL script
	#extract all dates from each result entry in results json object
	data = {
		'datetime': [fromisoformat(result['period']['datetimeTo']['local']) for result in results],
		'location_id': [location_id] * len(results),
		'element_id': [result['parameter']['id'] for result in results],
		'element_name': [result['parameter']['name'] for result in results],
		'value': [result['value'] for result in results],
		'units': [result['parameter']['units'] for result in results],
		'min_val': [result['summary']['min'] for result in results],
		'max_val': [result['summary']['max'] for result in results],
		'sd': [result['summary']['sd'] for result in results]
	}

	return data

#Establish client connection with OpenAQ - air quality API
def multi_aqi_request_to_df(sensor_ids: list[str], location_id: str, date_from, date_to: datetime) -> pd.DataFrame | None:
	
	#initiate dataframe to None
	res_df = None

	#loop over sensor ids, get sensor json response, then format it to extract needed parameters
	for sensor_id in sensor_ids:		
		#call get_info func
		res = get_sensor_aqi_json(str(sensor_id), date_from, date_to)
		
		#catch None response, !200 status_code
		if res == None or res.status_code != 200:

			#break out of current iteration of loop (current sensor_id)
			continue

		#convert resonse to json format
		json_response = json.loads(res.text)

		#if no results in response, proceed to next loop iter
		if len(json_response['results']) == 0 or 'results' not in json_response.keys():
			continue

		#extract desired data from json object
		res_dict = sensor_json_to_df(json_response, location_id)	
		
		#if res_df dataframe for responses not appended yet, set it to the dataframe with the dict data
		if res_df is None:
			res_df = DataFrame(res_dict)	

		#if it already exists, make a temp dataframe with current dict data, and concatenate the two. 
		else:
			res_df_temp = DataFrame(res_dict)	
			res_df = pd.concat([res_df, res_df_temp], ignore_index=True)
	
	return res_df


