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
"""

#Import dependencies
import os, sys, requests, json, csv, requests
import time
from datetime import datetime 
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

OPENAQ_API_KEY = os.getenv('OPENAQ_API_KEY')
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_PORT = os.getenv('DB_PORT')
DB_REGION = os.getenv('DB_REGION')
DB_IAMUSER = os.getenv('DB_IAMUSER')

#declare non-secret info
API_URL = 'https://api.openaq.org'


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

	curs = cnx.cursor()
	return cnx, curs


def check_rate_limit(json_response):
	#headers dict in the response contains following format: {'xRatelimitLimit': 60, 'xRatelimitRemaining': 59, 'xRatelimitUsed': 1, 'xRatelimitReset': 60}. reset_time is time in seconds until rate limit is reset. 
	req_limit, reqs_remaining, reqs_used, reset_time = json_response['headers'].values()
	
	#check if limit met, and if true, sleep for reset_time seconds
	if reqs_remaining = 0:
		time.sleep(reset_time)

#Get location info from location endpoint - taking location id as argument
# TODO: apply check_rate_limit func to every api_call. Either in function or in main
def get_loc(loc_id):
	client = OpenAQ(api_key = OPENAQ_API_KEY)	

	#define max number of retries and delay time in s
	max_attempts = 3
	delay = 10

	loc = None

	#Apply exponential backup to resolve too many requests error
	for i in range(max_attempts):
		try:
			loc = client.locations.get(loc_id)

			#if above line executes, break will break out of loop to prevent redundant requests
			break
		except RateLimitError:
			#sleep to back off the request rate limit
			time.sleep(delay)
			
			#exponentially increase delay time
			delay *= 2

			print(f'Trying location {loc_id}: attempt {i+1}')
			

	#if loc still didn't get executed or returned an empty list, (aka, loc variable doesn't exist) return None
	if not loc:
		return None

	jloc = loc.json()
	jloc_data = json.loads(jloc)
	return jloc_data

def transform_loc(jloc_data):
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
def get_sensor_aqi(sensor_id, date_from, date_to, limit=40, page=1) ->json | None:
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

	# TODO: Extract "retry()" into reusable function
	# Maybe use `lambda`
	def make_sensor_request():
		return requests.get(...)


	#define attempts and time delay for exponential back-off
	max_attempts = 3
	delay = 10

	#Define response as None before attempting to make a request
	response = None
# TODO: get rid of try/catch blocks
	#Apply exponential back-off to resolve too many requests error
	for i in range(max_attempts):
		try:
			#send get request
			response = requests.get(URL, headers=headers, params=params)

			#if response succeeds, break loop
			break
		except RateLimitError:
			#sleep to back off the request rate limit
			time.sleep(delay)
			
			#exponentially increase delay time
			delay *= 2

	return response	


def format_sensor_info(jres, location_id):	#select desired data to retain from entire json object
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
def get_aqi(sensor_ids: list[str], location_id: str, date_from, date_to: datetime) -> pd.DataFrame | None:
	#initiate dict to store sensor info
	
	#extract individual sensor details for each sensor id
	for sensor_id in sensor_ids:		#loop sensor ids, get sensor json response, then format it to extract needed parameters
		#call get_info func
		res = get_sensor_aqi(str(sensor_id), date_from, date_to)
		
		#catch None response
		if res == None:
			return None

		#If status code is not 200, return None
		if res.status_code != 200:
			return None
		
		#convert resonse to json format
		json_res = json.loads(res.text)

		#if no results in response, return None instead of formatting
		if len(json_res['results']) == 0:
			return None

		#extract desired data from json object
		res_dict = format_sensor_info(json_res, location_id)	
		
		#instantiate results datafame 
		res_df = None

		#if res_df dataframe for responses not appended yet, set it to the dataframe with the dict data
		if res_df is None:
			res_df = DataFrame(res_dict)	

		#if it already exists, make a temp dataframe with current dict data, and concatenate the two. 
		else:
			res_df_temp = DataFrame(res_dict)	
			res_df = pd.concat([res_df, res_df_temp], ignore_index=True)
	
	return res_df


#retrieves ids of all target locations from 'locations list.csv' file
def pull_location_ids(filename):
        #open file in read mode, read line to list object and return list
        with open(filename, 'r', newline='') as f:
                reader = csv.reader(f)
                data = list(reader)[0]
        return data
