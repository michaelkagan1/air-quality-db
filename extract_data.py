"""
The purpose of this .py file is to establish connection with the MYSQL server and the air quality API.
Send a request, save the data as a pandas df, then insert it into database. 
"""

#Import dependencies
import os, sys, requests, json, csv, requests
import time
from datetime import datetime 
from dotenv import load_dotenv
import boto3
import mysql.connector as cpy
import sqlalchemy as sa
import pymysql
from openaq import OpenAQ
from pandas import DataFrame
import pandas as pd
fromisoformat = datetime.fromisoformat

#Extract api keys and connection info
load_dotenv()
KEY = os.getenv('OPENAQ_API_KEY')
HOSTNAME = os.getenv('HOSTNAME')
PORT = os.getenv('PORT')
REGION = os.getenv('REGION')
IAMUSER = os.getenv('IAMUSER')
DBNAME = 'aqi'

#declare non-secret info
API_URL = 'https://api.openaq.org'

def get_token():	#obtain token
	client = boto3.client('rds')
	TOKEN = client.generate_db_auth_token(HOSTNAME, PORT, IAMUSER, REGION)
	if not TOKEN:
		print('Token request failed!')
		sys.exit()
	print(f'Token obtained... \n')
	return TOKEN


def connect_db():	#obtain connection
	TOKEN = get_token()
	config = {
		'host': HOSTNAME,
		'port': PORT,
		'user': IAMUSER,
		'password': TOKEN,
		'auth_plugin': 'mysql_clear_password'
		}
	cnx = cpy.connect(**config)

			#verify connection
	if cnx.is_connected():
		print('DB connection established...')
	else:
		print('DB connection failed')
		sys.exit()

	curs = cnx.cursor()
	return cnx, curs

#Get location info from location endpoint - taking location id as argument
def get_loc(loc_id):
	client = OpenAQ(api_key = KEY)	#This gets API Key automatically by searching environmental variables for OPENAQ-API-KEY, which is saved there already.

	#define max number of retries and delay time in s
	max_attempts = 3
	delay = 10

	#Apply exponential backup to resolve too many requests error
	for i in range(max_attempts):
		try:
			loc = client.locations.get(loc_id)

			#if above line executes, break will break out of loop to prevent redundant requests
			break
		except Exception:
			#sleep to back off the request rate limit
			time.sleep(delay)
			
			#exponentially increase delay time
			delay *= 2

			print(f'Trying location {loc_id}: attempt {i+1}')

	#if loc still didn't get executed, (aka, loc variable doesn't exist) return None
	if 'loc'  not in locals():
		return None

	jloc = loc.json()
	jloc_data = json.loads(jloc)
	return jloc_data

def transform_loc(jloc_data):
	res = jloc_data['results'][0]

	loc = {}
	loc['location_id'] = res['id']
	loc['latitude'] = res['coordinates']['latitude']
	loc['longitude'] = res['coordinates']['longitude']
	loc['locality'] = res['locality']
	loc['country_name'] = res['country']['name']
	loc['country_id'] = res['country']['id']

	sensors = {}
	sensors['sensor_id'] = [sensor['id'] for sensor in res['sensors']]
	sensors['element_id'] = [sensor['parameter']['id'] for sensor in res['sensors']]
	sensors['element_name'] = [sensor['parameter']['name'] for sensor in res['sensors']]

	sensor_ids = [sensor['id'] for sensor in res['sensors']]
	return sensor_ids, sensors, loc

def get_sensor_aqi(sensor_id, date_from, date_to, limit=40, page=1):
	#set temporary dates for dev

	#Prepare URL endpoint
	MEASUREMENT_DAY_ENDPOINT = '/v3/sensors/{sensor_id}/measurements/daily'
	URL = API_URL + f'{MEASUREMENT_DAY_ENDPOINT.replace("{sensor_id}", sensor_id)}'

	#Prepare authorization for get request
	params = {
		'datetime_from': date_from,
		'datetime_to': date_to,
		'limit': limit,
		'page': page
	}
	headers = {
		'accept': 'application/json',
		'X-API-KEY': KEY
		}

	#define attempts and time delay for exponential back-off
	max_attempts = 3
	delay = 10

	#Apply exponential back-off to resolve too many requests error
	for i in range(max_attempts):
		try:
			#send get request
			response = requests.get(URL, headers=headers, params=params)

			#if response succeeds, break loop
			break
		except Exception:
			#sleep to back off the request rate limit
			time.sleep(delay)
			
			#exponentially increase delay time
			delay *= 2



	#catch error
	if 'response' not in locals():
		return None
		#print(f'Error: {response.status_code}, {response.text}')

	return response	


def format_sensor_info(jres, location_id):	#select desired data to retain from entire json object
	data = {}
	try:
		results = jres['results']
	except:
		pdb.set_trace()

	#extract all dates from each result entry in results json object
	data['datetime'] = [fromisoformat(result['period']['datetimeTo']['local']) for result in results]
	data['location_id'] = [location_id] * len(results)
	data['element_id'] = [result['parameter']['id'] for result in results]
	data['element_name'] = [result['parameter']['name'] for result in results]
	data['value'] = [result['value'] for result in results]
	data['units'] = [result['parameter']['units'] for result in results]
	data['min_val'] = [result['summary']['min'] for result in results]
	data['max_val'] = [result['summary']['max'] for result in results]
	data['sd'] = [result['summary']['sd'] for result in results]

	return data

#Establish client connection with OpenAQ - air quality API
def get_aqi(sensor_ids, location_id, date_from, date_to):
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

		#if res_df dataframe for responses not created yet, set it to the dataframe with the dict data
		if 'res_df' not in locals():
			res_df = DataFrame(res_dict)	

		#if it already exists, make a temp dataframe with current dict data, and concatenate the two. 
		else:
			res_df_temp = DataFrame(res_dict)	
			res_df = pd.concat([res_df, res_df_temp], ignore_index=True)
	
	return res_df
