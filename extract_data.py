"""
The purpose of this .py file is to establish connection with the MYSQL server and the air quality API.
Send a request, save the data as a pandas df, then insert it into database. 
"""

#Import dependencies
import os, sys, requests, json, csv, requests
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

#
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

def create_db_engine_sa():
	TOKEN = get_token()
	#TOKEN = TOKEN.split('=')[-1]
	connection_string = f'mysql+pymysql://{IAMUSER}:{TOKEN}@{HOSTNAME}:{PORT}/{DBNAME}'
	connect_args = {'ssl': {'ca': 'info/us-east-2-bundle.pem'}}
	engine = sa.create_engine(connection_string, connect_args=connect_args)
	return engine, connection_string

#Get location info from location endpoint - taking location id as argument
loc_id=2178
def get_loc(loc_id):
	client = OpenAQ(api_key = KEY)	#This gets API Key automatically by searching environmental variables for OPENAQ-API-KEY, which is saved there already.
	loc = client.locations.get(loc_id)
	
	if not loc:
		print('API request failed!')
		sys.exit()
	print('API request successful')

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

def get_sensor_aqi(sensor_id, limit=20, page=1):
	#Prepare URL endpoint
	MEASUREMENT_DAY_ENDPOINT = '/v3/sensors/{sensor_id}/measurements/daily'
	URL = API_URL + f'{MEASUREMENT_DAY_ENDPOINT.replace("{sensor_id}", sensor_id)}'

	#Prepare authorization for get request
	params = {
		'datetime_to': '2024-12-26',
		'limit': limit,
		'page': page
	}
	headers = {
		'accept': 'application/json',
		'X-API-KEY': KEY
		}
	#send get request
	response = requests.get(URL, headers=headers, params=params)
	#catch error
	if response.status_code != 200:
		print(f'Error: {response.status_code}, {response.text}')
	return response	


def format_sensor_info(jres, location_id):	#select desired data to retain from entire json object
	data = {}
	results = jres['results']
	
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



def get_latest_loc_aqi(loc_id, limit=20, page=1):		#location id data: str
	#Prepare URL endpoint
	LATEST_LOC_ENDPOINT = '/v3/locations/{location_id}/latest'
	URL = API_URL + f'{LATEST_LOC_ENDPOINT.replace("{location_id}", loc_id)}'

	#Prepare authorization for get request
	params = {
		'datetime_to': '2024-12-26',
		'limit': limit,
		'page': page
	}
	headers = {
		'accept': 'application/json',
		'X-API-KEY': KEY
		}
	#send get request
	response = requests.get(URL, headers=headers)
	#catch error
	if response.status_code != 200:
		print(f'Error: {response.status_code}, {response.text}')
	return response	


def format_latest_loc_aqi(jres):	#select desired data to retain from entire json object
	data = {}
	coordinates = {}
	results = jres['results']
	#extract all dates from each result entry in results json object
	data['datetime'] = [fromisoformat(result['datetime']['local']) for result in results]
	data['sensor_id'] = [result['sensorsId'] for result in results]
	data['location_id'] = [result['locationsId'] for result in results]
	data['value'] = [result['value'] for result in results]
	coordinates['location_id'] = results[0]['locationsId']
	coordinates['lat'] = results[0]['coordinates']['latitude']
	coordinates['long'] = results[0]['coordinates']['longitude']
	return data, coordinates


def stream_latest_data(loc_id):
	#call get func to make api call for data
	res = get_latest_loc_aqi(loc_id)

	#convert response to string then laod into json object
	json_res = json.loads(res.text)

	#call format func to extract individual sensor data as dict, & coordinate data as dict
	#This coord data will be sent to database and inserted if unique
	aqi, coordinates = format_latest_loc_aqi(json_res)

'''
Tables to create in mysql database for aqi storage:
sensors
	- sensor id
	- element id
	- location id
	- PRIMARY KEY('sensor_id')
	- FOREIGN KEY('element id') REFERENCES `elements`(`id`)
	- FOREIGN KEY('location id') REFERENCES `locations`(`id`)

locations
	- id
	- coordinates
	- country
	- city
	- PRIMARY KEY('id')

elements
	- id
	- name
	- units
	- PRIMARY KEY('id')

aqi
	- id
	- datetime
	- sensorsId
	- locationsId
	- value
	- PRIMARY KEY('sensor id', 'datetime')
	- FOREIGN KEY('element id') REFERENCES `elements`(`id`)
	- FOREIGN KEY('location id') REFERENCES `locations`(`id`)
	
1. Create func/file to connect with mysql db and 
. Create continuous listener with 'Automator' or 'crontab -e'
	- infinite while true loop that starts up w/ comp and runs cont. in background
	- 
queue for if file crashes - for safety

'''
	


#Establish client connection with OpenAQ - air quality API
def get_aqi(sensor_ids, location_id):
	#initiate dict to store sensor info
	res_df = DataFrame()
	
	#extract individual sensor details for each sensor id
	for sensor_id in sensor_ids:		#loop sensor ids, get sensor json response, then format it to extract needed parameters
		#call get_info func
		res = get_sensor_aqi(str(sensor_id), limit=5)

		#convert resonse to json format
		json_res = json.loads(res.text)

		#extract desired data from json object
		res_dict = format_sensor_info(json_res, location_id)	
		res_df_temp = DataFrame(res_dict)	
		res_df = pd.concat([res_df, res_df_temp], ignore_index=True)
	
	return res_df
