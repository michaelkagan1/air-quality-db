import boto3
import mysql.connector as sqlconnector
import os
from datetime import datetime
from dotenv import load_dotenv
import streamlit as st

#Extract api keys and connection info
load_dotenv()

DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_PORT = os.getenv('DB_PORT')
DB_REGION = os.getenv('DB_REGION')
DB_IAMUSER = os.getenv('DB_IAMUSER')

aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')

def get_token():	#obtain token
	client = boto3.client(
		'rds', 
		aws_access_key_id = aws_access_key_id,
		aws_secret_access_key = aws_secret_access_key,
		region_name = DB_REGION
	)

	TOKEN = client.generate_db_auth_token(DB_HOSTNAME, DB_PORT, DB_IAMUSER, DB_REGION)
	if not TOKEN:
		raise Exception('Token request failed!')
	print(f'Token obtained {str(datetime.now())}... \n')
	return TOKEN

def connect_db():	#establish connection
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