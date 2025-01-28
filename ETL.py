"""
"""
from connectdb import connect_db
from extract_data import *
from pathlib import Path
import logging

#TODO: prevent redundant table inserts when locations are all known, especially for locations, sensors, countries tables. Just check if id in table. 
#TODO: handle negative and zero values in database

#establish path to current directory
path = Path(__file__).parent

#setup logger and config log level and output format
#options for logger are: logger.debug(), info(), warning(), error()
logger = logging.getLogger(__name__)
logging.basicConfig(
                filename=path/'etl.testlog',
                level=logging.INFO,
                format='%(asctime)s || %(levelname)s: %(message)s',
                force=True
                )   

#main ETL script
def main():

	#log program start
	logger.info('%s: ETL main started.', datetime.now().ctime())

	#Establish connection and cursor with database as IAM user
	cnx, curs = connect_db()

	#Call import for location ids. For testing use short list, later full list
	filename = 'locations list.csv'
	
	#use pathlib notation for defining path. static is directory in current dir.
	location_ids = location_ids_from_file(path/'static'/filename)
	
	#define date ranges for getting aqi data: date_to is todays date.
	date_to = date.today().isoformat()

	#date_from is the most recent (or max) date from the datetime column. Returns as datetime object
	curs.execute('SELECT MAX(datetime) FROM aqi')
	date_from = curs.fetchone()[0]
	date_from = date_from.date().isoformat()

	logger.info(f'Fetching AQI data from {date_from} to {date_to}.')

	for loc_id in location_ids:
		#send location endpoint request and return json object of response
		loc_response = get_location_response(loc_id)
		
		if loc_response is None:
			continue

		sensor_ids, dfs = location_res_to_dfs(loc_response)

		#unpack dataframes from dfs
		locations_df, countries_df, sensors_df, pollutants_df = dfs

		#get dataframe of all sensor aqi data at location at loc_id
		aqi_df = multi_aqi_request_to_df(sensor_ids, loc_id, date_from, date_to)

		if aqi_df.empty:	
			continue

		#Prepare tables, column headers and data frames for inserting into sql
		tables = ['countries', 'pollutants', 'locations', 'sensors', 'aqi']	#table names in SQL 
		dataframes = [countries_df, pollutants_df, locations_df, sensors_df, aqi_df]	#dataframes in the same order
		lines_commited = 0
		for tablename, df in zip(tables, dataframes):	#zip so each table and source dataframe can be associated with eachother. 
			#insert to all 5 tables in db
			insert_df_to_db(curs, tablename, df)
			lines_commited += df.shape[0]	

		#commit changes to sql. (like save)
		#cnx.commit()
		logger.info(f'{lines_commited} lines commited for location {loc_id}')
		print(f'{lines_commited} lines commited for location {loc_id}')
	return

#helper function for inserting a df to associated table in aqi database 
def insert_df_to_db(curs, tablename, df):
	# extract column headers from dataframe
	head = df.columns.to_list()

	#make string of '%s' pollutants for each value that will be inserted in each row. One per column in a dataframe. Use # of pollutants in the header list.
	placeholder = ', '.join(['%s']*len(head))

	#convert header list to a tuple, all in a string. also change single quotes to backticks.
	head = str(tuple(head)).replace("'","`")

	#Execute query: 1) insert table name, column headers string, and %s placeholder string (for prepared statement format)

	#Update id = id "resets" the id to itself if a key constraint is triggered, id is not changed, row is not altered, insert continues.

	query = "INSERT INTO `{}` {} VALUES ({}) ON DUPLICATE KEY UPDATE id = id".format(tablename, head, placeholder) 

	#row by row, change list into tuple, to plug into 'insert many' method. List of tuples is argument for insert_many. Each pollutant in list is a separate set of values to be inserted. 
	values = [(tuple(row)) for row in df.values] 

	#Try inserting into each table, print error on fail and keep looping
	try:
		curs.executemany(query, values)

	except Exception as e:
		logger.warning('Table insert unsuccessfull: %s', e)
		logger.warning(df.head())
		return

#retrieves ids of all target locations from 'locations list.csv' file. filename is file path as Pathlib object
def location_ids_from_file(filepath):
	#open file in read mode, read line to list object and return list
	with filepath.open(mode='r') as f:
			reader = csv.reader(f)
			data = list(reader)[0]
	return data


if __name__ == '__main__':
	main()

