"""

#DONE: Use builtin datetime methods (isoformat())
#DONE: use pathlib to navigate to static dir to get filename
"""
from extract_data import *
from pathlib import Path
import logging
import pdb

#setup logger and config log level and output format
#options for logger are: logger.debug(), info(), warning(), error()
logger = logging.getLogger(__name__)
logging.basicConfig(
                filename='etl.log',
                level=logging.INFO,
                format='%(asctime)s || %(levelname)s: %(message)s',
                force=True
                )   

#establish root bath
path = Path('.')

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

	logger.info('%s: Starting API requests.', datetime.now().ctime())
	
	#define date ranges for getting aqi data: date_to is todays date.
	date_to = date.today().isoformat()

	#date_from is the most recent (or max) date from the datetime column. Returns as datetime object
	curs.execute('SELECT MAX(datetime) FROM aqi')
	date_from = curs.fetchone()[0]
	date_from = date_from.date().isoformat()

	date_from = '2024-12-01'
	date_to = '2024-12-31'

	logger.info(f'Fetching AQI data from {date_from} to {date_to}.')

	for loc_id in location_ids:
		#send location endpoint request and return json object of response
		json_loc = get_location_response(loc_id)
		
		if json_loc is None:
			continue

		sensor_ids, dfs = location_json_to_dfs(json_loc)

		#unpack dataframes from dfs
		locations_df, countries_df, sensors_df, elements_df = dfs

		#get dataframe of all sensor aqi data at location at loc_id
		aqi_df = multi_aqi_request_to_df(sensor_ids, loc_id, date_from, date_to)

		if aqi_df.empty:	
			continue

		#Prepare tables, column headers and data frames for inserting into sql
		tables = ['countries', 'elements', 'locations', 'sensors', 'aqi']	#table names in SQL 
		dataframes = [countries_df, elements_df, locations_df, sensors_df, aqi_df]	#dataframes in the same order

		for tablename, df in zip(tables, dataframes):	#zip so each table and source dataframe can be associated with eachother. 
			#insert to all 5 tables in db
			insert_df_to_db(curs, tablename, df)

		#commit changes to sql. (like save)
		cnx.commit()
	
	return

#helper function for inserting a df to associated table in aqi database 
def insert_df_to_db(curs, tablename, df):
	# extract column headers from dataframe
	head = df.columns.to_list()

	#make string of '%s' elements for each value that will be inserted in each row. One per column in a dataframe. Use # of elements in the header list.
	placeholder = ', '.join(['%s']*len(head))

	#convert header list to a tuple, all in a string. also change single quotes to backticks.
	head = str(tuple(head)).replace("'","`")

	#Execute query: 1) insert table name, column headers string, and %s placeholder string (for prepared statement format)
	#elements table had displayName added, so I want this col to be updated by future inserts (which now include this data), only for this table
	if 'displayName' in df.columns:
		query = "INSERT INTO `{}` {} VALUES ({}) ON DUPLICATE KEY UPDATE displayName = VALUES(displayName)".format(tablename, head, placeholder) 

	else:
		#IGNORE used to ignore duplicate entries for other tables
		query = "INSERT IGNORE INTO `{}` {} VALUES ({})".format(tablename, head, placeholder) 

	#row by row, change list into tuple, to plug into 'insert many' method. List of tuples is argument for insert_many. Each element in list is a separate set of values to be inserted. 
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

