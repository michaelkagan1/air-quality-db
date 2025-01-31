#DONE: resolve NaN issue - converted to None in values generation
#DONE: moved param init to globals ahead of main, added tracking of successful inserts and fails during inserting, added check to prevent redundant insertion to 4 dfs
#DONE: handle isoformat string edge cases: ValueError: Invalid isoformat string: '2025-01-28T00:00:00Z'
from connectdb import connect_db
from extract_data import *
from pathlib import Path
from wakepy import keep
import logging
# import pdb

#TODO: prevent redundant table inserts when locations are all known, especially for locations, sensors, countries tables. Just check if id in table. 
#TODO: handle negative and zero values in database

#establish path to current directory
path = Path(__file__).parent

#setup logger and config log level and output format
#options for logger are: logger.debug(), info(), warning(), error()
logger = logging.getLogger(__name__)
logging.basicConfig(
                filename=path/'etl.log',
                level=logging.INFO,
                format='%(asctime)s || %(levelname)s: %(message)s',
                force=True
                )   

#Call import for location ids. For testing use short list, later full list
#use pathlib notation for defining path. static is directory in current dir.
filename = 'test_locations list.csv'
filepath = path/'static'/filename
with filepath.open(mode='r') as f:
		reader = csv.reader(f)
		location_ids  = list(reader)[0]

#date_from is the most recent (or max) date from the datetime column. Returns as datetime object

# curs.execute('SELECT MAX(datetime) FROM aqi')
# date_from = curs.fetchone()[0]
# date_from = date_from.date().isoformat()

#TODO: remove manual dates
date_from = '2024-11-01'
# date_to = '2024-11-30'

#define date ranges for getting aqi data: date_to is todays date.
date_to = date.today().isoformat()

# initalize counters for summary
locations_success = set()
total_aqi_inserts = 0
table_exceptions = {
	'countries': 0, 
	'pollutants': 0, 
	'locations': 0, 
	'sensors': 0,   
	'aqi': 0  
}             


#main ETL script
def main():

	#log program start
	logger.info('%s: ETL main started.', datetime.now().ctime())

	#Establish connection and cursor with database as IAM user
	cnx, curs = connect_db()

	logger.info(f'Fetching AQI data from {date_from} to {date_to}.')

	for loc_id in location_ids:
		# print('Starting location: ', loc_id)
		#send location endpoint request and return json object of response
		loc_response = get_location_response(loc_id, to_print=False)
		
		if loc_response is None: # or loc_response.results[0]:
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

		# check if sensor already in DB: if so, sensor, pollutant, location, and country insert is redundant. redefine tables and dfs as aqi info only
		if sensor_in_db(curs, sensor_id=sensors_df.loc[0]['id']):
			tables = tables[-1]
			dataframes = dataframes[-1]

		lines_commited = 0
		for tablename, df in zip(tables, dataframes):	#zip so each table and source dataframe can be associated with eachother. 
			#insert to all 5 tables in db
			insert_df_to_db(curs, tablename, df)
			lines_commited += df.shape[0]	

		#commit changes to sql. (like save)
		cnx.commit()
		logger.info(f'{lines_commited} lines commited for location {loc_id}')
		print(f'{lines_commited} lines inserted for location {loc_id}')
	return

#helper function for inserting a df to associated table in aqi database 
def insert_df_to_db(curs, tablename, df):
	global total_aqi_inserts  # Add this line to modify the global variable

	# extract column headers from dataframe
	head = df.columns.to_list()
	
	# print(f'before conversion: {df.iloc[0]}')
	# # recast numpy NaN type for any null values to None
	# df = df.where(pd.notna(df), None)
	# print(f'after conversion: {df.iloc[0]}')

	#make string of '%s' pollutants for each value that will be inserted in each row. One per column in a dataframe. Use # of pollutants in the header list.
	placeholder = ', '.join(['%s']*len(head))

	#convert header list to a tuple, all in a string. also change single quotes to backticks.
	head = str(tuple(head)).replace("'","`")

	#Execute query: 1) insert table name, column headers string, and %s placeholder string (for prepared statement format)
	#Update id = id "resets" the id to itself if a key constraint is triggered, id is not changed, row is not altered, insert continues.
	query = "INSERT INTO `{}` {} VALUES ({}) ON DUPLICATE KEY UPDATE id = id".format(tablename, head, placeholder) 

	# Change df into list of tuples, to plug into 'insert many' method. List of tuples is argument for insert_many. 
	# Each pollutant in list is a separate set of values to be inserted. 
	if tablename == 'sensors':	#sensors_df all integers. if not explicitly converted to ints, they will be retrieved as numpy.ints from df.values, which is not compatible w mysql insert
		values = [tuple(int(x) for x in row) for row in df.values]
	
	else:
		# list of rows from df, each as a tuple. If any NaNs, converted to None for compat. with SQL
		values = [tuple(None if pd.isna(x) else x for x in row)\
			 for row in df.values] 

	try:	#Try inserting into each table, print error on fail and keep looping
		curs.executemany(query, values)
		if tablename == 'aqi':	# only count actual measurement values that got inserted
			locations_success.add(df.loc[0]['location_id'])	# add location id from the first row to set of locations that went through
			total_aqi_inserts += len(values)

	except KeyboardInterrupt:
		raise()

	except Exception as e:
		table_exceptions[tablename] += 1	#count exception for tracking
		logger.warning(f'Table {tablename} insert unsuccessfull: %s', e)
		logger.warning(df.head())
		return

def sensor_in_db(curs, sensor_id):
	query = 'SELECT id FROM sensors WHERE id = %s'
	sensor_id = [int(sensor_id)]
	curs.execute(query, sensor_id)
	rows = curs.fetchall()
	if rows:
		print(f'{sensor_id} found in DB already.')
	return rows is True		# returns True only if not empty set


if __name__ == '__main__':
	# prevent screen from sleeping during execution
	with keep.running():
		main()
		print('='*50, '\n', 'IMPORT COMPLETE\n', f'{total_aqi_inserts} aqi measurements added.',
		f'{len(locations_success)}/ {len(location_ids)} locations returned data.', '\n')

		logger.info('='*50)
		logger.info(f'\nETL Summary:')
		logger.info(f'Date range: {date_from} to {date_to}.')
		logger.info(f'{total_aqi_inserts} aqi measurements added.')
		logger.info(f'{len(locations_success)}/ {len(location_ids)} locations returned data.')
		logger.info(f'Table insert exceptions: \n{table_exceptions}')

