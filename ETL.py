#TODO: check compatibility of progressbar with launchd background ops
#DONE: resolved conflict on updating duplicate values
#DONE: Added progress bar for cli output
#DONE: handled negative and zero values in database
from connectdb import connect_db
from extract_data import *
from pathlib import Path
from wakepy import keep
import psutil
import logging

from tqdm import tqdm	#library for progress bar in CLI

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
filename = 'locations list.csv'
filepath = path/'static'/filename
# filename = 'failed_locations.csv'
# filepath = path/'dev'/filename
with filepath.open(mode='r') as f:
		reader = csv.reader(f)
		location_ids  = list(reader)[0]

#Establish connection and cursor with database as IAM user
cnx, curs = connect_db()
	
#date_from is the most recent (or max) date from the datetime column. Returns as datetime object
curs.execute('SELECT MAX(datetime) FROM aqi')
date_from = curs.fetchone()[0] - datetime.timedelta(days=1)
date_from = date_from.date().isoformat() 

#define date ranges for getting aqi data: date_to is todays date.
date_to = datetime.date.today().isoformat()

# initalize counters for summary
locations_success = set()
total_aqi_inserts = 0
table_exceptions = { 'countries': 0, 'pollutants': 0, 'locations': 0, 'sensors': 0,   'aqi': 0  }             

# Establish parent process (specifically if run in background by launchd or not). If not, progress bar from tqdm library will be used.
from_launchd = os.getenv('RUNNING_FROM_LAUNCHD')
	
#main ETL script
def main():
	#log program start info
	logger.info('%s: ETL main started.', datetime.datetime.now().ctime())
	logger.info(f'Fetching AQI data from {date_from} to {date_to}.')

	#progress bar wrapper for iterating over ETL process
	# if from_launchd:
	# with tqdm(total=len(location_ids), desc='Processing...', ncols=100, leave=False) as pbar: 
	for loc_id in location_ids:
			# send location endpoint request and return json object of response
			loc_response = get_location_response(loc_id, to_print=False)
			
			if loc_response is None: # or loc_response.results[0]:
				# pbar.update(1)
				# sys.stdout.flush()
				continue

			sensor_ids, dfs = location_res_to_dfs(loc_response)

			#unpack dataframes from dfs
			locations_df, countries_df, sensors_df, pollutants_df = dfs
			#get dataframe of all sensor aqi data at location at loc_id
			aqi_df = multi_aqi_request_to_df(sensor_ids, loc_id, date_from, date_to)

			if aqi_df.empty:	
				# pbar.update(1)
				# sys.stdout.flush()
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
			# tqdm.write(f'{lines_commited} lines inserted for location {loc_id}')
			# pbar.update(1)
			# sys.stdout.flush()
	return

#helper function for inserting a df to associated table in aqi database 
def insert_df_to_db(curs, tablename, df):
	global total_aqi_inserts  # Add this line to modify the global variable

	# extract column headers from dataframe
	head = df.columns.to_list()

	#make string of '%s' pollutants for each value that will be inserted in each row. One per column in a dataframe. Use # of pollutants in the header list.
	placeholder = ', '.join(['%s']*len(head))

	#convert header list to a tuple, all in a string. also change single quotes to backticks.
	head = str(tuple(head)).replace("'","`")

	#Execute query: 1) insert table name, column headers string, and %s placeholder string (for prepared statement format)
	#Update id = id "resets" the id to itself if a key constraint is triggered, id is not changed, row is not altered, insert continues.
	query = "INSERT INTO `{}` {} VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(tablename, head, placeholder,
				', '.join(f"`{col}` = VALUES(`{col}`)" for col in df.columns[1:])) 

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

def sensor_in_db(curs, sensor_id):	# func for checking if sensor already in db table sensors - used for preventing redundant inserts
	query = 'SELECT id FROM sensors WHERE id = %s'
	sensor_id = [int(sensor_id)]
	curs.execute(query, sensor_id)
	rows = curs.fetchall()
	# if rows:
	# 	tqdm.write(f'{sensor_id} found in DB already.')
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

