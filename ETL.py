"""

# TODO: Use builtin datetime methods (isoformat())
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

#main ETL script
# TODO: Break up into easily understandable subfunctions
def main():

	#log program start
	logger.info('%s: ETL main started.', datetime.now().ctime())

	#Establish connection and cursor with database as IAM user
	cnx, curs = connect_db()

	#Call import for location ids. For testing use short list, later full list
	filename = 'small locations list.csv'
	location_ids = location_ids_from_file(filename)

	logger.info('%s: Starting API requests.', datetime.now().ctime())
	
	#define date ranges for getting aqi data
	#date_to is todays date.
	date_to = date.today().isoformat()

	#date_from is the most recent (or max) date from the datetime column. Returns as datetime object
	curs.execute('SELECT MAX(datetime) FROM aqi')
	date_from = curs.fetchone()[0]
	date_from = date_from.date().isoformat()

	print(f'Fetching AQI data from {date_from} to {date_to}...\n')
	logger.info(f'Fetching AQI data from {date_from} to {date_to}.')

	for loc_id in location_ids:
		#call get_loc to send location endpoint request and return json object of response
		json_loc = get_location_response(loc_id)
		
		if json_loc is None:
			continue

		sensor_ids, dfs = location_json_to_dfs(json_loc)

		#unpack dataframes from dfs
		locations_df, countries_df, sensors_df, elements_df = dfs

		aqi_df = multi_aqi_request_to_df(sensor_ids, loc_id, date_from, date_to)

		if aqi_df.empty:	
			continue

		#Prepare tables, column headers and data frames for inserting into sql
		tables = ['aqi', 'locations', 'countries', 'elements', 'sensors']	#table names in SQL 
		dataframes = [aqi_df, locations_df, countries_df, elements_df, sensors_df]	#dataframes in the same order


		for table, df in zip(tables, dataframes):	#zip so each table and source dataframe can be associated with eachother. 

			# extract column headers from dataframe
			head = df.columns.to_list()

			#make string of '%s' elements for each value that will be inserted in each row. One per column in a dataframe. Use # of elements in the header list.
			placeholder = ', '.join(['%s']*len(head))

			#convert header list to a tuple, all in a string. also change single quotes to backticks.
			head = str(tuple(head)).replace("'","`")

			#Execute query: 1) insert table name, column headers string, and %s placeholder string (for prepared statement format)
			#IGNORE used to ignore duplicate entries
			query = "INSERT IGNORE INTO `{}` {} VALUES ({})".format(table, head, placeholder) 

			#row by row, change list into tuple, to plug into 'insert many' method. List of tuples is argument for insert_many. Each element in list is a separate set of values to be inserted. 
			values = [(tuple(row)) for row in df.values] 

			#Try inserting into each table, print error on fail and keep looping
			try:
				curs.executemany(query, values)
			except Exception as e:
				logger.warning('Table insert unsuccessfull: %s', e)
				continue

		#commit changes to sql. (like save)
		cnx.commit()
	
#retrieves ids of all target locations from 'locations list.csv' file
def location_ids_from_file(filename):
        #open file in read mode, read line to list object and return list
        with open(filename, 'r', newline='') as f:
                reader = csv.reader(f)
                data = list(reader)[0]
        return data


if __name__ == '__main__':
	main(test=True)

