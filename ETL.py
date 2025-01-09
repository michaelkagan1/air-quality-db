"""

# TODO: Use builtin datetime methods (isoformat())
"""
from extract_data import *
from pathlib import Path
import logging
import pdb

#declare path 
# TODO: use os.path.join() and os.getcwd() to construct correct path for logs dynamically,
# in case you move the project
#PATH = '/Users/michaelkagan/Documents/Programming/SQL/AQI_Project/'

#setup logger and config log level and output format
#options for logger are: logger.debug(), info(), warning(), error()
logger = logging.getLogger(__name__)
logging.basicConfig(
                filename='etl.log',
                level=logging.INFO,
                format='%(asctime)s || %(levelname)s: %(message)s',
                force=True
                )   

#using pathlib library, set current dir as PATH
PATH = Path('.')

#main ETL script
# TODO: Break up into easily understandable subfunctions
def main(test):

	#log program start
	logger.info('%s: ETL main started.', datetime.now().ctime())

	#Establish connection and cursor with database as IAM user
	cnx, curs = connect_db()

	#if test arg set to True,pull current table data to check before and after export
	if test:
		print('\n\nData before insertions:')
		curs.execute('SELECT * FROM aqi ORDER BY id DESC LIMIT 10')
		rows = curs.fetchall()
		for row in rows:
			print(row)

	print('\n')

	#Call import for location ids. For testing use short list, later full list
	filename = 'locations list.csv'
	location_ids = location_ids_from_file(filename)

	logger.info('%s: Starting API requests.', datetime.now().ctime())
	print('Starting API requests...')
	print('===============================================================\n')
	
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
		"""
		call location_json_to_df to pass on the json object and parse data into 3 separate dataframes:
			- sensor_ids is a list of ids (int), loc is a dict of
			- sensors = dict with (sensor id, element id, element name)
			- loc = location dict with (id, locality, country, country id, latitude, longitude
		"""
		sensor_ids, sensors, loc = location_json_to_df(json_loc)

		#convert sensors and loc dict objects into dataframes
		sensors_df_temp = DataFrame(sensors)	#no index given because multiple sensors (might have to adjust in edge case)	
		loc_df_temp = DataFrame(loc, index=[0])	#uses index of 0 becuase only 1 row of data in df
			#Call import for AQI and output into dataframe

		aqi_df_temp = multi_aqi_request_to_df(sensor_ids, loc['location_id'], date_from, date_to)
		if aqi_df_temp is None or aqi_df_temp.shape[0] == 0:	#condition for catching empty or None aqi dataframes
			continue
				#response_df is a dataframe with columns [datetime, loc_id, element_id, element name, 
				#value, units, min, max, sd]

		""" Desired dataframe columns: """
		#define column names for 5 SQL tables that match with dataframes
		aqi_cols = ['datetime', 'location_id', 'element_id', 'value', 'units', 'min_val', 'max_val', 'sd']	
		locations_cols = ['location_id', 'latitude', 'longitude', 'country_id', 'locality']
		countries_cols = ['country_id', 'country_name']
		elements_cols = ['element_id', 'element_name', 'units']
		sensors_cols = ['sensor_id', 'element_id', 'location_id']
		
		#Define dataframes by pulling data from temporary dataframes
		aqi_df = aqi_df_temp[aqi_cols].copy()
		aqi_df['datetime'] = aqi_df['datetime'].apply(str)	#convert datetime column to string
		sensors_df = sensors_df_temp[sensors_cols[:2]]
		sensors_df[['location_id']] = loc_id	#plugs in loc_id variable into location_id column in sensors_df
		locations_df = loc_df_temp[locations_cols] 
		countries_df = loc_df_temp[countries_cols]
		elements_df = aqi_df_temp[elements_cols]

		#rename columns so ids in each df are just "id" and name is just "name" - so that columns can remain 
		#descriptive in building dataframes but not redundant in sql.
		locations_df = locations_df.rename(columns = {'location_id': 'id'})
		elements_df = elements_df.rename(columns = {'element_id': 'id', 'element_name': 'name'})
		sensors_df = sensors_df.rename(columns = {'sensor_id': 'id'})
		countries_df = countries_df.rename(columns = {'country_id': 'id', 'country_name': 'name'})

		#rename column id headers to just be id for compatability with SQL
		for header_list in [locations_cols, countries_cols, elements_cols, sensors_cols]:
			header_list[0] = 'id'

		#Prepare tables, column headers and data frames for inserting into sql
		tables = ['aqi', 'locations', 'countries', 'elements', 'sensors']	#table names in SQL 
		headers = [aqi_cols, locations_cols, countries_cols, elements_cols, sensors_cols] #column headers
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
				print(f'Error inserting into {table}: {e}\n')
				#pdb.set_trace()
				continue

		#commit changes to sql. (like save)
		cnx.commit()
	
	#Again, if test selected, print out "after" table data
	if test:
		print('\nData after insertions:')
		curs.execute('SELECT * FROM aqi ORDER BY id DESC LIMIT 10')
		rows = curs.fetchall()
		for row in rows:
			print(row)

	#Change datefrom text file to todays date, which is also the date_to variable
	with open(PATH+'datefrom.txt', 'w') as f:
		#This overwrites the existing date to the new written text
		f.write(date_to)

#retrieves ids of all target locations from 'locations list.csv' file
def location_ids_from_file(filename):
        #open file in read mode, read line to list object and return list
        with open(filename, 'r', newline='') as f:
                reader = csv.reader(f)
                data = list(reader)[0]
        return data


if __name__ == '__main__':
	main(test=True)

