from extract_data import *
import pdb

#Import files with helper and connection functions

#main
def main(test):
	#check if I need to do historical import or latest import 
	##skip for now
	
	#Establish connection and cursor with database as IAM user
	cnx, curs = connect_db()

	#Select database `aqi`
	curs.execute('USE aqi')

	#clear cursor result for future queries
	curs.fetchall()

	#if test set to True,pull current table data to check before and after export
	if test:
		print('\n\nData before insertions:')
		curs.execute('SELECT * FROM aqi')
		rows = curs.fetchall()
		for row in rows:
			print(row)

	print('\n\n')
	input('Checkpoint reached. Enter to continue...')

	#Call import for location <<USE MANUAL ENTRY LOC_ID FOR NOW>>
	location_ids = pull_location_ids()

	json_loc = get_loc(loc_id)
	sensor_ids, sensors, loc = transform_loc(json_loc)#sensor_ids is a list of ids (int), loc is a dict of
					#sensors = dict with (sensor id, element id, element name)
					#location = dict with (id, locality, country, country id, latitude, longitude)

	sensors_df_temp = DataFrame(sensors)	#no index given because multiple sensors (might have to adjust in edge case)	
	loc_df_temp = DataFrame(loc, index=[0])	#uses index of 0 becuase only 1 row of data in df
	
	#Call import for AQI
	aqi_df_temp = get_aqi(sensor_ids, loc['location_id'])
			#response_df is a dataframe with columns [datetime, loc_id, element_id, element name, 
			#value, units, min, max, sd]

	""" 
	Desired dataframe columns: 
	"""
	aqi_cols = ['datetime', 'location_id', 'element_id', 'value', 'units', 'min_val', 'max_val', 'sd']	
	locations_cols = ['location_id', 'latitude', 'longitude', 'country_id', 'locality']
	countries_cols = ['country_id', 'country_name']
	elements_cols = ['element_id', 'element_name', 'units']
	sensors_cols = ['sensor_id', 'element_id', 'location_id']
	
	#Define dataframes by pulling data from temporary dataframes
	aqi_df = aqi_df_temp[aqi_cols]
	aqi_df['datetime'] = aqi_df['datetime'].map(str)	#convert datetime column to string
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
	for headers in [locations_cols, countries_cols, elements_cols, sensors_cols]:
		headers[0] = 'id'

	#Prepare tables, column headers and data frames for inserting into sql
	tables = ['aqi', 'locations', 'countries', 'elements', 'sensors']
	headers = [aqi_cols, locations_cols, countries_cols, elements_cols, sensors_cols]
	dataframes = [aqi_df, locations_df, countries_df, elements_df, sensors_df]


	for table, df in zip(tables, dataframes):	#zip so each table, col header string, and source dataframe can be associated with eachother. 
		head = df.columns.to_list()
		placeholder = ', '.join(['%s']*len(head))
		head = str(tuple(head)).replace("'","`")

		query = "INSERT IGNORE INTO `{}` {} VALUES ({})".format(table, head, placeholder) #ignore used to ignore duplicate entries
		values = [(tuple(row)) for row in df.values] 	#row by row, change list into tuple, to plug into insert many method
		#Try inserting into each table, print error on fail and keep looping
		try:
			curs.executemany(query, values)
		except Exception as e:
			print(f'Error inserting into {table}: {e}')

	cnx.commit()

	print('\n\n')
	input('Checkpoint reached. Enter to continue...')


	#Again, if test selected, print out "after" table data
	if test:
		print('\n\nData after insertions:')
		curs.execute('SELECT * FROM aqi')
		rows = curs.fetchall()
		for row in rows:
			print(row)

	
def check_table(table='aqi'):
	cnx, curs = connect_db()
	#curs.execute(f'SELECT * FROM {table}')
	curs.execute('USE `aqi`')
	curs.fetchall()

	curs.execute(f'SELECT * FROM {table}')
	rows = curs.fetchall()
	for row in rows:
		print(row)
	
def pull_location_ids():
	with open('small locations list.csv', 'r', newline='') as f:
		reader = csv.reader(f)
		data = list(reader)[0]
	return data
	
	
	
if __name__ == '__main__':
	main(test=False)

