

#Import files with helper and connection functions

#main
def main():
	#check if I need to do historical import or latest import 

	#Call import for location
	loc_id = '2124'	#change to a saved list of locations
	json_loc = get_location(loc_id)
	sensor_ids, loc = transform_loc(json_loc)

		#Call import for AQI
		for sensor in sensor_ids:


	
	


def stream_latest_data(loc_id):
        #call get func to make api call for data
        res = get_latest_loc_aqi(loc_id)

        #convert response to string then laod into json object
        json_res = json.loads(res.text)

        #call format func to extract individual sensor data as dict, & coordinate data as dict
        #This coord data will be sent to database and inserted if unique
        aqi, coordinates = format_latest_loc_aqi(json_res)

	aqi_cols = ['datetime', 'sensor_id', 'location_id', 'value']	
	sensors_cols = ['sensor_id', 'element_id', 'location_id']
	locations_cols = ['location_id', 'latitude', 'longitude', 'country', 'city']
	elements_cols = ['element_id', 'name', 'units']

	sensor_df = aqi[['location_id']]	
	
	#check if location_id in table

	#if not in table:
		#call get_loc
		#process output into 
	
