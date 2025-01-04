"""
The purpose of this script is to create a list of location ids to be used for data streaming from the Open AQ API. 
This is done by:
        1. obtaining all countries that are served by the OpenAQ API with the countries endpoint
        2. parsing response to a dataframe with country name, id, and code
        3. Getting the capital name and coordinates for each country in the list (because I want to collect aqi data from every country's capital
        4. Sending a get request to the coordinates of the capitals to see which of those cities have aqi systems. Available locations are narrowed down in a list called available_countries, with their location_ids representing the associated ids. 
        5. Finally, a list of location ids is saved in a csv file as one row of data.
        6. This will be used by the ETL script as a template for which location ids to query. 
"""
from extract_data import *
from countryinfo import CountryInfo
import csv, time
import pdb

def main():
        #establish connection with mysql db
        cnx, curs = connect_db()

        #send request to API for countries info, process json data, then convert to dataframe
        URL = '/v3/countries'   #endpoint for getting list of all countries
        countries_json = send_get_request(URL, limit=200)
        countries_df = format_countries_json(countries_json)
        
        #make copy for modifying rows
        cdc = countries_df.copy()

        #initiate lists for tracking capitals, their coordinates, and skipped countries
        capitals = []
        coordinates = []
        skipped = []

        #loop over countries in the name column of cdc and add all countries to either skipped or capitals/coordinates lists
        for country in cdc['name']:
                try:
                        #use helper fn to extract capital name and its coords for each country.
                        coords, capital = get_capital_coord(country)

                        #Append each to list
                        capitals.append(capital)
                        coordinates.append(coords)

                #If countryinfo library doesn't have info for a country or any other exception, remove it from dataframe
                except Exception:       
                        #if response does not contain 2 items, drop given country from dataframe
                        cdc.drop(index = cdc[cdc['name'] == country].index, inplace=True)

                        #add country to skipped list for tracking
                        skipped.append(country)
        
        #add both lists to new columns in the dataframe
        cdc['capital'] = capitals
        cdc['coordinates'] = coordinates
        
        #from countries dataframe, get list of available countries (whose capital cities have a location in OpenAQ API) and location ids.
        available_countries, location_ids = get_available_locations(cdc, return_locs=True)

        #create a boolean mask by mapping a boolean response func on the country name column. If name is in avaiable list, it will be true. 
        mask = cdc.name.map(lambda x: x in available_countries)
        cdc = cdc.loc[mask] 

        #save location ids as csv file in one row. 
        filename='locations list.csv'
        save_list(location_ids, filename)

        return cdc
                
        
                

"""Optional parameters to include can be found at OpenAQ endpoint documentation.
for locations: coordinates, countries_id...
"""
def send_get_request(URL, limit, **parameters): #coordinates, country_id - optional argument with 4 decimal precision, WGS 84 format
        #assemble url for request
        URL = API_URL + URL

        #declare params with limit condition only
        params = {'limit': limit }

        #convert function args to dict object, add these to params dict. If it's empty, it won't change params
        parameter_args = dict(parameters)
        params.update(dict(parameter_args))

        #set up headers dict for request
        headers = {
                'accept': 'application/json',
                'X-API-KEY': KEY
                }

        delay = 0.5       #delay time for get requests that exceed request frequency limit
        #maximum of 5 times, retry request if there is a 'too many requests' error (429), otherwise, break
        for i in range(5):
                #send get request
                response = requests.get(URL, headers=headers, params=params)

                if response.status_code == 200: 
                        #if request does succeed, parse data as json object and return
                        data = response.text
                        json_data = json.loads(data)
                        return json_data

                #status code 429 represents 'too many requests', so get requests need to "back off" or slow down. sleep for `delay` time and double the delay time for the next try in case there's another 429 code.
                elif response.status_code == 429: 
                        time.sleep(delay)
                        delay *= 2
                #if the status code is not 200 and also not 429, that means its some other issue - in that case, break loop and for loop
                else:
                        break

        #catch error if there is an error that isn't 429, or after 5 retries, still fails, also return None.
        if response.status_code != 200:
                print(f'Error: {response.status_code}, {response.text}')
                return None
        
#format data into desired dataframe. source data is json object. 
def format_countries_json(response_json):

        #initialize empty dict - for inserting parsed country data 
        data_dict = {}

        #only use results section of json data to save keystrokes
        results = response_json['results']

        #use list comprehension to extract id, name, and country code for each result in the results section.
        data_dict['id'] = [res['id'] for res in results]
        data_dict['name'] = [res['name'] for res in results]
        data_dict['code'] = [res['code'] for res in results]

        #compile dictionary and return as a dataframe
        return DataFrame(data_dict)

def get_capital_coord(country):
        #call CountryInfo with given country name to extract geographic data
        country = CountryInfo(country)

        #use exception handling in case CountryInfo library did not recognize input name
        try:
                #pull out capital and capital_coordinates from json data of country. finally return parsed capital and coords
                capital = country.capital()
                coords = country.capital_latlng()
                coords = ', '.join(str(x) for x in coords)
                return coords, capital 

        except KeyError:
                #if CountryInfo doesn't have info for a country, pass. This will be omitted in dataset
                pass

def get_available_locations(dataframe, return_locs=False):
        cdc = dataframe.copy()

        #Define endpoint in OpenAQ API - 
        URL = '/v3/locations'   #endpoint for getting list of all locations

        #initialize lists for tracking countries, location_ids, and countries that the API doensn't return locations for.
        available_countries = []
        location_ids = []
        notfound_countries = []

        #loop through index in cdc table. append countreies and ids to lists. 
        for i in cdc.index:
                country = cdc.at[i, 'name']
                coord = cdc.at[i, 'coordinates']
                try:
                        locations_json = send_get_request(URL, limit=3, coordinates=coord, radius=20000)
                        if len(locations_json['results']) > 0:
                                available_countries.append(country)
                                location_ids.append(locations_json['results'][0]['id'])
                        else:
                                notfound_countries.append(country)
                except Exception:
                        #pdb.set_trace()
                        notfound_countries.append(country)
                        continue

        #report how many countries found and not found        
        print(f'{len(available_countries)} locations successful')
        print(f'{len(notfound_countries)} locations not found')
        print(f'Failed:\n{notfound_countries}') 
        
        if return_locs:
                return available_countries, location_ids
        return available_countries
 
#helper function to save list object to filename
def save_list(data_list, filename):
        with open(filename, 'w') as f:
                write = csv.writer(f)
                write.writerow(data_list)
        return

if __name__ == '__main__':
        #calls main script to output dataframe cdc (countries with ids, capital, and coordinates of capital)
        cdc = main()
        print(cdc)
        
