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
import csv, time, os
from dotenv import load_dotenv

# init client
api = OpenAQ()
        
def main():
        #send request to API for countries info, process json data, then convert to dataframe (request func auto rate-limits)
        #countries_json = send_get_request(limit=300, endpoint='countries')
        countries_resp = api.countries.list(limit=200)
        # countries_df = format_countries_json(countries_json)
        countries_df = format_countries_resp(countries_resp)
        
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
        location_ids = get_available_locations(cdc)

        #create a boolean mask by mapping a boolean response func on the country name column. If name is in avaiable list, it will be true. 
        # mask = cdc.name.map(lambda x: x in available_countries)
        # cdc = cdc.loc[mask] 

        #save location ids as csv file in one row. 
        filename='locations list1.csv'
        save_list(location_ids, filename)

"""Optional parameters to include can be found at OpenAQ endpoint documentation.
for locations: coordinates, countries_id...
"""
def send_get_request(limit, endpoint=None, box=None): #coordinates, country_id - optional argument with 4 decimal precision, WGS 84 format

        #send get request
        # try to get response = requests.get(URL, headers=headers, params=params)
        try:
                if endpoint == 'countries':
                        response = api.countries.list(limit=limit)
                elif endpoint == 'locations':
                        pollutants=[1,2,3,5]        # make sure searches for presence of key pollutants
                        response = api.locations.list(limit=limit, bbox=box)    # , parameters_id=pollutants)
                
                print(f'{response.headers.x_ratelimit_used} call(s) placed')
                #catch/limit rate limiting
                if response.headers.x_ratelimit_remaining == 0:
                        print(f'\nRate limit reached. Sleeping {response.headers.x_ratelimit_reset} seconds...')
                        rest = response.headers.x_ratelimit_reset
                        time.sleep(rest)

                return json.loads(response.json())

        # catch ratelimit errors indefinitely, and recursively call get request after taking a nice nap
        except RateLimitError:
                time.sleep(nap=30)
                send_get_request(limit, endpoint, box)

        #catch error if there is an error that isn't 429, or after 5 retries, still fails, also return None.
        if response.status_code != 200:
                print(f'Error: {response.status_code}, {response.text}')
                return None
        
#format data into desired dataframe. source data is json object. 
def format_countries_resp(response):

        #initialize empty dict - for inserting parsed country data 
        data_dict = {}

        #only use results section of json data to save keystrokes
        results = response.results

        #use list comprehension to extract id, name, and country code for each result in the results section.
        data_dict['id'] = [res.id for res in results]
        data_dict['name'] = [res.name for res in results]
        data_dict['code'] = [res.code for res in results]

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
#                coords = ', '.join(str(x) for x in coords)
                return coords, capital 

        except KeyError:
                #if CountryInfo doesn't have info for a country, return None. This will be handled in main
                return None

def bbox_gen(coord):
        # separate coords to lat and long
        lat, long = coord
        #min long, min lat, max long, max lat
        dif = 0.2
        return [round(long-dif,4), round(lat-dif,4), round(long+dif, 4), round(lat+dif, 4)]

# check rate limit and sleep if required
def check_rate_limit(response, to_print=True):
        if to_print:
                print(f'{response.headers.x_ratelimit_used} call(s) placed')
        #catch/limit rate limiting
        if response.headers.x_ratelimit_remaining == 0:
                if to_print:
                        print(f'\nRate limit reached. Sleeping {response.headers.x_ratelimit_reset} seconds...')
                rest = response.headers.x_ratelimit_reset
                time.sleep(rest)

def get_available_locations(dataframe):
        cdc = dataframe.copy()

        #initialize lists for tracking countries, location_ids, and countries that the API doensn't return locations for.
        available_countries = []
        location_ids = []
        notfound_countries = []

        #loop through index in cdc table. append countreies and ids to lists. 
        for i in cdc.index:
                country = cdc.at[i, 'name']
                coord = cdc.at[i, 'coordinates']

                box = bbox_gen(coord)
                box_str = ','.join(map(str,box))
                #print(f'{box}\t{box_str}')
                try:
                        #locations_json = send_get_request(limit=3, endpoint='locations', box=box_str)
                        locations_resp = api.locations.list(limit=10, bbox=box_str)
                        check_rate_limit(locations_resp, to_print=True)

                        if locations_resp.meta.found != 0:
                                available_countries.append(country)
                                [location_ids.append(res.id) for res in locations_resp.results]
                                # location_ids.append(locations_json['results'][0]['id'])
                        else:
                                notfound_countries.append(country)
                except Exception:
                        notfound_countries.append(country)
                        continue

        #report how many countries found and not found        
        print('\nRESULTS\n', '='*50)
        print(f'{len(available_countries)} countries successful')
        print(f'{len(location_ids)} locations saved')
        print(f'{len(notfound_countries)} locations not found')
        print(f'Failed:\n{notfound_countries}') 

        
        return location_ids
 
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