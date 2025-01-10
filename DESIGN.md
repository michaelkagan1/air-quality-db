# air-quality-db
Data pipeline for air quality data from OpenAQ API to MYSQL server in AWS.

#### Video


### Purpose
The purpose of the AQI data pipeline is to automatically collect, parse, and store daily air quality data to monitor environmental conditions in the capital cities of up to ~100 countries. 

### Scope
The scope of the project encompasses the design and implementation of a relational database system to manage air quality data. The data pipeline covers several critical components: storing raw AQI measurements, handling sensor metadata, and organizing geographical information. The database includes tables to capture specific air quality parameters, the locations of sensors, and their respective readings over time. Finally, the pipeline is automated to execute batch data transfers daily at 12pm using launchd job scheduler. 

### Entities
##### AQI (Air Quality Index): 
This table stores the actual air quality measurements. Each record corresponds to a specific date and time, location, and pollutant element. It includes the value of the measurement, its corresponding units, and statistical data such as the minimum, maximum, and standard deviation of the readings.
##### Locations: 
This table holds information about the physical locations where the sensors are deployed. Each location is identified by latitude, longitude, and the country of origin. Optional fields also allow for specifying the locality within the country.
##### Countries: 
The countries table associates each location with a country, enabling easy categorization and retrieval of data by country.
Elements: The elements table defines the types of air quality parameters being measured, such as PM2.5, CO2, or NO2. Each element includes a name and the units of measurement.
##### Sensors: 
This table connects elements and locations, representing the sensors responsible for collecting air quality data. A sensor is linked to both a specific element (pollutant) and a location. 

![Entity-relationship Diagram](https://github.com/user-attachments/assets/f559439f-c9ea-4135-826c-d6c869a1591c)

### Relationships
The AQI table has foreign key relationships with both the locations and elements tables. Each AQI entry is tied to a specific location and element, allowing for easy retrieval of air quality data by location and pollutant.
The sensors table establishes relationships between locations and elements. This allows the system to track which sensors are located at which sites and measure which pollutants.
The countries table is linked to the locations table, providing geographical context to the AQI data. Each location belongs to a specific country.

### Optimizations
##### Normalization: The database schema has been designed to minimize redundancy by separating location, element, and country information into distinct tables. This enhances data integrity and reduces the risk of inconsistent data entries.
##### Indexing: The AQI table has been indexed on the combination of datetime, location_id, and element_id, ensuring quick lookups and efficient querying when analyzing data by time, location, or element.
##### Constraints: The use of foreign key constraints ensures that data integrity is maintained across tables. For example, any AQI record must refer to valid entries in the locations and elements tables, preventing the insertion of invalid data. A "UNIQUE" table constraint on `aqi` places an additional check which prevents duplicate data entry, and is definied by a unique set of three variables: datetime, location, and element. 

### Limitations
##### Data Granularity: The data pipeline is designed to import one reading per day per sensor, which is sufficient for long-term analysis across months or seasons, but may be inadequate for finer analysis, for example for different times of day. In addition, not all daily averages are computed from the same number of measurements. This data is available in the API, however not in the scope of my database. 
##### Sensor Operation: The system assumes consistent air monitoring across all sensors at all locations, however not all locations contain the same set of sensors, and not all locations are equaly operational. Some locations may have missing measurements altogether, so this data would have to be obtained elsewhere if desired.
##### Geospatial Limitations: The latitude and longitude fields in the locations table are stored as text, which could pose challenges for complex geospatial queries or analysis.
