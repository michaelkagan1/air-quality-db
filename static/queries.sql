--This sql file shows examples of common queries that a user might run on this database to gain insights into the air quality data.
-- It also has commonly run queries that the admin runs to maintain it. 


-- select all sensors (elements) that are the location in the US
SELECT name, displayName, units 
FROM elements
WHERE id IN (
	SELECT element_id 
	FROM sensors
	WHERE location_id IN (
		SELECT id 
		FROM locations
		WHERE country_id = (
			SELECT id
			FROM countries
			WHERE name = 'United States'
		)
	)
);


-- select all aqi values from the past day in the United States, including the element name
SELECT `datetime`, `elements`.`name` AS 'element', `value`
FROM aqi JOIN elements ON aqi.element_id = elements.id 
	 JOIN locations ON aqi.location_id = locations.id
	 JOIN countries ON locations.country_id = countries.id
WHERE countries.name = 'United States'
AND `datetime` = (
	SELECT MAX(datetime)
	FROM aqi
);


-- get average values by locality in the last week, as well as number of data points
SELECT `datetime`, `locality`, `elements`.`name` AS 'element', ROUND(AVG(`value`), 3) AS 'average_value', COUNT(*) AS 'data_points'
FROM aqi JOIN elements ON aqi.element_id = elements.id 
	 JOIN locations ON aqi.location_id = locations.id
	 JOIN countries ON locations.country_id = countries.id
WHERE countries.name = 'United States'
AND `datetime` BETWEEN 
		(SELECT DATE_SUB(
			(SELECT MAX(`datetime`) FROM `aqi`), 
			INTERVAL 1 MONTH))
	AND 
		(SELECT MAX(`datetime`) FROM `aqi`)
GROUP BY locality, element;


--Select countries and number of sensors (measurments) in each
SELECT name, COUNT(value) AS 'number measurements'
FROM countries 
JOIN locations ON countries.id = locations.country_id
JOIN aqi ON locations.id = aqi.location_id
GROUP BY country_id
ORDER BY name;


--Select all pm2.5 measurements 
SELECT name, datetime, value, min_val, max_val, sd
FROM countries 
JOIN locations ON countries.id = locations.country_id
JOIN aqi ON locations.id = aqi.location_id
WHERE element_id = 2
AND value > 0
ORDER BY name, datetime
LIMIT 50;


--Select average pm2.5 measurement in each country
SELECT name, ROUND(AVG(value),2) AS 'avg_pm2.5', COUNT(value) AS 'number measurements'
FROM countries 
JOIN locations ON countries.id = locations.country_id
JOIN aqi ON locations.id = aqi.location_id
WHERE element_id = 2
GROUP BY country_id
HAVING `avg_pm2.5` > 0
ORDER BY `avg_pm2.5` DESC;


-- Commonly run queries to maintain/ populate DB

--Retreive latest datetime that new data was entered.
SELECT MAX(`datetime`) FROM `aqi`;


--Multi-row insertion from dataframe, with ignore parameter if unique/ key duplicates are entered.
INSERT IGNORE INTO `aqi` (`datetime`, `location_id`, `element_id`, `value`, `min_val`, `max_val`, `sd`)
VALUES ('2024-12-01 18:00:00', 2537, 5, 0.234, '0.1', 1.39, 0.211),
	('2024-12-01 18:00:00', 2537, 5, 0.234, '0.1', 1.39, 0.211),
	('2024-12-01 18:00:00', 2537, 5, 0.234, '0.1', 1.39, 0.211);


--Multi-row insert from dataframe with update parameter for updating displayName column in elements table
	-- because displayName col added after table existed, so NULL values had to be updated.
	-- prepared query is formatted in python script
INSERT INTO `{}` {} VALUES ({}) ON DUPLICATE KEY UPDATE displayName = VALUES(displayName);

