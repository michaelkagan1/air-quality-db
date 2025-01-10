--This sql file shows 3 examples of common queries that a user might run on this database to gain insights into the air quality data.


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

