CREATE TABLE IF NOT EXISTS aqi (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`datetime` DATETIME NOT NULL,
	`location_id` INT UNSIGNED NOT NULL,
	`element_id` INT UNSIGNED NOT NULL,
	`value` FLOAT NOT NULL,
	`min_val` FLOAT NOT NULL,
	`max_val` FLOAT NOT NULL,
	`sd` FLOAT NOT NULL,
	PRIMARY KEY(`id`),
	FOREIGN KEY(`location_id`) REFERENCES `locations`(`id`),
	FOREIGN KEY(`element_id`) REFERENCES `elements`(`id`),
	UNIQUE(`datetime`, `location_id`, `element_id`)
);

CREATE TABLE IF NOT EXISTS `locations` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`latitude` VARCHAR(20) NOT NULL,
	`longitude` VARCHAR(20) NOT NULL,
	`country_id` SMALLINT UNSIGNED NOT NULL,
	`locality` VARCHAR(50),
	FOREIGN KEY(`country_id`) REFERENCES `countries`(`id`),
	PRIMARY KEY(`id`)
);

CREATE TABLE IF NOT EXISTS `countries` (
	`id` SMALLINT UNSIGNED AUTO_INCREMENT,
	`name` VARCHAR(30),
	PRIMARY KEY(`id`)
);

CREATE TABLE IF NOT EXISTS `elements` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`name` VARCHAR(30),
	`units` VARCHAR(10),
	`displayName` VARCHAR(20),
	PRIMARY KEY(`id`)
);

CREATE TABLE IF NOT EXISTS `sensors` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`element_id` INT UNSIGNED NOT NULL,
	`location_id` INT UNSIGNED NOT NULL,
	PRIMARY KEY(`id`),
	FOREIGN KEY(`element_id`) REFERENCES `elements`(`id`),
	FOREIGN KEY(`location_id`) REFERENCES `locations`(`id`)
);

-- To change all mentions of element to pollutant:
	-- 1. DONE remove foreign keys referencing elements 
		-- ALTER TABLE aqi DROP FOREIGN KEY aqi_ibfk_2;
		-- ALTER TABLE sensors DROP FOREIGN KEY sensors_ibfk_1;
	-- 2. DONE change name of elements table to pollutants
		-- ALTER TABLE elements RENAME TO pollutants;
	-- 3. DONE change column names from element_id to pollutant_id
		-- ALTER TABLE aqi RENAME COLUMN element_id TO pollutant_id;
		-- ALTER TABLE sensors RENAME COLUMN element_id TO pollutant_id;
	-- 4. DONE replace foreign keys to refernce pollutant_id
		-- ALTER TABLE aqi ADD FOREIGN KEY(pollutant_id) REFERENCES pollutants(id);
		-- ALTER TABLE sensors ADD FOREIGN KEY(pollutant_id) REFERENCES pollutants(id);
	-- 5. DONE Rename index KEYS in aqi
		-- ALTER TABLE aqi RENAME KEY aqi_element_index TO aqi_pollutant_index;
		
-- Add gdp per cap to countries
	-- Possibly other data to enrich analysis (health)