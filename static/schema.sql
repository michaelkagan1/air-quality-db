-- Add comments and update in git

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
