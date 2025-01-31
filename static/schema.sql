CREATE TABLE `aqi` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `datetime` datetime NOT NULL,
  `location_id` int unsigned NOT NULL,
  `pollutant_id` int unsigned NOT NULL,
  `value` float NOT NULL,
  `min_val` float NOT NULL,
  `max_val` float NOT NULL,
  `sd` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `datetime` (`datetime`,`location_id`,`pollutant_id`),
  KEY `aqi_location_index` (`location_id`),
  KEY `aqi_pollutant_index` (`pollutant_id`),
  CONSTRAINT `aqi_ibfk_1` FOREIGN KEY (`location_id`) REFERENCES `locations` (`id`),
  CONSTRAINT `aqi_ibfk_2` FOREIGN KEY (`pollutant_id`) REFERENCES `pollutants` (`id`)
)

CREATE TABLE `locations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `latitude` varchar(20) NOT NULL,
  `longitude` varchar(20) NOT NULL,
  `country_id` smallint unsigned NOT NULL,
  `locality` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `country_id` (`country_id`),
  CONSTRAINT `locations_ibfk_1` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`)
)


CREATE TABLE `countries` (
  `id` smallint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) DEFAULT NULL,
  `gdp_per_capita` float DEFAULT NULL,
  `region` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
)

CREATE TABLE `pollutants` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) DEFAULT NULL,
  `units` varchar(10) DEFAULT NULL,
  `displayName` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
)

CREATE TABLE `sensors` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `pollutant_id` int unsigned NOT NULL,
  `location_id` int unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `location_id` (`location_id`),
  KEY `pollutant_id` (`pollutant_id`),
  CONSTRAINT `sensors_ibfk_2` FOREIGN KEY (`location_id`) REFERENCES `locations` (`id`),
  CONSTRAINT `sensors_ibfk_3` FOREIGN KEY (`pollutant_id`) REFERENCES `pollutants` (`id`)
) 