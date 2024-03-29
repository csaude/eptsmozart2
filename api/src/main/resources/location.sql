CREATE TABLE `location` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `location_id` int(11) DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `sisma_id` varchar(50) DEFAULT NULL,
  `datim_id` varchar(50) DEFAULT NULL,
  `sisma_hdd_id` varchar(50) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `province_name` varchar(255) DEFAULT NULL,
  `province_district` varchar(255) DEFAULT NULL,
  `selected` boolean DEFAULT FALSE,
  PRIMARY KEY (`id`),
  UNIQUE KEY `location_unique_name_uuid_mapping` (`name`, `location_uuid`),
  KEY `location_source_location_id` (`location_id`),
  KEY `location_uuid` (`location_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8