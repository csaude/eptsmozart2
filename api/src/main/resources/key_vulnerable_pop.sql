CREATE TABLE `key_vulnerable_pop` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `encounter_date` date DEFAULT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `encounter_change_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `pop_type` int(11) NOT NULL,
  `pop_id` int(11) NOT NULL,
  `pop_other` VARCHAR(255) DEFAULT NULL,
  `key_vulnerable_pop_uuid` varchar(38) DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `key_vulnerable_pop_uniqueness_key` (`key_vulnerable_pop_uuid`),
  KEY `key_vulnerable_pop_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8