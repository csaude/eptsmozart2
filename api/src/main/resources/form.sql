CREATE TABLE `form` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_id` int(11) DEFAULT NULL,
  `encounter_uuid` char(38) DEFAULT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `created_date` date DEFAULT NULL,
  `encounter_date` datetime DEFAULT NULL,
  `change_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `form_encounter_uuid` (`encounter_uuid`),
  KEY `form_patient_uuid` (`patient_uuid`),
  KEY `form_encounter_date` (`encounter_date`),
  KEY `form_source_encounter_id` (`encounter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8