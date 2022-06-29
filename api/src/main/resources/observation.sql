CREATE TABLE `observation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_id` int(11) DEFAULT NULL,
  `encounter_uuid` char(38) DEFAULT NULL,
  `encounter_date` varchar(255) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_id` varchar(255) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `concept_id` int(11) NOT NULL,
  `concept_name` varchar(255) DEFAULT NULL,
  `observation_date` datetime DEFAULT NULL,
  `value_numeric` double DEFAULT NULL,
  `value_coded` int(11) DEFAULT NULL,
  `value_coded_name` varchar(255) DEFAULT NULL,
  `value_text` varchar(10) DEFAULT NULL,
  `value_datetime` datetime DEFAULT NULL,
  `date_created` datetime,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `observation_encounter_id` (`encounter_id`),
  KEY `observation_encounter_uuid` (`encounter_uuid`),
  KEY `observation_patient_id` (`patient_id`),
  KEY `observation_patient_uuid` (`patient_uuid`),
  KEY `observation_encounter_date` (`encounter_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8