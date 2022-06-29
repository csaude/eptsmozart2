CREATE TABLE `patient_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_id` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `source_id` int(11) DEFAULT NULL,
  `source_type` varchar(255) DEFAULT NULL,
  `state_id` int(11) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `state_date` date DEFAULT NULL,
  `state_uuid` varchar(255) DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `patients_uuid` (`patient_uuid`),
  KEY `patient_source_patient_id` (`patient_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8