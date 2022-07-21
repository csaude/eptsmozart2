CREATE TABLE `program` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_id` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `program_id` int(11) DEFAULT NULL,
  `program` varchar(255) DEFAULT NULL,
  `date_enrolled` datetime DEFAULT NULL,
  `date_completed` datetime DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `location_name` varchar(255) DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `enrolment_uuid` char(38) DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `program_patient_id` (`patient_id`),
  KEY `program_patient_uuid` (`patient_uuid`),
  KEY `program_date_enrolled` (`date_enrolled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8