CREATE TABLE `patient_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_uuid` char(38) DEFAULT NULL,
  `program_id` int(11) DEFAULT NULL,
  `program_enrolment_date` datetime DEFAULT NULL,
  `program_completed_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `enrolment_uuid` char(38) DEFAULT NULL,
  `source_id` int(11) DEFAULT NULL,
  `state_id` int(11) DEFAULT NULL,
  `state_date` date DEFAULT NULL,
  `state_uuid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `patient_state_patient_uuid` (`patient_uuid`),
  KEY `program_date_enrolled` (`program_enrolment_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8