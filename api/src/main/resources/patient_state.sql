CREATE TABLE `patient_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_uuid` char(38) DEFAULT NULL,
  `program_id` int(11) DEFAULT NULL,
  `program_enrollment_date` datetime DEFAULT NULL,
  `program_completed_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `enrollment_uuid` char(38) DEFAULT NULL,
  `encounter_uuid` char(38) DEFAULT NULL,
  `source_id` int(11) DEFAULT NULL,
  `state_id` int(11) DEFAULT NULL,
  `state_date` date DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `state_uuid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `patient_state_uuid` (`state_uuid`),
  KEY `patient_state_patient_uuid` (`patient_uuid`),
  KEY `program_date_enrolled` (`program_enrollment_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8