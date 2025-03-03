CREATE TABLE `patient` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_id` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `sex` char(2) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  `birthdate_estimated` tinyint(1) DEFAULT NULL,
  `date_created` date DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `patients_uuid` (`patient_uuid`),
  KEY `patients_birthdate` (`birthdate`),
  KEY `patient_source_patient_id` (`patient_id`),
  KEY `patient_sex` (`sex`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8