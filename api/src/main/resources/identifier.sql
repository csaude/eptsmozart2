CREATE TABLE `identifier` (
  `identifier_seq` int(11) NOT NULL AUTO_INCREMENT,
  `patient_uuid` char(38) DEFAULT NULL,
  `identifier_type` int(11) DEFAULT NULL,
  `identifier_type_name` varchar(255) DEFAULT NULL,
  `identifier_value` varchar(255) DEFAULT NULL,
  `primary` tinyint(4) DEFAULT NULL,
  `identifier_uuid` varchar(38) DEFAULT NULL,
  PRIMARY KEY (`identifier_seq`),
  UNIQUE KEY `identifier_uuid` (`identifier_uuid`),
  KEY `identifier_patient_uuid` (`patient_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8