CREATE TABLE `clinical_consultation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `consultation_date` date DEFAULT NULL,
  `scheduled_date` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `clinical_consultation_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8