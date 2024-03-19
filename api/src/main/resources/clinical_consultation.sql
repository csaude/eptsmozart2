CREATE TABLE `clinical_consultation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `consultation_date` date NOT NULL,
  `scheduled_date` date DEFAULT NULL,
  `bp_diastolic` double DEFAULT NULL,
  `bp_systolic` double DEFAULT NULL,
  `who_staging` int(11) DEFAULT NULL,
  `weight` double DEFAULT NULL,
  `height` double DEFAULT NULL,
  `arm_circumference` double DEFAULT NULL,
  `nutritional_grade` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `clinical_consultation_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8