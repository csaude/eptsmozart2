CREATE TABLE `dsd` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `encounter_date` varchar(255) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `source_id` int(11) NOT NULL,
  `dsd_id` int(11) NOT NULL,
  `dsd_state_id` int(11) NOT NULL,
  `date_created` datetime,
  PRIMARY KEY (`id`),
  KEY `dsd_encounter_uuid` (`encounter_uuid`),
  KEY `dsd_patient_uuid` (`patient_uuid`),
  KEY `dsd_encounter_date` (`encounter_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8