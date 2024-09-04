CREATE TABLE `dsd_supportgroup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `encounter_date` date DEFAULT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `encounter_created_date` datetime DEFAULT NULL,
  `encounter_change_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `dsd_supportgroup_id` int(11) NOT NULL,
  `dsd_supportgroup_state` int(11) NOT NULL,
  `dsd_supportgroup_uuid` char(38) NOT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dsd_supportgroup_uniqueness_key` (`dsd_supportgroup_uuid`),
  KEY `dsd_supportgroup_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8