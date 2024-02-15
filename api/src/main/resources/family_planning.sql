CREATE TABLE `family_planning` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `fp_concept_id` int(11) NOT NULL,
  `fp_date` datetime DEFAULT NULL,
  `fp_method` int(11) DEFAULT NULL,
  `fp_uuid` char(38) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `family_planning_uuid` (`fp_uuid`),
  KEY `family_planning_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8