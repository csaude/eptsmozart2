CREATE TABLE `sti` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `sti_concept_id` int(11) NOT NULL,
  `sti_date` datetime DEFAULT NULL,
  `sti_value` int(11) DEFAULT NULL,
  `sti_uuid` char(38) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `sti_uuid` (`sti_uuid`),
  KEY `sti_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8