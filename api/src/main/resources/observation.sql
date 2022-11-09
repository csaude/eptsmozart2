CREATE TABLE `observation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `concept_id` int(11) NOT NULL,
  `observation_date` datetime DEFAULT NULL,
  `value_numeric` double DEFAULT NULL,
  `value_concept_id` int(11) DEFAULT NULL,
  `value_text` varchar(10) DEFAULT NULL,
  `value_datetime` datetime DEFAULT NULL,
  `obs_uuid` char(38) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `observation_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8