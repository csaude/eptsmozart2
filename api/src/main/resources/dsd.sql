CREATE TABLE `dsd` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `dsd_id` int(11) NOT NULL,
  `dsd_state_id` int(11) NOT NULL,
  `date_created` datetime,
  PRIMARY KEY (`id`),
  KEY `dsd_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8