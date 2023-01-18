CREATE TABLE `keypop_vulnerable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `pop_type` int(11) NOT NULL,
  `pop_id` int(11) NOT NULL,
  `pop_other` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `keypop_vulnerable_uniqueness_key` (`encounter_uuid`, `pop_type`, `pop_id`),
  KEY `keypop_vulnerable_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8