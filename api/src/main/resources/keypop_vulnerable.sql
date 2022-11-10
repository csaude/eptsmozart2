CREATE TABLE `keypop_vulnerable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `pop_type` int(11) NOT NULL,
  `pop_id` int(11) NOT NULL,
  `pop_other` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8