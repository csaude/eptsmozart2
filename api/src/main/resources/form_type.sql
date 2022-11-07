CREATE TABLE `form_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `form_type_id` int(11) DEFAULT NULL,
  `form_type_name` varchar(255) DEFAULT NULL,
  `form_type_uuid` char(38) DEFAULT NULL,
  `encounter_type_id` int(11) DEFAULT NULL,
  `encounter_type_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8