CREATE TABLE `prophylaxis` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `encounter_date` datetime NOT NULL,
  `regimen_prophylaxis_tpt` int(11) DEFAULT NULL,
  `regimen_prophylaxis_ctx` int(11) DEFAULT NULL,
  `regimen_prophylaxis_prep` int(11) DEFAULT NULL,
  `no_of_units` double DEFAULT NULL,
  `prophylaxis_status` int(11) DEFAULT NULL,
  `secondary_effects_tpt` int(11) DEFAULT NULL,
  `secondary_effects_ctz` int(11) DEFAULT NULL,
  `dispensation_type` int(11) DEFAULT NULL,
  `next_pickup_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `prophylaxis_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
