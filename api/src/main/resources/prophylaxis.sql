CREATE TABLE `prophylaxis` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `encounter_date` datetime NOT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `encounter_change_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `regimen_prophylaxis_tpt` int(11) DEFAULT NULL,
  `regimen_prophylaxis_ctx` int(11) DEFAULT NULL,
  `prophylaxis_status` int(11) DEFAULT NULL,
  `secondary_effects_tpt` int(11) DEFAULT NULL,
  `secondary_effects_ctz` int(11) DEFAULT NULL,
  `dispensation_type` int(11) DEFAULT NULL,
  `next_pickup_date` datetime DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `prophylaxis_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
