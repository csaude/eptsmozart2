CREATE TABLE `counseling` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `encounter_date` datetime NOT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_type` int(11) DEFAULT NULL,
  `patient_uuid` char(38) DEFAULT NULL,
  `encounter_created_date` datetime DEFAULT NULL,
  `encounter_change_date` datetime DEFAULT NULL,
  `location_uuid` char(38) DEFAULT NULL,
  `diagnosis_reveal` int(11) DEFAULT NULL,
  `hiv_disclosure` int(11) DEFAULT NULL,
  `adherence_plan` int(11) DEFAULT NULL,
  `secondary_effects` int(11) DEFAULT NULL,
  `adherence_art` int(11) DEFAULT NULL,
  `adherence_percent` double DEFAULT NULL,
  `consultation_reason` int(11) DEFAULT NULL,
  `accept_contact` int(11) DEFAULT NULL,
  `accept_date` datetime DEFAULT NULL,
  `psychosocial_refusal` int(11) DEFAULT NULL,
  `psychosocial_sick` int(11) DEFAULT NULL,
  `psychosocial_notbelieve` int(11) DEFAULT NULL,
  `psychosocial_lotofpills` int(11) DEFAULT NULL,
  `psychosocial_feelbetter` int(11) DEFAULT NULL,
  `psychosocial_lackfood` int(11) DEFAULT NULL,
  `psychosocial_lacksupport` int(11) DEFAULT NULL,
  `psychosocial_depression` int(11) DEFAULT NULL,
  `psychosocial_notreveal` int(11) DEFAULT NULL,
  `psychosocial_toxicity` int(11) DEFAULT NULL,
  `psychosocial_lostpills` int(11) DEFAULT NULL,
  `psychosocial_stigma` int(11) DEFAULT NULL,
  `psychosocial_transport` int(11) DEFAULT NULL,
  `psychosocial_vm` int(11) DEFAULT NULL,
  `psychosocial_cultural` int(11) DEFAULT NULL,
  `psychosocial_druguse` int(11) DEFAULT NULL,
  `pp1` int(11) DEFAULT NULL,
  `pp2` int(11) DEFAULT NULL,
  `pp3` int(11) DEFAULT NULL,
  `pp4` int(11) DEFAULT NULL,
  `pp5` int(11) DEFAULT NULL,
  `pp6` int(11) DEFAULT NULL,
  `pp7` int(11) DEFAULT NULL,
  `keypop_lubricants` int(11) DEFAULT NULL,
  `source_database` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `counseling_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
