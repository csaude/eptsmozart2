CREATE TABLE `tb_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) NOT NULL,
  `encounter_date` datetime NOT NULL,
  `tb_symptom` int(11) DEFAULT NULL,
  `symptom_fever` int(11) DEFAULT NULL,
  `symptom_weight_loss` int(11) DEFAULT NULL,
  `symptom_night_sweat` int(11) DEFAULT NULL,
  `symptom_cough` int(11) DEFAULT NULL,
  `symptom_asthenia` int(11) DEFAULT NULL,
  `symptom_tb_contact` int(11) DEFAULT NULL,
  `symptom_adenopathy` int(11) DEFAULT NULL,
  `tb_diagnose` int(11) DEFAULT NULL,
  `tb_treatment` int(11) DEFAULT NULL,
  `tb_treatment_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `tb_data_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
