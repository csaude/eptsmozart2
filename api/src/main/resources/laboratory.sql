CREATE TABLE `laboratory` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `encounter_uuid` char(38) DEFAULT NULL,
  `encounter_date` datetime DEFAULT NULL,
  `lab_test_id` int(11) NOT NULL,
  `request` int(11) DEFAULT NULL,
  `order_date` datetime DEFAULT NULL,
  `sample_collection_date` datetime DEFAULT NULL,
  `result_report_date` datetime DEFAULT NULL,
  `result_qualitative_id` int(11) DEFAULT NULL,
  `result_numeric` double DEFAULT NULL,
  `result_units` varchar(255) DEFAULT NULL,
  `result_comment` varchar(255) DEFAULT NULL,
  `specimen_type_id` int(11) DEFAULT NULL,
  `labtest_uuid` char(38) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `laboratory_labtest_uuid` (`labtest_uuid`),
  KEY `laboratory_encounter_uuid` (`encounter_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8