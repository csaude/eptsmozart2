SET sql_log_bin = 0;
DROP TABLE IF EXISTS `encounter_obs`;

CREATE TABLE `encounter_obs` (
  `encounter_id` int(11) NOT NULL DEFAULT '0',
  `encounter_type` int(11) NOT NULL,
  `patient_id` int(11) NOT NULL DEFAULT '0',
  `location_id` int(11) DEFAULT NULL,
  `form_id` int(11) DEFAULT NULL,
  `encounter_datetime` datetime DEFAULT NULL,
  `e_date_created` datetime DEFAULT NULL,
  `e_date_changed` datetime DEFAULT NULL,
  `encounter_uuid` char(38) CHARACTER SET utf8 NOT NULL,
  `obs_id` int(11) NOT NULL,
  `concept_id` int(11) NOT NULL,
  `obs_datetime` datetime DEFAULT NULL,
  `obs_group_id` int(11) DEFAULT NULL,
  `value_coded` int(11) DEFAULT NULL,
  `value_drug` int(11) DEFAULT NULL,
  `value_datetime` datetime DEFAULT NULL,
  `value_numeric` double DEFAULT NULL,
  `value_text` text CHARACTER SET utf8,
  `comments` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `o_date_created` datetime DEFAULT NULL,
  `obs_uuid` char(38) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`obs_id`),
  UNIQUE KEY `encobs_obsuuid_idx` (`obs_uuid`),
  KEY `encobs_obs_id_idx` (`obs_id`),
  KEY `encobs_loc_id_idx` (`location_id`),
  KEY `encobs_enc_id_idx` (`encounter_id`),
  KEY `encobs_obsgroup_id_idx` (`obs_group_id`),
  KEY `encobs_concept_idx` (`concept_id`),
  KEY `encobs_valuecoded_idx` (`value_coded`),
  KEY `encobs_patient_idx` (`patient_id`),
  KEY `encobs_encuuid_idx` (`encounter_uuid`),
  KEY `encobs_encdatetime_idx` (`encounter_datetime`),
  KEY `encobs_obsdatetime_idx` (`obs_datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `encounter_obs`(encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime,
e_date_created, e_date_changed,encounter_uuid, obs_id, concept_id, obs_datetime, obs_group_id, value_coded,
value_drug, value_datetime, value_numeric, value_text, comments, o_date_created, obs_uuid)
SELECT e.encounter_id, encounter_type, patient_id, e.location_id, form_id,
encounter_datetime, e.date_created e_date_created, e.date_changed e_date_changed, e.uuid encounter_uuid,
obs_id, concept_id, obs_datetime, obs_group_id, value_coded,value_drug, value_datetime, value_numeric,
value_text, comments, o.date_created o_date_created, o.uuid obs_uuid
FROM encounter e 
JOIN obs o on e.encounter_id=o.encounter_id AND !e.voided AND !o.voided 
AND (o.obs_datetime NOT LIKE '%00-00-00 00:00:00%' OR e.encounter_datetime NOT LIKE '%00-00-00 00:00:00%');

SET sql_log_bin = 1;