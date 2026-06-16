SET sql_log_bin = 0;

INSERT INTO `encounter_obs`(encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime,
e_date_created, e_date_changed,encounter_uuid, obs_id, concept_id, obs_datetime, obs_group_id, value_coded,
value_drug, value_datetime, value_numeric, value_text, comments, o_date_created, obs_uuid)
SELECT e.encounter_id, encounter_type, patient_id, e.location_id, form_id,
encounter_datetime, e.date_created e_date_created, e.date_changed e_date_changed, e.uuid encounter_uuid,
obs_id, concept_id, obs_datetime, obs_group_id, value_coded,value_drug, value_datetime, value_numeric,
value_text, comments, o.date_created o_date_created, o.uuid obs_uuid
FROM encounter e
JOIN obs o on e.encounter_id=o.encounter_id AND !e.voided AND !o.voided
AND (o.obs_datetime NOT LIKE '%00-00-00 00:00:00%' OR e.encounter_datetime NOT LIKE '%00-00-00 00:00:00%')
AND o.obs_id BETWEEN ? AND ?;

SET sql_log_bin = 1;