package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 11/10/22.
 */
public class KeyVulnerablePopTableGenerator extends InsertFromSelectGenerator {
	
	public static final String CREATE_TABLE_FILE_NAME = "key_vulnerable_pop.sql";
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 35 };
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 23703, 23710 };
	
	@Override
	public String getTable() {
		return "key_vulnerable_pop";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected void etl() throws SQLException {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		String insertSql = new StringBuilder("INSERT IGNORE INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".key_vulnerable_pop (encounter_uuid, pop_type, pop_id, pop_other, key_vulnerable_pop_uuid,  ")
		        .append(
		            "encounter_date, form_id, encounter_type, patient_uuid, encounter_created_date, encounter_change_date, location_uuid, source_database) ")
		        .append("SELECT e.uuid as encounter_uuid, o.concept_id, o.value_coded, o.value_text, o.uuid, ")
		        .append("e.encounter_datetime, e.form_id, e.encounter_type, p.patient_uuid, e.date_created, ")
		        .append("e.date_changed, l.uuid, '").append(Mozart2Properties.getInstance().getSourceOpenmrsInstance())
		        .append("' AS source_database FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(endDate).append("'")
		        .append(" AND !o.voided AND o.concept_id IN ").append(inClause(CONCEPT_IDS)).append(" JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id").toString();
		
		runSql(insertSql, null);
	}
}
