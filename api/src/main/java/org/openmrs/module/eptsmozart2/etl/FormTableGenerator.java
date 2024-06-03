package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class FormTableGenerator extends InsertFromSelectGenerator {
	
	public static final String CREATE_TABLE_FILE_NAME = "form.sql";
	
	public static final Integer[] ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 7, 9, 13, 18, 21, 28,
	        34, 35, 51, 60, 80, 81, 90 };
	
	public static final Integer[] VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 52, 53 };
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	public String getTable() {
		return "form";
	}
	
	@Override
	protected void etl() throws SQLException {
		etlEncounterDatetimeBasedRecords();
		etlValueDatetimeBasedRecords();
	}
	
	private void etlEncounterDatetimeBasedRecords() throws SQLException {
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".form (encounter_id, encounter_uuid, form_id, encounter_type, patient_uuid, ")
		        .append("created_date, encounter_date, change_date, location_uuid, source_database) ")
		        .append("SELECT e.encounter_id, e.uuid, f.form_id, e.encounter_type, p.patient_uuid, ")
		        .append("e.date_created, e.encounter_datetime, e.date_changed, l.uuid, '")
		        .append(Mozart2Properties.getInstance().getSourceOpenmrsInstance()).append("' AS source_database FROM ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on p.patient_id = e.patient_id AND !e.voided ").append("AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS))
		        .append(" AND e.encounter_datetime <= ?  JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".form f on f.form_id = e.form_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id ").append(" ORDER BY e.encounter_id").toString();
		
		Map<Integer, Object> params = new HashMap<>();
		params.put(1, Date.valueOf(Mozart2Properties.getInstance().getEndDate()));
		runSql(insertSql, params);
	}
	
	private void etlValueDatetimeBasedRecords() throws SQLException {
		final Integer[] CONCEPTS = new Integer[] { 23891, 23866 };
		String insertSql = new StringBuilder("INSERT IGNORE INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".form (encounter_id, encounter_uuid, form_id, encounter_type, patient_uuid, ")
		        .append("created_date, encounter_date, change_date, location_uuid, source_database) ")
		        .append("SELECT e.encounter_id, e.uuid, f.form_id, e.encounter_type, p.patient_uuid, ")
		        .append("e.date_created, o.value_datetime, e.date_changed, l.uuid, '")
		        .append(Mozart2Properties.getInstance().getSourceOpenmrsInstance()).append("' AS source_database FROM ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on p.patient_id = e.patient_id AND !e.voided AND e.encounter_type IN ")
		        .append(inClause(VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS)).append(" JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".form f on f.form_id = e.form_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPTS)).append(" AND o.value_datetime <= ? ").append("ORDER BY e.encounter_id")
		        .toString();
		
		Map<Integer, Object> params = new HashMap<>();
		params.put(1, Date.valueOf(Mozart2Properties.getInstance().getEndDate()));
		runSql(insertSql, params);
	}
}
