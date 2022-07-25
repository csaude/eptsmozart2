package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class FormTableGenerator implements Generator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FormTableGenerator.class);
	
	public static final String CREATE_TABLE_FILE_NAME = "form.sql";
	
	public static final Integer[] ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 7, 9, 13, 18, 21, 28,
	        34, 35, 51, 60 };
	
	public static final Integer[] VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 52, 53 };
	
	private Integer toBeGenerated = 0;
	
	private Integer currentlyGenerated = 0;
	
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	public String getTable() {
		return "form";
	}
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	@Override
	public Void call() throws Exception {
		long startTime = System.currentTimeMillis();
		try {
			createTable(getCreateTableSql());
			etlEncounterDatetimeBasedRecords();
			etlValueDatetimeBasedRecords();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void etlEncounterDatetimeBasedRecords() throws SQLException {
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(AppProperties.getInstance().getNewDatabaseName())
		        .append(
		            ".form (encounter_id, encounter_uuid, form_id, form_name, encounter_type, encounter_type_name, patient_id, ")
		        .append(
		            "patient_uuid, created_date, encounter_date, change_date, location_id, location_uuid, source_database) ")
		        .append(
		            "SELECT e.encounter_id, e.uuid, f.form_id, f.name, et.encounter_type_id, et.name, p.patient_id, p.patient_uuid, ")
		        .append("e.date_created, e.encounter_datetime, e.date_changed, l.location_id, l.uuid, '")
		        .append(AppProperties.getInstance().getDatabaseName()).append("' AS source_database FROM ")
		        .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on p.patient_id = e.patient_id AND !e.voided ").append("AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN (")
		        .append(AppProperties.getInstance().getLocationsIdsString())
		        .append(") AND e.encounter_datetime <= ?  JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".form f on f.form_id = e.form_id JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter_type et on e.encounter_type = et.encounter_type_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id ").append(" ORDER BY e.encounter_id").toString();
		
		runSql(insertSql);
	}
	
	private void etlValueDatetimeBasedRecords() throws SQLException {
		final Integer[] CONCEPTS = new Integer[] { 23891, 23866 };
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(AppProperties.getInstance().getNewDatabaseName())
		        .append(
		            ".form (encounter_id, encounter_uuid, form_id, form_name, encounter_type, encounter_type_name, patient_id, ")
		        .append(
		            "patient_uuid, created_date, encounter_date, change_date, location_id, location_uuid, source_database) ")
		        .append(
		            "SELECT e.encounter_id, e.uuid, f.form_id, f.name, et.encounter_type_id, et.name, p.patient_id, p.patient_uuid, ")
		        .append("e.date_created, e.encounter_datetime, e.date_changed, l.location_id, l.uuid, '")
		        .append(AppProperties.getInstance().getDatabaseName()).append("' AS source_database FROM ")
		        .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on p.patient_id = e.patient_id AND !e.voided AND e.encounter_type IN ")
		        .append(inClause(VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN (")
		        .append(AppProperties.getInstance().getLocationsIdsString()).append(") JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".form f on f.form_id = e.form_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter_type et on e.encounter_type = et.encounter_type_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPTS)).append(" AND o.value_datetime <= ? ").append("ORDER BY e.encounter_id")
		        .toString();
		
		runSql(insertSql);
	}
	
	private void runSql(String sql) throws SQLException {
		try(Connection connection = ConnectionPool.getConnection();
			PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setDate(1, Date.valueOf(AppProperties.getInstance().getEndDate()));
			int moreToGo = ps.executeUpdate();
			toBeGenerated += moreToGo;
			currentlyGenerated += moreToGo;
		} catch (SQLException e) {
			LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), sql, e);
			throw e;
		}
	}
}
