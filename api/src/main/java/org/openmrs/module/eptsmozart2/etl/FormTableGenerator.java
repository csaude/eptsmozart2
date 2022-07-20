package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class FormTableGenerator implements Generator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FormTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "form.sql";
	
	private static final Integer[] ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 7, 9, 13, 18, 21, 28,
	        34, 35, 51, 60 };
	
	private static final Integer[] VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS = new Integer[] { 52, 53 };
	
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
                .append(".form (encounter_id, encounter_uuid, form_id, form_name, ")
                .append("encounter_type, encounter_type_name, patient_id, patient_uuid, created_date, encounter_date, change_date, location_id, ")
                .append("location_uuid, source_database) SELECT e.encounter_id, e.form_id, e.uuid as encounter_uuid, e.encounter_type, e.patient_id, ")
                .append("e.date_created, e.date_changed, e.encounter_datetime, e.location_id, f.name as form_name, et.name as encounter_type_name, ")
                .append("pe.uuid as patient_uuid, l.uuid as location_uuid, '").append(AppProperties.getInstance().getDatabaseName())
                .append("' AS source_database FROM ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".encounter as e left join ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".form as f on e.form_id = f.form_id left join ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".encounter_type as et on e.encounter_type = et.encounter_type_id ").append("left join ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".person pe on e.patient_id = pe.person_id ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".location l on e.location_id = l.location_id WHERE !e.voided ").append("AND e.encounter_type IN ")
                .append(inClause(ENCOUNTER_DATETIME_BASED_ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN (")
                .append(AppProperties.getInstance().getLocationsIdsString()).append(") AND e.encounter_datetime <= '")
                .append(AppProperties.getInstance().getFormattedEndDate(null))
                .append("' AND e.patient_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .append(" ORDER BY e.encounter_id").toString();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            int moreToGo = statement.executeUpdate(insertSql);
            toBeGenerated += moreToGo;
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertSql, e);
            throw e;
        }
    }
	
	private void etlValueDatetimeBasedRecords() throws SQLException {
        String insertSql = new StringBuilder("INSERT INTO ")
                .append(AppProperties.getInstance().getNewDatabaseName())
                .append(".form (encounter_id, encounter_uuid, form_id, form_name, ")
                .append("encounter_type, encounter_type_name, patient_id, patient_uuid, created_date, encounter_date, change_date, location_id, ")
                .append("location_uuid, source_database) SELECT e.encounter_id, e.form_id, e.uuid as encounter_uuid, e.encounter_type, e.patient_id, ")
                .append("e.date_created, e.date_changed, e.encounter_datetime, e.location_id, f.name as form_name, et.name as encounter_type_name, ")
                .append("pe.uuid as patient_uuid, l.uuid as location_uuid, '").append(AppProperties.getInstance().getDatabaseName())
                .append("' AS source_database FROM ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".encounter as e left join ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".form as f on e.form_id = f.form_id left join ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".encounter_type as et on e.encounter_type = et.encounter_type_id ").append("left join ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".person pe on e.patient_id = pe.person_id ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".location l on e.location_id = l.location_id AND !l.retired INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".obs o on e.encounter_id = e.encounter_id AND !o.voided AND o.concept_id IN (23891,23866) AND o.value_datetime <= '")
                .append(AppProperties.getInstance().getFormattedEndDate(null)).append("' WHERE !e.voided ")
                .append("AND e.encounter_type IN ").append(inClause(VALUE_DATETIME_BASED_ENCOUNTER_TYPE_IDS))
                .append(" AND e.location_id IN (").append(AppProperties.getInstance().getLocationsIdsString())
                .append(") AND e.patient_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .append(" ORDER BY e.encounter_id").toString();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            int moreToGo = statement.executeUpdate(insertSql);
            toBeGenerated += moreToGo;
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertSql, e);
            throw e;
        }
    }
}
