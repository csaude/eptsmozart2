package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/11/22.
 */
public class ClinicalConsultationTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "clinical_consultation.sql";
	
	private static final Integer SCHEDULED_DATE_CONCEPT = 1410;
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException {
		return prepareInsertStatement(resultSet, null);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(AppProperties.getInstance().getNewDatabaseName())
		        .append(".clinical_consultation (encounter_id, encounter_uuid, encounter_type, patient_id, patient_uuid, ")
		        .append(
		            "consultation_date, scheduled_date, observation_date, source_database) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
		        .toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setInt(1, results.getInt("encounter_id"));
				insertStatement.setString(2, results.getString("uuid"));
				insertStatement.setInt(3, results.getInt("encounter_type"));
				insertStatement.setInt(4, results.getInt("patient_id"));
				insertStatement.setString(5, results.getString("patient_uuid"));
				insertStatement.setDate(6, results.getDate("encounter_datetime"));
				insertStatement.setDate(7, results.getDate("value_datetime"));
				insertStatement.setDate(8, results.getDate("obs_datetime"));
				insertStatement.setString(9, AppProperties.getInstance().getDatabaseName());
				
				insertStatement.addBatch();
				++count;
			}
			return insertStatement;
		}
		catch (SQLException e) {
			LOGGER.error("Error preparing insert statement for table {}", getTable());
			throw e;
		}
	}
	
	@Override
	protected String getTable() {
		return "clinical_consultation";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter WHERE !voided AND encounter_type IN ").append(inClause(ENCOUNTER_TYPE_IDS))
		        .append(" AND patient_id IN (SELECT patient_id FROM ")
				.append(AppProperties.getInstance().getNewDatabaseName()).append(".patient )");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder(
		        "SELECT e.*, o.obs_datetime, o.concept_id, o.value_datetime, o.obs_datetime, pe.uuid as patient_uuid FROM ")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".encounter e LEFT JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id = ")
		        .append(SCHEDULED_DATE_CONCEPT).append(" JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".person pe ON ").append("e.patient_id = pe.person_id")
		        .append(" WHERE !e.voided AND e.encounter_type IN ").append(inClause(ENCOUNTER_TYPE_IDS))
		        .append(" AND e.patient_id IN (SELECT patient_id FROM ")
				.append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
		        .append(" ORDER BY e.encounter_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
