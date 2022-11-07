package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class ObservationTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "observation.sql";
	
	private static final Integer[] CONCEPT_IDS = new Integer[] { 1190, 1982, 6332, 165174, 23703, 23710 };
	
	private static final int VALUE_DATETIME_CONCEPT = 1190;
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 9, 18, 35, 53 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException {
		return prepareInsertStatement(resultSet, null);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(
		            ".observation (encounter_id, encounter_uuid, encounter_date, encounter_type, patient_id, patient_uuid, ")
		        .append(
		            "concept_id, concept_name, observation_date, value_numeric, value_coded, value_coded_name, value_text, ")
		        .append("value_datetime, date_created, obs_uuid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
				insertStatement.setString(2, results.getString("encounter_uuid"));
				insertStatement.setDate(3, results.getDate("encounter_date"));
				insertStatement.setInt(4, results.getInt("encounter_type"));
				insertStatement.setInt(5, results.getInt("patient_id"));
				insertStatement.setString(6, results.getString("patient_uuid"));
				insertStatement.setInt(7, results.getInt("concept_id"));
				insertStatement.setString(8, results.getString("concept_name"));
				insertStatement.setDate(9, results.getDate("obs_datetime"));
				
				double valueNumeric = results.getDouble("value_numeric");
				if (results.wasNull()) {
					insertStatement.setNull(10, Types.DOUBLE);
				} else {
					insertStatement.setDouble(10, valueNumeric);
				}
				
				int valueCoded = results.getInt("value_coded");
				if (results.wasNull()) {
					insertStatement.setNull(11, Types.INTEGER);
				} else {
					insertStatement.setInt(11, valueCoded);
				}
				
				insertStatement.setString(12, results.getString("value_coded_name"));
				insertStatement.setString(13, results.getString("value_text"));
				insertStatement.setDate(14, results.getDate("value_datetime"));
				insertStatement.setTimestamp(15, results.getTimestamp("date_created"));
				insertStatement.setString(16, results.getString("uuid"));
				
				insertStatement.addBatch();
				++count;
			}
			return insertStatement;
		}
		catch (SQLException e) {
			LOGGER.error("Error preparing insert statement for table {}", getTable());
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		}
	}
	
	@Override
	public String getTable() {
		return "observation";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" AND CASE WHEN o.concept_id = ").append(VALUE_DATETIME_CONCEPT)
		        .append(" THEN o.value_datetime <= '").append(endDate).append("' ELSE o.obs_datetime <= '").append(endDate)
		        .append("' END");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT o.*, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, o.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_datetime as encounter_date, cn.name as concept_name, ")
		        .append("cn1.name as value_coded_name FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" LEFT JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".concept_name cn on cn.concept_id = o.concept_id AND !cn.voided AND cn.locale = 'en' AND ")
		        .append("cn.locale_preferred LEFT JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".concept_name cn1 on cn1.concept_id = o.value_coded AND !cn1.voided AND cn1.locale = 'en' AND ")
		        .append("cn1.locale_preferred  WHERE !o.voided AND o.concept_id IN ").append(inClause(CONCEPT_IDS))
		        .append(" AND CASE WHEN o.concept_id = ").append(VALUE_DATETIME_CONCEPT)
		        .append(" THEN o.value_datetime <= '").append(endDate).append("' ELSE o.obs_datetime <= '").append(endDate)
		        .append("' END ORDER BY o.obs_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
