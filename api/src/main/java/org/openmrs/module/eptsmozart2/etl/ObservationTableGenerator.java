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
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".observation (encounter_uuid, encounter_date, encounter_type, ")
		        .append("concept_id, observation_date, value_numeric, value_concept_id, value_text, ")
		        .append("value_datetime, obs_uuid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setString(1, results.getString("encounter_uuid"));
				insertStatement.setDate(2, results.getDate("encounter_date"));
				insertStatement.setInt(3, results.getInt("encounter_type"));
				insertStatement.setInt(4, results.getInt("concept_id"));
				insertStatement.setDate(5, results.getDate("obs_datetime"));
				
				double valueNumeric = results.getDouble("value_numeric");
				if (results.wasNull()) {
					insertStatement.setNull(6, Types.DOUBLE);
				} else {
					insertStatement.setDouble(6, valueNumeric);
				}
				
				int valueCoded = results.getInt("value_coded");
				if (results.wasNull()) {
					insertStatement.setNull(7, Types.INTEGER);
				} else {
					insertStatement.setInt(7, valueCoded);
				}
				
				insertStatement.setString(8, results.getString("value_text"));
				insertStatement.setDate(9, results.getDate("value_datetime"));
				insertStatement.setString(10, results.getString("uuid"));
				
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
		        .append("p.patient_uuid, e.encounter_datetime as encounter_date FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" AND CASE WHEN o.concept_id = ").append(VALUE_DATETIME_CONCEPT)
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
