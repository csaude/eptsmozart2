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
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 1190, 1343, 1369, 1465, 1982, 5085, 5086, 5089, 5356, 6332,
	        165174, 23808 };
	
	public static final int VALUE_DATETIME_CONCEPT = 1190;
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 9, 18, 35, 53 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException {
		return prepareInsertStatement(resultSet, null);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".observation (encounter_uuid, concept_id, observation_date, ")
		        .append("value_numeric, value_concept_id, value_text, ")
		        .append("value_datetime, obs_uuid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				final int conceptId = results.getInt("concept_id");
				insertStatement.setString(1, results.getString("encounter_uuid"));
				insertStatement.setInt(2, conceptId);
				insertStatement.setDate(3, results.getDate("obs_datetime"));
				
				double valueNumeric = results.getDouble("value_numeric");
				if (results.wasNull()) {
					insertStatement.setNull(4, Types.DOUBLE);
				} else {
					insertStatement.setDouble(4, valueNumeric);
				}
				
				int valueCoded = results.getInt("value_coded");
				if (results.wasNull()) {
					insertStatement.setNull(5, Types.INTEGER);
				} else {
					insertStatement.setInt(5, valueCoded);
				}
				
				//MOZ-2:162 & MOZ2-163
				if (conceptId == 23808 || conceptId == 1369) {
					insertStatement.setString(6, results.getString("comments"));
				} else {
					insertStatement.setString(6, results.getString("value_text"));
				}
				
				insertStatement.setDate(7, results.getDate("value_datetime"));
				insertStatement.setString(8, results.getString("uuid"));
				
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
