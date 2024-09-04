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
public class ObservationTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "observation.sql";
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 1406, 1190, 1255, 1369, 1465, 1982, 2015, 6300, 6332, 23764,
	        23783, 23807, 23808, 23811 };
	
	public static final int VALUE_DATETIME_CONCEPT = 1190;
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 5, 6, 9, 18, 35, 51, 53 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int CONCEPT_ID_POS = 2;
	
	protected final int OBS_DATE_POS = 3;
	
	protected final int VAL_NUM_POS = 4;
	
	protected final int VAL_CONCEPT_POS = 5;
	
	protected final int VAL_TEXT_POS = 6;
	
	protected final int VAL_DATE_POS = 7;
	
	protected final int OBS_UUID_POS = 8;
	
	protected final int ENCOUNTER_DATE_POS = 9;
	
	protected final int ENC_TYPE_POS = 10;
	
	protected final int ENC_CREATED_DATE_POS = 11;
	
	protected final int ENC_CHANGE_DATE_POS = 12;
	
	protected final int FORM_ID_POS = 13;
	
	protected final int PATIENT_UUID_POS = 14;
	
	protected final int LOC_UUID_POS = 15;
	
	protected final int SRC_DB_POS = 16;
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".observation (encounter_uuid, concept_id, observation_date, ")
		        .append("value_numeric, value_concept_id, value_text, ")
		        .append("value_datetime, obs_uuid, encounter_date, encounter_type, encounter_created_date, ")
		        .append("encounter_change_date, form_id, patient_uuid, location_uuid, source_database) ")
		        .append(" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				final int conceptId = results.getInt("concept_id");
				insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
				insertStatement.setInt(CONCEPT_ID_POS, conceptId);
				insertStatement.setDate(OBS_DATE_POS, results.getDate("obs_datetime"));
				insertStatement.setTimestamp(ENCOUNTER_DATE_POS, results.getTimestamp("encounter_datetime"));
				insertStatement.setInt(ENC_TYPE_POS, results.getInt("encounter_type"));
				insertStatement.setTimestamp(ENC_CREATED_DATE_POS, results.getTimestamp("e_date_created"));
				insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, results.getTimestamp("e_date_changed"));
				insertStatement.setInt(FORM_ID_POS, results.getInt("form_id"));
				insertStatement.setString(PATIENT_UUID_POS, results.getString("patient_uuid"));
				insertStatement.setString(LOC_UUID_POS, results.getString("loc_uuid"));
				insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
				
				String valueText = results.getString("value_text");
				double valueNumeric = results.getDouble("value_numeric");
				if (results.wasNull()) {
					//MOZ2-167
					if (conceptId == 165326) {
						try {
							valueNumeric = Double.parseDouble(valueText);
							insertStatement.setDouble(VAL_NUM_POS, valueNumeric);
						}
						catch (NumberFormatException e) {
							// Ignore
							insertStatement.setNull(VAL_NUM_POS, Types.DOUBLE);
						}
					} else {
						insertStatement.setNull(VAL_NUM_POS, Types.DOUBLE);
					}
				} else {
					insertStatement.setDouble(VAL_NUM_POS, valueNumeric);
				}
				
				int valueCoded = results.getInt("value_coded");
				if (results.wasNull()) {
					insertStatement.setNull(VAL_CONCEPT_POS, Types.INTEGER);
				} else {
					insertStatement.setInt(VAL_CONCEPT_POS, valueCoded);
				}
				
				//MOZ-2:162 & MOZ2-163
				if (conceptId == 23808 || conceptId == 1369) {
					insertStatement.setString(VAL_TEXT_POS, results.getString("comments"));
				} else {
					insertStatement.setString(VAL_TEXT_POS, valueText);
				}
				
				insertStatement.setDate(VAL_DATE_POS, results.getDate("value_datetime"));
				insertStatement.setString(OBS_UUID_POS, results.getString("uuid"));
				
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
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" WHERE !o.voided AND o.concept_id IN ").append(inClause(CONCEPT_IDS))
		        .append(" AND CASE WHEN o.concept_id = ").append(VALUE_DATETIME_CONCEPT)
		        .append(" THEN o.value_datetime <= '").append(endDate).append("' ELSE o.obs_datetime <= '").append(endDate)
		        .append("' END");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT o.*, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, o.person_id as patient_id, ")
		        .append("e.date_created as e_date_created, e.date_changed as e_date_changed, ")
		        .append("e.encounter_datetime, e.form_id, p.patient_uuid, l.uuid as loc_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id WHERE !o.voided AND o.concept_id IN ")
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
