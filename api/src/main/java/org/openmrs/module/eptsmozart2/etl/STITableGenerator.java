package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class STITableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(STITableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "sti.sql";
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 174, 6258 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int STI_CONCEPT_ID_POS = 2;
	
	protected final int STI_DATE_POS = 3;
	
	protected final int STI_VALUE_POS = 4;
	
	protected final int STI_UUID_POS = 5;
	
	protected final int ENCOUNTER_DATE_POS = 6;
	
	protected final int ENC_TYPE_POS = 7;
	
	protected final int ENC_CREATED_DATE_POS = 8;
	
	protected final int ENC_CHANGE_DATE_POS = 9;
	
	protected final int FORM_ID_POS = 10;
	
	protected final int PATIENT_UUID_POS = 11;
	
	protected final int LOC_UUID_POS = 12;
	
	protected final int SRC_DB_POS = 13;
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".sti (encounter_uuid, sti_concept_id, sti_date, sti_value, sti_uuid, ")
		        .append("encounter_date, encounter_type, encounter_created_date, encounter_change_date, ")
		        .append("form_id, patient_uuid, location_uuid, source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
				insertStatement.setTimestamp(ENCOUNTER_DATE_POS, results.getTimestamp("encounter_datetime"));
				insertStatement.setInt(ENC_TYPE_POS, results.getInt("encounter_type"));
				insertStatement.setTimestamp(ENC_CREATED_DATE_POS, results.getTimestamp("e_date_created"));
				insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, results.getTimestamp("e_date_changed"));
				insertStatement.setInt(FORM_ID_POS, results.getInt("form_id"));
				insertStatement.setString(PATIENT_UUID_POS, results.getString("patient_uuid"));
				insertStatement.setString(LOC_UUID_POS, results.getString("loc_uuid"));
				insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
				insertStatement.setInt(STI_CONCEPT_ID_POS, results.getInt("concept_id"));
				insertStatement.setDate(STI_DATE_POS, results.getDate("obs_datetime"));
				insertStatement.setInt(STI_VALUE_POS, results.getInt("value_coded"));
				insertStatement.setString(STI_UUID_POS, results.getString("uuid"));
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
		return "sti";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(*) FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" AND o.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("'").toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT e.uuid as encounter_uuid, e.encounter_datetime, ")
		        .append("e.encounter_id as e_encounter_id, e.encounter_type, ")
		        .append("e.date_created as e_date_created, e.date_changed as e_date_changed, ")
		        .append("e.form_id, p.patient_uuid, l.uuid as loc_uuid, o.* FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" AND o.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' ORDER BY o.obs_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
