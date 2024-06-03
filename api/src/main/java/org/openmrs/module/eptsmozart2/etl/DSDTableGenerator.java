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
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 11/1/22.
 */
public class DSDTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "dsd.sql";
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 35 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT IGNORE INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".dsd (encounter_uuid, dsd_id, dsd_state_id, dsd_uuid, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, ")
		        .append("form_id, patient_uuid, location_uuid, source_database, encounter_date) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setString(1, results.getString("encounter_uuid"));
				insertStatement.setTimestamp(12, results.getTimestamp("encounter_datetime"));
				insertStatement.setInt(2, results.getInt("dsd_id"));
				insertStatement.setInt(3, results.getInt("dsd_state_id"));
				insertStatement.setString(4, results.getString("dsd_uuid"));
				insertStatement.setInt(5, results.getInt("encounter_type"));
				insertStatement.setTimestamp(6, results.getTimestamp("e_date_created"));
				insertStatement.setTimestamp(7, results.getTimestamp("e_date_changed"));
				insertStatement.setInt(8, results.getInt("form_id"));
				insertStatement.setString(9, results.getString("patient_uuid"));
				insertStatement.setString(10, results.getString("loc_uuid"));
				insertStatement.setString(11, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
				
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
		return "dsd";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o1 JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o2 on o1.obs_group_id = o2.obs_group_id AND o1.encounter_id = o2.encounter_id AND ")
		        .append("o1.concept_id = 165174 AND o2.concept_id = 165322 AND !o1.voided AND !o2.voided JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o1.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o1.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.encounter_datetime <= '").append(endDate).append("'");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder(
		        "SELECT o1.encounter_id, o1.value_coded as dsd_id, o2.value_coded as dsd_state_id, o1.date_created, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, e.date_created as e_date_created, ")
		        .append("e.date_changed as e_date_changed, e.form_id, o1.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_datetime, o1.uuid as dsd_uuid, e.form_id, l.uuid as loc_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o1 JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o2 on o1.obs_group_id = o2.obs_group_id AND o1.encounter_id = o2.encounter_id AND ")
		        .append("o1.concept_id = 165174 AND o2.concept_id = 165322 AND !o1.voided AND !o2.voided JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o1.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o1.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.encounter_datetime <= '").append(endDate)
		        .append("' JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id ORDER BY o1.obs_group_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
