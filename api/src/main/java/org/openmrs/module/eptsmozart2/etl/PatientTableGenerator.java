package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.getPatientListQuery;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/14/22.
 */
public class PatientTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PatientTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "patient.sql";
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient (patient_id, patient_uuid, gender, birthdate, birthdate_estimated, ")
		        .append("date_created, source_database) VALUES (?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setInt(1, results.getInt("patient_id"));
				insertStatement.setString(2, results.getString("patient_uuid"));
				insertStatement.setString(3, results.getString("gender"));
				insertStatement.setDate(4, results.getDate("birthdate"));
				insertStatement.setBoolean(5, results.getBoolean("birthdate_estimated"));
				insertStatement.setTimestamp(6, results.getTimestamp("date_created"));
				insertStatement.setString(7, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
				
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
		return "patient";
	}
	
	@Override
	protected String countQuery() {
		return "SELECT COUNT(*) FROM ".concat(Mozart2Properties.getInstance().getDatabaseName())
		        .concat(".patient WHERE !voided").concat(" AND patient_id IN (").concat(getPatientListQuery()).concat(")");
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT p.patient_id, pe.uuid as patient_uuid, pe.gender, pe.birthdate, ")
		        .append("pe.birthdate_estimated, pe.dead, pe.death_date, p.date_created ").append("FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".patient p inner join ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".person pe on p.patient_id = pe.person_id WHERE !p.voided ").append(" AND p.patient_id IN (")
		        .append(getPatientListQuery()).append(")").append("ORDER BY p.patient_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
