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

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/15/22.
 */
public class IdentifierTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "identifier.sql";
	
	public static final Integer[] IDENTIFIER_TYPES = new Integer[] { 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".identifier (patient_uuid, identifier_type, identifier_value, `preferred`, ")
		        .append("identifier_uuid) VALUES (?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setString(1, results.getString("patient_uuid"));
				insertStatement.setInt(2, results.getInt("identifier_type"));
				insertStatement.setString(3, results.getString("identifier"));
				insertStatement.setBoolean(4, results.getBoolean("preferred"));
				insertStatement.setString(5, results.getString("identifier_uuid"));
				
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
		return "identifier";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(*) FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".patient_identifier pi JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON pi.patient_id = p.patient_id AND !pi.voided AND pi.identifier_type IN ")
		        .append(inClause(IDENTIFIER_TYPES)).toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT pi.patient_id, p.patient_uuid, pi.identifier_type, pi.identifier, ")
		        .append("pi.preferred, pi.uuid as identifier_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".patient_identifier pi JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON pi.patient_id = p.patient_id AND !pi.voided AND pi.identifier_type IN ")
		        .append(inClause(IDENTIFIER_TYPES)).append(" ORDER BY pi.patient_identifier_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
