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


/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class FormTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FormTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "form.sql";
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
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
		        .append(".form (encounter_id, encounter_uuid, form_id, form_name, ")
		        .append(
		            "encounter_type, encounter_type_name, patient_id, patient_uuid, created_date, encounter_date, change_date, location_id, ")
		        .append("location_uuid, source_database) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
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
				insertStatement.setInt(3, results.getInt("form_id"));
				insertStatement.setString(4, results.getString("form_name"));
				insertStatement.setInt(5, results.getInt("encounter_type"));
				insertStatement.setString(6, results.getString("encounter_type_name"));
				insertStatement.setInt(7, results.getInt("patient_id"));
				insertStatement.setString(8, results.getString("patient_uuid"));
				insertStatement.setDate(9, results.getDate("date_created"));
				insertStatement.setDate(10, results.getDate("encounter_datetime"));
				insertStatement.setDate(11, results.getDate("date_changed"));
				insertStatement.setInt(12, results.getInt("location_id"));
				insertStatement.setString(13, results.getString("location_uuid"));
				insertStatement.setString(14, AppProperties.getInstance().getDatabaseName());
				
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
		return "form";
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter WHERE !voided  AND patient_id IN (SELECT patient_id FROM ")
				.append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
		        .toString();
		
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder(
		        "SELECT e.encounter_id, e.form_id, e.uuid as encounter_uuid, e.encounter_type, e.patient_id, ")
		        .append(
		            "e.date_created, e.date_changed, e.encounter_datetime, e.location_id, f.name as form_name, et.name as encounter_type_name, ")
		        .append("pe.uuid as patient_uuid, l.uuid as location_uuid FROM ")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".encounter as e left join ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".form as f on e.form_id = f.form_id left join ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter_type as et on e.encounter_type = et.encounter_type_id ").append("left join ")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".person pe on e.patient_id = pe.person_id ")
		        .append("left join ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".location l on e.location_id = l.location_id WHERE !e.voided ")
				.append(" AND e.patient_id IN (SELECT patient_id FROM ")
				.append(AppProperties.getInstance().getNewDatabaseName())
				.append(".patient)").append(" ORDER BY e.encounter_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
}
