package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class FormTypeTableGenerator extends ObservableGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FormTypeTableGenerator.class);
	
	public static final String CREATE_TABLE_FILE_NAME = "form_type.sql";
	
	private Integer toBeGenerated = 0;
	
	private Integer currentlyGenerated = 0;
	
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	public String getTable() {
		return "form_type";
	}
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	@Override
	public Void call() throws Exception {
		long startTime = System.currentTimeMillis();
		try {
			createTable(getCreateTableSql());
			etl();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void etl() throws SQLException {
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".form_type (form_type_id, form_type_name, form_type_uuid, encounter_type_id, encounter_type_name) ")
		        .append("SELECT f.form_id, f.name, f.uuid, et.encounter_type_id, et.name FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".form f JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter_type et on f.encounter_type = et.encounter_type_id").toString();
		
		runSql(insertSql);
	}
	
	private void runSql(String sql) throws SQLException {
		try(Connection connection = ConnectionPool.getConnection();
			PreparedStatement ps = connection.prepareStatement(sql)) {
			int moreToGo = ps.executeUpdate();
			toBeGenerated += moreToGo;
			currentlyGenerated += moreToGo;
		} catch (SQLException e) {
			LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), sql, e);
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		}
	}
}
