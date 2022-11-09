package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.DbUtils;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class ObservationLookupTableGenerator extends ObservableGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationLookupTableGenerator.class);
	
	public static final String CREATE_TABLE_FILE_NAME = "observation_lookup.sql";
	
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	public String getTable() {
		return "observation_lookup";
	}
	
	@Override
	public Void call() throws Exception {
		long startTime = System.currentTimeMillis();
		try {
			createTable(getCreateTableSql());
			conceptETL();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void conceptETL() throws SQLException {
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".observation_lookup (concept_id, concept_name) ").append("SELECT concept_id, name FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".concept_name ")
		        .append("WHERE !voided AND locale='en' AND concept_name_type='FULLY_SPECIFIED'").toString();
		
		runSql(insertSql, null);
	}
}
