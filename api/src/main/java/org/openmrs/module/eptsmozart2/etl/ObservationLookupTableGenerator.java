package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class ObservationLookupTableGenerator extends InsertFromSelectGenerator {
	
	public static final String CREATE_TABLE_FILE_NAME = "observation_lookup.sql";
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	public String getTable() {
		return "observation_lookup";
	}
	
	@Override
	protected void etl() throws SQLException {
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".observation_lookup (concept_id, concept_name) ").append("SELECT concept_id, name FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".concept_name ")
		        .append("WHERE !voided AND locale='en' AND concept_name_type='FULLY_SPECIFIED'").toString();
		
		runSql(insertSql, null);
	}
}
