package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 11/10/22.
 */
public class KeyPopVulnerableTableGenerator extends ObservableGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KeyPopVulnerableTableGenerator.class);
	
	public static final String CREATE_TABLE_FILE_NAME = "keypop_vulnerable.sql";
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 35 };
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 23703, 23710 };
	
	@Override
	public String getTable() {
		return "keypop_vulnerable";
	}
	
	@Override
	public Void call() throws Exception {
		long startTime = System.currentTimeMillis();
		try {
			createTable(Utils.readFileToString(CREATE_TABLE_FILE_NAME));
			etl();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void etl() throws SQLException {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".keypop_vulnerable (encounter_uuid, pop_type, pop_id, pop_other) ")
		        .append("SELECT e.uuid as encounter_uuid, o.concept_id, o.value_coded, o.value_text FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.encounter_datetime <= '").append(endDate).append("'")
		        .append(" AND !o.voided AND o.concept_id IN ").append(inClause(CONCEPT_IDS)).toString();
		
		runSql(insertSql, null);
	}
}
