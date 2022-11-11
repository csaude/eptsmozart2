package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/21/22.
 */
public class ProgramTableGenerator extends InsertFromSelectGenerator {
	
	public static final String CREATE_TABLE_FILE_NAME = "program.sql";
	
	@Override
	public String getTable() {
		return "program";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected void etl() throws SQLException {
        String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".program (patient_uuid,program_id, program, date_enrolled,date_completed, ")
                .append("location_id, location_name, location_uuid, enrolment_uuid) SELECT p.patient_uuid, ")
                .append("pg.program_id, program.name, pg.date_enrolled, pg.date_completed, pg.location_id, l.name, l.uuid,  pg.uuid FROM ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
                .append(Mozart2Properties.getInstance().getDatabaseName()).append(".patient_program pg ON p.patient_id = pg.patient_id JOIN ")
                .append(Mozart2Properties.getInstance().getDatabaseName()).append(".program ON pg.program_id = program.program_id ")
				.append("AND !pg.voided AND pg.date_enrolled <= ? JOIN ")
                .append(Mozart2Properties.getInstance().getDatabaseName()).append(".location l ON l.location_id=pg.location_id ")
				.append("ORDER BY pg.patient_program_id").toString();

		Map<Integer, Object> params = new HashMap<>();
		params.put(1, Date.valueOf(Mozart2Properties.getInstance().getEndDate()));
		runSql(insertSql, params);
    }
}
