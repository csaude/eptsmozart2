package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/21/22.
 */
public class ProgramTableGenerator implements Generator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "program.sql";
	
	private static Integer[] PROGRAM_IDS = new Integer[] { 1, 2, 3 };
	
	private Integer toBeGenerated = 0;
	
	private Integer currentlyGenerated = 0;
	
	@Override
	public String getTable() {
		return "program";
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
			createTable(Utils.readFileToString(CREATE_TABLE_FILE_NAME));
			etl();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void etl() throws SQLException {
        String insertSql = new StringBuilder("INSERT INTO ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".program (patient_id, patient_uuid,program_id, program, date_enrolled,date_completed, ")
                .append("location_id, location_name, location_uuid, enrolment_uuid, source_database) SELECT p.patient_id, p.patient_uuid, ")
                .append("pg.program_id, program.name, pg.date_enrolled, pg.date_completed, pg.location_id, l.name, l.uuid,  pg.uuid, '")
                .append(AppProperties.getInstance().getDatabaseName()).append("' AS source_database FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient p JOIN ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".patient_program pg ON p.patient_id = pg.patient_id JOIN ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".program ON pg.program_id = program.program_id JOIN ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".location l ON l.location_id=pg.location_id ")
                .append("WHERE !pg.voided AND pg.location_id IN (").append(AppProperties.getInstance().getLocationsIdsString())
                .append(") AND pg.program_id IN ").append(inClause(PROGRAM_IDS)).append(" AND pg.date_enrolled <= '")
                .append(AppProperties.getInstance().getFormattedEndDate(null))
                .append("' ORDER BY pg.patient_program_id").toString();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            int moreToGo = statement.executeUpdate(insertSql);
            toBeGenerated += moreToGo;
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertSql, e);
            throw e;
        }
    }
}
