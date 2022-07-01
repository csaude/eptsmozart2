package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/8/22.
 */
public class PatientStateTableGenerator implements Generator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "patient_state.sql";
	
	private Integer toBeGenerated = 0;
	
	private Integer currentlyGenerated = 0;
	
	@Override
	public Void call() throws SQLException, IOException {
		long startTime = System.currentTimeMillis();
		try {
			createTable();
			etlPersonDeathState();
			etlProgramBasedRecords();
			
			// FICHA RESUMO & FICHA CLINICA
			etlObsBasedRecords(new Integer[] { 6, 9, 53 }, new Integer[] { 6273, 6272 }, null);
			
			// 121 - TARV: VISITA DOMICILIARIA
			etlObsBasedRecords(new Integer[] { 21 }, new Integer[] { 2031, 23944, 23945, 2016 }, new Integer[] { 1366, 1706,
			        23863 });
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	@Override
	public String getTable() {
		return "patient_state";
	}
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	private void createTable() throws IOException, SQLException {
        String createSql = getCreateTableSql();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            statement.addBatch("USE ".concat(AppProperties.getInstance().getNewDatabaseName()));
            statement.addBatch("DROP TABLE IF EXISTS ".concat(getTable()));
            statement.addBatch(createSql);
            statement.executeBatch();
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), createSql, e);
            throw e;
        }
    }
	
	private void etlProgramBasedRecords() throws SQLException {
	    String countQuery = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".person pe ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName()).append(".patient_program pg ON ")
                .append("pe.person_id = pg.patient_id AND !pg.voided AND pg.program_id = 2 INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".patient_state ps on ps.patient_program_id=pg.patient_program_id AND !ps.voided INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".program_workflow_state pws on pws.program_workflow_state_id=ps.state AND pws.program_workflow_state_id != 6 INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".concept_name cn on cn.concept_id = pws.concept_id AND ")
                .append("!cn.voided AND cn.locale = 'en' AND cn.locale_preferred WHERE !pe.voided")
                .append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .toString();

        String insertStatement = new StringBuilder("INSERT INTO ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".patient_state (patient_id, patient_uuid, source_id, source_type, state_id, state, state_date, state_uuid, source_database) ")
                .append("SELECT pe.person_id as patient_id, pe.uuid as patient_uuid, 2 as source_id, 'Program enrolment' as source_type, cn.concept_id,")
                .append("cn.name, ps.start_date, ps.uuid, '").append(AppProperties.getInstance().getDatabaseName())
                .append("' AS source_database FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".person pe ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName()).append(".patient_program pg ON ")
                .append("pe.person_id = pg.patient_id AND !pg.voided AND pg.program_id = 2 INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".patient_state ps on ps.patient_program_id=pg.patient_program_id AND !ps.voided INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".program_workflow_state pws on pws.program_workflow_state_id=ps.state AND pws.program_workflow_state_id != 6 INNER JOIN ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".concept_name cn on cn.concept_id = pws.concept_id AND ")
                .append("!cn.voided AND cn.locale = 'en' AND cn.locale_preferred WHERE !pe.voided")
                .append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .toString();

        ResultSet resultSet = null;
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {

            resultSet = statement.executeQuery(countQuery);
            resultSet.next();
            int moreToGo = resultSet.getInt(1);

            toBeGenerated += moreToGo;
            statement.execute(insertStatement);
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertStatement, e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.getStatement().close();
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }
    }
	
	private void etlObsBasedRecords(Integer[] encounterTypes, Integer[] concepts, Integer[] valueCoded) throws SQLException {
	    StringBuilder countQuery = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".person pe ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".encounter e on pe.person_id = e.patient_id AND !e.voided AND e.encounter_type IN ").append(inClause(encounterTypes))
                .append(" LEFT JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".form f ON f.form_id = e.form_id ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ").append(inClause(concepts));

        StringBuilder sb = new StringBuilder("INSERT INTO ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".patient_state(patient_id, patient_uuid, source_id, source_type, state_id, state, state_date, state_uuid, source_database) ")
                .append("SELECT pe.person_id, pe.uuid, e.form_id, f.name, ")
                .append("o.value_coded, cn.name, o.obs_datetime, o.uuid, '").append(AppProperties.getInstance().getDatabaseName())
                .append("' AS source_database FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".person pe ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".encounter e on pe.person_id = e.patient_id AND !e.voided AND e.encounter_type IN ").append(inClause(encounterTypes))
                .append(" LEFT JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".form f ON f.form_id = e.form_id ")
                .append("INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ").append(inClause(concepts));

        if(valueCoded != null) {
            countQuery.append(" AND o.value_coded IN ").append(inClause(valueCoded));
            sb.append(" AND o.value_coded IN ").append(inClause(valueCoded));
        }

        countQuery.append(" INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".concept_name cn ON cn.concept_id = o.value_coded AND !cn.voided AND cn.locale = 'en' AND cn.locale_preferred ")
                .append("WHERE !pe.voided").append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)");

        sb.append(" INNER JOIN ").append(AppProperties.getInstance().getDatabaseName())
                .append(".concept_name cn ON cn.concept_id = o.value_coded AND !cn.voided AND cn.locale = 'en' AND cn.locale_preferred ")
                .append("WHERE !pe.voided").append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)");

        ResultSet resultSet = null;
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery(countQuery.toString());
            resultSet.next();
            int moreToGo = resultSet.getInt(1);

            toBeGenerated += moreToGo;
            statement.execute(sb.toString());
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), sb.toString(), e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.getStatement().close();
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }
    }
	
	private void etlPersonDeathState() throws SQLException {
	    String countQuery = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName())
                .append(".person pe where dead = 1").append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .toString();

        String insert = new StringBuilder("INSERT INTO ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".patient_state (patient_id, patient_uuid, source_id, source_type, state_id, state, state_date, source_database) ")
                .append("SELECT pe.person_id, pe.uuid, 1 as source_id,'Demographic' as source_type, 1366 as state_id, ")
                .append("'PATIENT HAS DIED' as state, death_date, '").append(AppProperties.getInstance().getDatabaseName())
                .append("' AS source_database FROM ").append(AppProperties.getInstance().getDatabaseName())
                .append(".person pe where dead = 1").append(" AND pe.person_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .toString();

        ResultSet resultSet = null;
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery(countQuery.toString());
            resultSet.next();
            int moreToGo = resultSet.getInt(1);

            toBeGenerated += moreToGo;
            statement.execute(insert);
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insert, e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.getStatement().close();
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }
    }
}
