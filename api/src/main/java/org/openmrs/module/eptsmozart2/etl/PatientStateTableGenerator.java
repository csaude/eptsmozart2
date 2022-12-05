package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/8/22.
 */
public class PatientStateTableGenerator extends InsertFromSelectGenerator {
	
	public static final String CREATE_TABLE_FILE_NAME = "patient_state.sql";
	
	@Override
	public String getTable() {
		return "patient_state";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected void etl() throws SQLException {
		etlPersonDeathState();
		etlProgramBasedRecords();
		
		// FICHA RESUMO & FICHA CLINICA
		etlObsBasedRecords(new Integer[] { 6, 9, 53 }, new Integer[] { 6273, 6272 }, null);
		
		// 121 - TARV: VISITA DOMICILIARIA
		Integer[] valueCoded = new Integer[] { 1366, 1706, 23863 };
		etlObsBasedRecords(new Integer[] { 21 }, new Integer[] { 2031, 23944, 23945, 2016 }, valueCoded);
	}
	
	private void etlProgramBasedRecords() throws SQLException {
		String insertStatement = new StringBuilder("INSERT INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient_state (patient_uuid, program_id, program_enrolment_date, program_completed_date, ")
		        .append("location_uuid, enrolment_uuid, source_id, state_id, state_date, state_uuid) ")
		        .append("SELECT p.patient_uuid, pg.program_id, pg.date_enrolled, pg.date_completed, ")
		        .append("l.uuid,  pg.uuid, pg.program_id, pws.concept_id, ")
		        .append("ps.start_date, ps.uuid FROM ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p INNER JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".patient_program pg ON p.patient_id = pg.patient_id AND !pg.voided JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l ON l.location_id=pg.location_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(
		            ".patient_state ps on ps.patient_program_id=pg.patient_program_id AND !ps.voided AND ps.start_date <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("' INNER JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(
		            ".program_workflow_state pws on pws.program_workflow_state_id=ps.state AND pws.program_workflow_state_id != 6")
		        .toString();
		
		runSql(insertStatement, null);
	}
	
	private void etlObsBasedRecords(Integer[] encounterTypes, Integer[] concepts, Integer[] valueCoded) throws SQLException {
		StringBuilder sb = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient_state(patient_uuid, source_id, state_id, state_date, state_uuid) ")
		        .append("SELECT p.patient_uuid, e.form_id, o.value_coded, o.obs_datetime, o.uuid FROM ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p INNER JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on p.patient_id = e.patient_id AND !e.voided AND e.encounter_type IN ")
		        .append(inClause(encounterTypes)).append(" INNER JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ")
		        .append(inClause(concepts)).append(" AND o.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("'");
		
		if (valueCoded != null) {
			sb.append(" AND o.value_coded IN ").append(inClause(valueCoded));
		}
		runSql(sb.toString(), null);
	}
	
	private void etlPersonDeathState() throws SQLException {
		String insert = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient_state (patient_uuid, source_id, state_id, state_date) ")
		        .append("SELECT pe.uuid, 0 as source_id, 1366 as state_id, death_date FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".person pe WHERE pe.dead = 1 AND pe.death_date <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("' AND pe.person_id IN (SELECT patient_id FROM ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient)").toString();
		
		runSql(insert, null);
	}
}
