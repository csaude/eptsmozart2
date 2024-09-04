package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/11/22.
 */
public class ClinicalConsultationTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClinicalConsultationTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "clinical_consultation.sql";
	
	public static final Integer[] CONCEPT_IDS = new Integer[] { 1410, 6310, 5085, 5086, 5356, 5089, 5090, 1343, 23738 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int SCHEDULED_DATE_POS = 3;
	
	protected final int BP_DIASTOLIC_POS = 4;
	
	protected final int BP_SYSTOLIC_POS = 5;
	
	protected final int WHO_STAGING_POS = 6;
	
	protected final int WEIGHT_POS = 7;
	
	protected final int HEIGHT_POS = 8;
	
	protected final int ARM_CIRCUM_POS = 9;
	
	protected final int NUTRI_GRADE_POS = 10;
	
	protected final int ENC_TYPE_POS = 11;
	
	protected final int ENC_CREATED_DATE_POS = 12;
	
	protected final int ENC_CHANGE_DATE_POS = 13;
	
	protected final int FORM_ID_POS = 14;
	
	protected final int PATIENT_UUID_POS = 15;
	
	protected final int LOC_UUID_POS = 16;
	
	protected final int SRC_DB_POS = 17;
	
	protected final int CONSULT_DATE_POS = 18;
	
	@Override
	public String getTable() {
		return "clinical_consultation";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		StringBuilder sb = new StringBuilder("SELECT COUNT(DISTINCT e.encounter_id) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id LEFT JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" WHERE !e.voided AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("'");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT o.*, e.uuid as encounter_uuid, e.encounter_datetime, ")
		        .append("e.encounter_id as e_encounter_id, e.date_created as e_date_created, ")
		        .append("e.date_changed as e_date_changed, e.encounter_type, e.form_id, ")
		        .append("p.patient_uuid, l.uuid as loc_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter e JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id LEFT JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o on e.encounter_id = o.encounter_id AND !o.voided AND o.concept_id IN ")
		        .append(inClause(CONCEPT_IDS)).append(" WHERE !e.voided AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("' ORDER BY e_encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT IGNORE INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".clinical_consultation (encounter_uuid, encounter_date, scheduled_date, ")
		        .append("bp_diastolic, bp_systolic, who_staging, weight, height, arm_circumference, nutritional_grade, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, form_id, ")
		        .append("patient_uuid, location_uuid, source_database, consultation_date) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
	protected Set<Integer> getAllPositionsNotSet() {
		Set<Integer> positionsNotSet = new HashSet<>();
		positionsNotSet.addAll(Arrays.asList(SCHEDULED_DATE_POS, BP_DIASTOLIC_POS, BP_SYSTOLIC_POS, WHO_STAGING_POS,
											 WEIGHT_POS, HEIGHT_POS, ARM_CIRCUM_POS,
											 NUTRI_GRADE_POS));
		return positionsNotSet;
	}
	
	@Override
	protected void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException {
		Timestamp encounterDatetime = scrollableResultSet.getTimestamp("encounter_datetime");
		insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
		insertStatement.setTimestamp(ENCOUNTER_DATE_POS, encounterDatetime);
		insertStatement.setTimestamp(CONSULT_DATE_POS, encounterDatetime);
		insertStatement.setInt(ENC_TYPE_POS, scrollableResultSet.getInt("encounter_type"));
		insertStatement.setTimestamp(ENC_CREATED_DATE_POS, scrollableResultSet.getTimestamp("e_date_created"));
		insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, scrollableResultSet.getTimestamp("e_date_changed"));
		insertStatement.setInt(FORM_ID_POS, scrollableResultSet.getInt("form_id"));
		insertStatement.setString(PATIENT_UUID_POS, scrollableResultSet.getString("patient_uuid"));
		insertStatement.setString(LOC_UUID_POS, scrollableResultSet.getString("loc_uuid"));
		insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
		
		Integer conceptId = scrollableResultSet.getInt("concept_id");
		if (conceptId != null) {
			if (conceptId == 1410 || conceptId == 6310) {
				insertStatement.setDate(SCHEDULED_DATE_POS, scrollableResultSet.getDate("value_datetime"));
				positionsNotSet.remove(SCHEDULED_DATE_POS);
			} else if (conceptId == 5086) {
				insertStatement.setDouble(BP_DIASTOLIC_POS, scrollableResultSet.getDouble("value_numeric"));
				positionsNotSet.remove(BP_DIASTOLIC_POS);
			} else if (conceptId == 5085) {
				insertStatement.setDouble(BP_SYSTOLIC_POS, scrollableResultSet.getDouble("value_numeric"));
				positionsNotSet.remove(BP_SYSTOLIC_POS);
			} else if (conceptId == 5356) {
				insertStatement.setInt(WHO_STAGING_POS, scrollableResultSet.getInt("value_coded"));
				positionsNotSet.remove(WHO_STAGING_POS);
			} else if (conceptId == 5089) {
				insertStatement.setDouble(WEIGHT_POS, scrollableResultSet.getDouble("value_numeric"));
				positionsNotSet.remove(WEIGHT_POS);
			} else if (conceptId == 5090) {
				insertStatement.setDouble(HEIGHT_POS, scrollableResultSet.getDouble("value_numeric"));
				positionsNotSet.remove(HEIGHT_POS);
			} else if (conceptId == 1343) {
				insertStatement.setDouble(ARM_CIRCUM_POS, scrollableResultSet.getDouble("value_numeric"));
				positionsNotSet.remove(ARM_CIRCUM_POS);
			} else if (conceptId == 23738) {
				insertStatement.setInt(NUTRI_GRADE_POS, scrollableResultSet.getInt("value_coded"));
				positionsNotSet.remove(NUTRI_GRADE_POS);
			}
		}
		
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case SCHEDULED_DATE_POS:
						insertStatement.setNull(pos, Types.DATE);
						break;
					case WHO_STAGING_POS:
					case NUTRI_GRADE_POS:
						insertStatement.setNull(pos, Types.INTEGER);
						break;
					default:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
				}
			}
		}
	}
}
