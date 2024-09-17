package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class HomeVisitTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "home_visit.sql";
	
	public static final Integer[] HOME_VISIT_CONCEPT_IDS = new Integer[] { 2172, 23993, 1981, 24008, 6254, 24009, 6255,
	        24010, 23842, 23947, 24027, 24028, 2180 };
	
	public static final Integer ENCOUNTER_TYPE_ID = 21;
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int HOME_VISIT_DATE_POS = 2;
	
	protected final int FORM_ID_POS = 3;
	
	protected final int ENC_TYPE_POS = 4;
	
	protected final int PATIENT_UUID_POS = 5;
	
	protected final int ENC_CREATED_DATE_POS = 6;
	
	protected final int ENC_CHANGE_DATE_POS = 7;
	
	protected final int LOC_UUID_POS = 8;
	
	protected final int RV_HIVEXPCHILD_POS = 9;
	
	protected final int RV_POSPCR_POS = 10;
	
	protected final int RV_TBACTIVE_POS = 11;
	
	protected final int RV_LTFU_POS = 12;
	
	protected final int RV_HIGHVL_POS = 13;
	
	protected final int RV_THERAFAILURE_POS = 14;
	
	protected final int RV_DEFAULTER_POS = 15;
	
	protected final int RV_IIT_POS = 16;
	
	protected final int RV_PREVFUP_POS = 17;
	
	protected final int RV_OTHER_POS = 18;
	
	protected final int PHCALL_REINTEGRATION_POS = 19;
	
	protected final int VISIT_TYPE_POS = 20;
	
	protected final int FIRST_VISITFOUND_POS = 21;
	
	protected final int SEC_VISITDATE_POS = 22;
	
	protected final int SEC_VISITFOUND_POS = 23;
	
	protected final int THI_VISITDATE_POS = 24;
	
	protected final int THI_VISITFOUND_POS = 25;
	
	protected final int VISIT_NUMBER_POS = 26;
	
	protected final int REF_HEALTHFACILITY_POS = 27;
	
	protected final int PAT_RETURNED_POS = 28;
	
	protected final int PAT_RETURNEDDATE_POS = 29;
	
	protected final int CARD_RETURNEDDATE_POS = 30;
	
	protected final int SRC_DB_POS = 31;
	
	@Override
	public String getTable() {
		return "home_visit";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(DISTINCT e.encounter_id) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(HOME_VISIT_CONCEPT_IDS)).toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.*, p.patient_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(HOME_VISIT_CONCEPT_IDS)).append(" ORDER BY e.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".home_visit (encounter_uuid, home_visit_date, form_id, encounter_type, patient_uuid, ")
		        .append("encounter_created_date, encounter_change_date, location_uuid, ")
		        .append("reason_visit_hivexposed_child, reason_visit_pos_pcr, reason_visit_tb_active, ")
		        .append("reason_visit_ltfu, reason_visit_high_vl, reason_visit_therapeutic_failure, ")
		        .append("reason_visit_defaulter, reason_visit_iit, reason_visit_prev_fup, reason_visit_other, ")
		        .append("phone_call_reintegration, visit_type, first_visit_found, ")
		        .append("second_visit_date, second_visit_found, third_visit_date, ")
		        .append("third_visit_found, visit_number, referred_health_facility, ")
		        .append("patient_returned, patient_returned_date, card_returned_date, ").append("source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
		        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(RV_HIVEXPCHILD_POS, RV_POSPCR_POS, RV_TBACTIVE_POS, RV_LTFU_POS,
				RV_HIGHVL_POS, RV_THERAFAILURE_POS, RV_DEFAULTER_POS, RV_IIT_POS, RV_PREVFUP_POS,
				RV_OTHER_POS, PHCALL_REINTEGRATION_POS, VISIT_TYPE_POS, FIRST_VISITFOUND_POS,
				SEC_VISITDATE_POS, SEC_VISITFOUND_POS, THI_VISITDATE_POS, THI_VISITFOUND_POS,
				VISIT_NUMBER_POS, REF_HEALTHFACILITY_POS, PAT_RETURNED_POS, PAT_RETURNEDDATE_POS,
				CARD_RETURNEDDATE_POS
		));
        return positionsNotSet;
    }
	
	@Override
	protected void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException {
		insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
		insertStatement.setTimestamp(HOME_VISIT_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
		insertStatement.setInt(ENC_TYPE_POS, scrollableResultSet.getInt("encounter_type"));
		insertStatement.setTimestamp(ENC_CREATED_DATE_POS, scrollableResultSet.getTimestamp("e_date_created"));
		insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, scrollableResultSet.getTimestamp("e_date_changed"));
		insertStatement.setInt(FORM_ID_POS, scrollableResultSet.getInt("form_id"));
		insertStatement.setString(PATIENT_UUID_POS, scrollableResultSet.getString("patient_uuid"));
		insertStatement.setString(LOC_UUID_POS,
		    Mozart2Properties.getInstance().getLocationUuidById(scrollableResultSet.getInt("location_id")));
		insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
		int resultConceptId = scrollableResultSet.getInt("concept_id");
		int valueCoded = scrollableResultSet.getInt("value_coded");
		if (resultConceptId == 2172) {
			switch (valueCoded) {
				case 2170:
					insertStatement.setInt(RV_HIVEXPCHILD_POS, valueCoded);
					positionsNotSet.remove(RV_HIVEXPCHILD_POS);
					break;
				case 165352:
					insertStatement.setInt(RV_POSPCR_POS, valueCoded);
					positionsNotSet.remove(RV_POSPCR_POS);
					break;
				case 23761:
					insertStatement.setInt(RV_TBACTIVE_POS, valueCoded);
					positionsNotSet.remove(RV_TBACTIVE_POS);
					break;
				case 5240:
					insertStatement.setInt(RV_LTFU_POS, valueCoded);
					positionsNotSet.remove(RV_LTFU_POS);
					break;
				case 23912:
					insertStatement.setInt(RV_HIGHVL_POS, valueCoded);
					positionsNotSet.remove(RV_HIGHVL_POS);
					break;
				case 1790:
					insertStatement.setInt(RV_THERAFAILURE_POS, valueCoded);
					positionsNotSet.remove(RV_THERAFAILURE_POS);
					break;
				case 165353:
					insertStatement.setInt(RV_DEFAULTER_POS, valueCoded);
					positionsNotSet.remove(RV_DEFAULTER_POS);
					break;
				case 1707:
					insertStatement.setInt(RV_IIT_POS, valueCoded);
					positionsNotSet.remove(RV_IIT_POS);
					break;
				case 6253:
					insertStatement.setInt(RV_PREVFUP_POS, valueCoded);
					positionsNotSet.remove(RV_PREVFUP_POS);
					break;
				case 2171:
					insertStatement.setInt(RV_OTHER_POS, valueCoded);
					positionsNotSet.remove(RV_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 23993) {
			insertStatement.setInt(PHCALL_REINTEGRATION_POS, valueCoded);
			positionsNotSet.remove(PHCALL_REINTEGRATION_POS);
		} else if (resultConceptId == 1981) {
			insertStatement.setInt(VISIT_TYPE_POS, valueCoded);
			positionsNotSet.remove(VISIT_TYPE_POS);
		} else if (resultConceptId == 24008) {
			insertStatement.setInt(FIRST_VISITFOUND_POS, valueCoded);
			positionsNotSet.remove(FIRST_VISITFOUND_POS);
		} else if (resultConceptId == 6254) {
			insertStatement.setTimestamp(SEC_VISITDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(SEC_VISITDATE_POS);
		} else if (resultConceptId == 24009) {
			insertStatement.setInt(SEC_VISITFOUND_POS, valueCoded);
			positionsNotSet.remove(SEC_VISITFOUND_POS);
		} else if (resultConceptId == 6255) {
			insertStatement.setTimestamp(THI_VISITDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(THI_VISITDATE_POS);
		} else if (resultConceptId == 24010) {
			insertStatement.setInt(THI_VISITFOUND_POS, valueCoded);
			positionsNotSet.remove(THI_VISITFOUND_POS);
		} else if (resultConceptId == 23842) {
			insertStatement.setInt(VISIT_NUMBER_POS, valueCoded);
			positionsNotSet.remove(VISIT_NUMBER_POS);
		} else if (resultConceptId == 23947) {
			insertStatement.setInt(REF_HEALTHFACILITY_POS, valueCoded);
			positionsNotSet.remove(REF_HEALTHFACILITY_POS);
		} else if (resultConceptId == 24027) {
			insertStatement.setInt(PAT_RETURNED_POS, valueCoded);
			positionsNotSet.remove(PAT_RETURNED_POS);
		} else if (resultConceptId == 24028) {
			insertStatement.setTimestamp(PAT_RETURNEDDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(PAT_RETURNEDDATE_POS);
		} else if (resultConceptId == 2180) {
			insertStatement.setTimestamp(CARD_RETURNEDDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(CARD_RETURNEDDATE_POS);
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case SEC_VISITDATE_POS:
					case THI_VISITDATE_POS:
					case PAT_RETURNEDDATE_POS:
					case CARD_RETURNEDDATE_POS:
						insertStatement.setNull(pos, Types.TIMESTAMP);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
