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
public class CCUTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "ccu.sql";
	
	public static final Integer[] CCU_CONCEPT_IDS = new Integer[] { 2126, 2115, 1465, 5990, 2100, 23962, 1963, 174, 165435,
	        165444, 2105, 2103, 2094, 165436, 2117, 1185, 1185, 23966, 165345, 2149 };
	
	public static final Integer ENCOUNTER_TYPE_ID = 28;
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int SCREENING_DATE_POS = 2;
	
	protected final int FORM_ID_POS = 3;
	
	protected final int ENC_TYPE_POS = 4;
	
	protected final int PATIENT_UUID_POS = 5;
	
	protected final int ENC_CREATED_DATE_POS = 6;
	
	protected final int ENC_CHANGE_DATE_POS = 7;
	
	protected final int LOC_UUID_POS = 8;
	
	protected final int AGE_FIRSTSEX_POS = 9;
	
	protected final int MENSTRUAL_CYCLE_POS = 10;
	
	protected final int LAST_MENSTRDATE_POS = 11;
	
	protected final int MENOPAUSAL_POS = 12;
	
	protected final int VAG_BLEEDING_POS = 13;
	
	protected final int SCREENING_TYPE_POS = 14;
	
	protected final int STI_POS = 15;
	
	protected final int STI_LEUKORRHEA_POS = 16;
	
	protected final int STI_GEN_ULCER_POS = 17;
	
	protected final int SCREEN_HISTORY_POS = 18;
	
	protected final int LAST_SCRDATE_POS = 19;
	
	protected final int LAST_SCRRESULT_POS = 20;
	
	protected final int LAST_SCRRESULT_DATE_POS = 21;
	
	protected final int PELVIC_EXAM_POS = 22;
	
	protected final int SPECX_ECTROPIC_POS = 23;
	
	protected final int SPECX_ATROPHIC_VAG_POS = 24;
	
	protected final int SPECX_CERVICITIS_POS = 25;
	
	protected final int SPECX_BLEEDING_POS = 26;
	
	protected final int SPECX_NO_BLEEDING_POS = 27;
	
	protected final int SPECX_OTHER_POS = 28;
	
	protected final int VIA_RESULT_POS = 29;
	
	protected final int HPV_RESULT_POS = 30;
	
	protected final int VIA_SAMEDAY_TREAT_POS = 31;
	
	protected final int VIA_TREATTYPE_POS = 32;
	
	protected final int VIA_TREATTYPE_DATE_POS = 33;
	
	protected final int DLTRT_BROKEN_POS = 34;
	
	protected final int DLTRT_NOEQUIP_POS = 35;
	
	protected final int DLTRT_SOCIAL_POS = 36;
	
	protected final int DLTRT_REFHOSPITAL_POS = 37;
	
	protected final int DLTRT_OTHER_POS = 38;
	
	protected final int REFHOSP_POS = 39;
	
	protected final int REFHOSP_LEEP_POS = 40;
	
	protected final int REFHOSP_THERMOCOAG_POS = 41;
	
	protected final int REFHOSP_CRYOTHERAPY_POS = 42;
	
	protected final int REFHOSP_CONIZATION_POS = 43;
	
	protected final int REFHOSP_HYSTERECTOMY_POS = 44;
	
	protected final int REFHOSP_OTHER_POS = 45;
	
	protected final int REFHOSP_TREATDATE_POS = 46;
	
	protected final int SRC_DB_POS = 47;
	
	@Override
	public String getTable() {
		return "ccu";
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
		        .append(" WHERE e.concept_id IN ").append(inClause(CCU_CONCEPT_IDS)).toString();
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
		        .append(" WHERE e.concept_id IN ").append(inClause(CCU_CONCEPT_IDS)).append(" ORDER BY e.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".ccu (encounter_uuid, screening_date, form_id, encounter_type, patient_uuid, ")
		        .append("encounter_created_date, encounter_change_date, location_uuid, ")
		        .append("age_firstsex, menstrual_cycle, last_menstruation_date, ")
		        .append("menapausal, vaginal_bleeding, screening_type, sti, sti_leukorrhea, ")
		        .append("sti_genital_ulcer, screening_history, last_screening_date, last_screening_result, ")
		        .append("last_screening_result_date, pelvic_exam, speculum_exam_ectropic, ")
		        .append("speculum_exam_atrophic_vaginitis, speculum_exam_cervicitis, ")
		        .append("speculum_exam_bleeding, speculum_exam_no_bleeding, speculum_exam_other, ")
		        .append("via_result, hpv_result, via_same_day_treatment, via_treatment_type, ")
		        .append("via_treatment_type_date, delayed_treatment_broken_machine, ")
		        .append("delayed_treatment_no_equipment, delayed_treatment_social_reasons, ")
		        .append("delayed_treatment_reference_hospital, delayed_treatment_other, ")
		        .append("reference_hospital, ref_hospital_leep, ref_hospital_thermocoagulation, ")
		        .append("ref_hospital_cryotherapy, ref_hospital_conization, ref_hospital_hysterectomy, ")
		        .append("ref_hospital_other, ref_hospital_treatment_date, ").append("source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
		        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(AGE_FIRSTSEX_POS, MENSTRUAL_CYCLE_POS, LAST_MENSTRDATE_POS, MENOPAUSAL_POS,
				VAG_BLEEDING_POS, SCREENING_TYPE_POS, STI_POS, STI_LEUKORRHEA_POS, STI_GEN_ULCER_POS,
				SCREEN_HISTORY_POS, LAST_SCRDATE_POS, LAST_SCRRESULT_POS, LAST_SCRRESULT_DATE_POS,
				PELVIC_EXAM_POS,SPECX_ECTROPIC_POS,SPECX_ATROPHIC_VAG_POS, SPECX_CERVICITIS_POS,
				SPECX_BLEEDING_POS, SPECX_NO_BLEEDING_POS, SPECX_OTHER_POS, VIA_RESULT_POS, HPV_RESULT_POS,
				VIA_SAMEDAY_TREAT_POS, VIA_TREATTYPE_POS, VIA_TREATTYPE_DATE_POS, DLTRT_BROKEN_POS,
				DLTRT_NOEQUIP_POS, DLTRT_SOCIAL_POS, DLTRT_REFHOSPITAL_POS, DLTRT_OTHER_POS, REFHOSP_POS,
				REFHOSP_LEEP_POS, REFHOSP_THERMOCOAG_POS, REFHOSP_CRYOTHERAPY_POS, REFHOSP_CONIZATION_POS,
				REFHOSP_HYSTERECTOMY_POS, REFHOSP_OTHER_POS, REFHOSP_TREATDATE_POS));
        return positionsNotSet;
    }
	
	@Override
	protected void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException {
		insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
		insertStatement.setTimestamp(SCREENING_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
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
		if (resultConceptId == 2126) {
			insertStatement.setDouble(AGE_FIRSTSEX_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(AGE_FIRSTSEX_POS);
		} else if (resultConceptId == 2115) {
			insertStatement.setInt(MENSTRUAL_CYCLE_POS, valueCoded);
			positionsNotSet.remove(MENSTRUAL_CYCLE_POS);
		} else if (resultConceptId == 1465) {
			insertStatement.setTimestamp(LAST_MENSTRDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(LAST_MENSTRDATE_POS);
		} else if (resultConceptId == 5990) {
			insertStatement.setInt(MENOPAUSAL_POS, valueCoded);
			positionsNotSet.remove(MENOPAUSAL_POS);
		} else if (resultConceptId == 2100) {
			insertStatement.setInt(VAG_BLEEDING_POS, valueCoded);
			positionsNotSet.remove(VAG_BLEEDING_POS);
		} else if (resultConceptId == 23962) {
			insertStatement.setInt(SCREENING_TYPE_POS, valueCoded);
			positionsNotSet.remove(SCREENING_TYPE_POS);
		} else if (resultConceptId == 1963) {
			insertStatement.setInt(STI_POS, valueCoded);
			positionsNotSet.remove(STI_POS);
		} else if (resultConceptId == 174) {
			switch (valueCoded) {
				case 5993:
					insertStatement.setInt(STI_LEUKORRHEA_POS, valueCoded);
					positionsNotSet.remove(STI_LEUKORRHEA_POS);
					break;
				case 864:
					insertStatement.setInt(STI_GEN_ULCER_POS, valueCoded);
					positionsNotSet.remove(STI_GEN_ULCER_POS);
					break;
			}
		} else if (resultConceptId == 165435) {
			insertStatement.setInt(SCREEN_HISTORY_POS, valueCoded);
			insertStatement.setTimestamp(LAST_SCRDATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(SCREEN_HISTORY_POS);
			positionsNotSet.remove(LAST_SCRDATE_POS);
		} else if (resultConceptId == 165444) {
			insertStatement.setInt(LAST_SCRRESULT_POS, valueCoded);
			insertStatement.setTimestamp(LAST_SCRRESULT_DATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(LAST_SCRRESULT_POS);
			positionsNotSet.remove(LAST_SCRRESULT_DATE_POS);
		} else if (resultConceptId == 2105) {
			insertStatement.setInt(PELVIC_EXAM_POS, valueCoded);
			positionsNotSet.remove(PELVIC_EXAM_POS);
		} else if (resultConceptId == 2103) {
			switch (valueCoded) {
				case 2101:
					insertStatement.setInt(SPECX_ECTROPIC_POS, valueCoded);
					positionsNotSet.remove(SPECX_ECTROPIC_POS);
					break;
				case 2102:
					insertStatement.setInt(SPECX_ATROPHIC_VAG_POS, valueCoded);
					positionsNotSet.remove(SPECX_ATROPHIC_VAG_POS);
					break;
				case 149:
					insertStatement.setInt(SPECX_CERVICITIS_POS, valueCoded);
					positionsNotSet.remove(SPECX_CERVICITIS_POS);
					break;
				case 2100:
					insertStatement.setInt(SPECX_BLEEDING_POS, valueCoded);
					positionsNotSet.remove(SPECX_BLEEDING_POS);
					break;
				case 2099:
					insertStatement.setInt(SPECX_NO_BLEEDING_POS, valueCoded);
					positionsNotSet.remove(SPECX_NO_BLEEDING_POS);
					break;
				case 1406:
					insertStatement.setInt(SPECX_OTHER_POS, valueCoded);
					positionsNotSet.remove(SPECX_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 2094) {
			insertStatement.setInt(VIA_RESULT_POS, valueCoded);
			positionsNotSet.remove(VIA_RESULT_POS);
		} else if (resultConceptId == 165436) {
			insertStatement.setInt(HPV_RESULT_POS, valueCoded);
			positionsNotSet.remove(HPV_RESULT_POS);
		} else if (resultConceptId == 2117) {
			insertStatement.setInt(VIA_SAMEDAY_TREAT_POS, valueCoded);
			positionsNotSet.remove(VIA_SAMEDAY_TREAT_POS);
		} else if (resultConceptId == 1185) {
			insertStatement.setInt(VIA_TREATTYPE_POS, valueCoded);
			insertStatement.setTimestamp(VIA_TREATTYPE_DATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(VIA_TREATTYPE_POS);
			positionsNotSet.remove(VIA_TREATTYPE_DATE_POS);
		} else if (resultConceptId == 23966) {
			switch (valueCoded) {
				case 23964:
					insertStatement.setInt(DLTRT_BROKEN_POS, valueCoded);
					positionsNotSet.remove(DLTRT_BROKEN_POS);
					break;
				case 23965:
					insertStatement.setInt(DLTRT_NOEQUIP_POS, valueCoded);
					positionsNotSet.remove(DLTRT_NOEQUIP_POS);
					break;
				case 20377:
					insertStatement.setInt(DLTRT_SOCIAL_POS, valueCoded);
					positionsNotSet.remove(DLTRT_SOCIAL_POS);
					break;
				case 165345:
					insertStatement.setInt(DLTRT_REFHOSPITAL_POS, valueCoded);
					positionsNotSet.remove(DLTRT_REFHOSPITAL_POS);
					break;
				case 5622:
					insertStatement.setInt(DLTRT_OTHER_POS, valueCoded);
					positionsNotSet.remove(DLTRT_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 165345) {
			insertStatement.setInt(REFHOSP_POS, valueCoded);
			positionsNotSet.remove(REFHOSP_POS);
		} else if (resultConceptId == 2149) {
			switch (valueCoded) {
				case 23970:
					insertStatement.setInt(REFHOSP_LEEP_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_LEEP_POS);
					break;
				case 23972:
					insertStatement.setInt(REFHOSP_THERMOCOAG_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_THERMOCOAG_POS);
					break;
				case 23974:
					insertStatement.setInt(REFHOSP_CRYOTHERAPY_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_CRYOTHERAPY_POS);
					break;
				case 23973:
					insertStatement.setInt(REFHOSP_CONIZATION_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_CONIZATION_POS);
					break;
				case 23971:
					insertStatement.setInt(REFHOSP_HYSTERECTOMY_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_HYSTERECTOMY_POS);
					break;
				case 5622:
					insertStatement.setInt(REFHOSP_OTHER_POS, valueCoded);
					positionsNotSet.remove(REFHOSP_OTHER_POS);
					break;
			}
			insertStatement.setTimestamp(REFHOSP_TREATDATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(REFHOSP_TREATDATE_POS);
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case AGE_FIRSTSEX_POS:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case LAST_MENSTRDATE_POS:
					case LAST_SCRDATE_POS:
					case LAST_SCRRESULT_DATE_POS:
					case VIA_TREATTYPE_DATE_POS:
					case REFHOSP_TREATDATE_POS:
						insertStatement.setNull(pos, Types.TIMESTAMP);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
