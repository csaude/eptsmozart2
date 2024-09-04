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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class PrepTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "prep.sql";
	
	public static final Integer[] PREP_CONCEPT_IDS = new Integer[] { 1040, 165194, 165289, 790, 1322, 6258, 299, 1080,
	        165210, 165213, 165217, 165174, 23703, 165196, 165294, 1713, 165221, 6317, 23771, 165223, 1982, 6332, 165225,
	        165228 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 80, 81 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int HIV_TEST_RES_POS = 3;
	
	protected final int HIV_TEST_RES_DATE_POS = 4;
	
	protected final int ACCEPT_PREP_POS = 5;
	
	protected final int CREATINE_RES_POS = 6;
	
	protected final int CREATINE_DATE_POS = 7;
	
	protected final int HEPB_RES_POS = 8;
	
	protected final int STI_SCREEN_POS = 9;
	
	protected final int SIFILIS_RES_POS = 10;
	
	protected final int STI_LEU_POS = 11;
	
	protected final int STI_URE_POS = 12;
	
	protected final int STI_GEN_POS = 13;
	
	protected final int STI_PEL_POS = 14;
	
	protected final int STI_INF_POS = 15;
	
	protected final int STI_GRA_POS = 16;
	
	protected final int STI_OUT_POS = 17;
	
	protected final int STI_START_DATE_POS = 18;
	
	protected final int REG_PROPHY_PREP_POS = 19;
	
	protected final int NO_UNITS_POS = 20;
	
	protected final int DSD_PREP_POS = 21;
	
	protected final int KEY_MSM_POS = 22;
	
	protected final int KEY_DRUG_POS = 23;
	
	protected final int KEY_TG_POS = 24;
	
	protected final int KEY_SW_POS = 25;
	
	protected final int KEY_PRISON_POS = 26;
	
	protected final int KEY_AYR_POS = 27;
	
	protected final int KEY_POLICE_POS = 28;
	
	protected final int KEY_MINER_POS = 29;
	
	protected final int KEY_DRIVER_POS = 30;
	
	protected final int KEY_SERODISC_POS = 31;
	
	protected final int KEY_PREG_POS = 32;
	
	protected final int KEY_LACT_POS = 33;
	
	protected final int KEY_OTHER_POS = 34;
	
	protected final int RENAL_FAILURE_POS = 35;
	
	protected final int PILLS_LEFTOVER_POS = 36;
	
	protected final int ADH_COUNSELING_POS = 37;
	
	protected final int CONDOMS_POS = 38;
	
	protected final int LUBS_POS = 39;
	
	protected final int PREGNANT_POS = 40;
	
	protected final int LACTANT_POS = 41;
	
	protected final int PREP_HIVPOS_POS = 42;
	
	protected final int PREP_SIDEEFFECT_POS = 43;
	
	protected final int PREP_NORISK_POS = 44;
	
	protected final int PREP_USERREQ_POS = 45;
	
	protected final int PREP_OTHER_POS = 46;
	
	protected final int NEXT_CONSULT_POS = 47;
	
	protected final int ENC_TYPE_POS = 48;
	
	protected final int ENC_CREATED_DATE_POS = 49;
	
	protected final int ENC_CHANGE_DATE_POS = 50;
	
	protected final int FORM_ID_POS = 51;
	
	protected final int PATIENT_UUID_POS = 52;
	
	protected final int LOC_UUID_POS = 53;
	
	protected final int SRC_DB_POS = 54;
	
	@Override
	public String getTable() {
		return "prep";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(DISTINCT e.encounter_id) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" WHERE !o.voided AND o.concept_id IN ").append(inClause(PREP_CONCEPT_IDS)).toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.encounter_datetime, e.uuid as encounter_uuid, ")
		        .append("e.encounter_id as e_encounter_id, e.encounter_type, ")
		        .append("e.date_created as e_date_created, e.date_changed as e_date_changed, ")
		        .append("e.form_id, p.patient_uuid, l.uuid as loc_uuid, o.* FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(PREP_CONCEPT_IDS)).append(" ORDER BY o.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".prep (encounter_uuid, encounter_date, hiv_test_result, hiv_test_result_date, ")
		        .append("accept_prep_start, creatine_result, creatine_date, hepb_result, ")
		        .append("sti_screen, sifilis_result, sti_diagnosis_leu, sti_diagnosis_ure, ")
		        .append("sti_diagnosis_gen, sti_diagnosis_pel, sti_diagnosis_inf, ")
		        .append("sti_diagnosis_gra, sti_diagnosis_out, sti_start_date, ")
		        .append("regimen_prophylaxis_prep, no_of_units, dsd_prep, key_vulnerable_msm, ")
		        .append("key_vulnerable_drug, key_vulnerable_tg, key_vulnerable_sw, key_vulnerable_prison, ")
		        .append("key_vulnerable_ayr, key_vulnerable_police, key_vulnerable_miner, ")
		        .append("key_vulnerable_driver, key_vulnerable_serodiscordant, ")
		        .append("key_vulnerable_pregnant, key_vulnerable_lactant, key_vulnerable_other, ")
		        .append("renal_failure_signs, pills_leftover, adherence_counseling, condoms_offer, ")
		        .append("lubricants_offer, pregnant, lactant, prep_interrupted_hivpos, ")
		        .append("prep_interrupted_sideeffect, prep_interrupted_norisk, ")
		        .append("prep_interrupted_userrequest, prep_interrupted_other, next_consult_prep, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, ")
		        .append("form_id, patient_uuid, location_uuid, source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
		        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
		        .append("?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(HIV_TEST_RES_POS, HIV_TEST_RES_DATE_POS, ACCEPT_PREP_POS, CREATINE_RES_POS,
											 CREATINE_DATE_POS, HEPB_RES_POS, STI_SCREEN_POS, SIFILIS_RES_POS, STI_LEU_POS,
											 STI_URE_POS, STI_GEN_POS, STI_PEL_POS, STI_INF_POS, STI_GRA_POS, STI_OUT_POS,
											 STI_START_DATE_POS, REG_PROPHY_PREP_POS, NO_UNITS_POS, DSD_PREP_POS, KEY_MSM_POS,
											 KEY_DRUG_POS, KEY_TG_POS, KEY_SW_POS, KEY_PRISON_POS, KEY_AYR_POS, KEY_POLICE_POS,
											 KEY_MINER_POS, KEY_DRIVER_POS, KEY_SERODISC_POS, KEY_PREG_POS, KEY_LACT_POS,
											 KEY_OTHER_POS, RENAL_FAILURE_POS, PILLS_LEFTOVER_POS, ADH_COUNSELING_POS,
											 CONDOMS_POS, LUBS_POS, PREGNANT_POS, LACTANT_POS, PREP_HIVPOS_POS,
											 PREP_SIDEEFFECT_POS, PREP_NORISK_POS, PREP_USERREQ_POS, PREP_OTHER_POS,
											 NEXT_CONSULT_POS
		));
        return positionsNotSet;
    }
	
	@Override
	protected void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException {
		insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
		insertStatement.setTimestamp(ENCOUNTER_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
		insertStatement.setInt(ENC_TYPE_POS, scrollableResultSet.getInt("encounter_type"));
		insertStatement.setTimestamp(ENC_CREATED_DATE_POS, scrollableResultSet.getTimestamp("e_date_created"));
		insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, scrollableResultSet.getTimestamp("e_date_changed"));
		insertStatement.setInt(FORM_ID_POS, scrollableResultSet.getInt("form_id"));
		insertStatement.setString(PATIENT_UUID_POS, scrollableResultSet.getString("patient_uuid"));
		insertStatement.setString(LOC_UUID_POS, scrollableResultSet.getString("loc_uuid"));
		insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
		
		int resultConceptId = scrollableResultSet.getInt("concept_id");
		int valueCoded = scrollableResultSet.getInt("value_coded");
		int encounterType = scrollableResultSet.getInt("encounter_type");
		
		if (resultConceptId == 1040) {
			insertStatement.setInt(HIV_TEST_RES_POS, valueCoded);
			insertStatement.setTimestamp(HIV_TEST_RES_DATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(HIV_TEST_RES_POS);
			positionsNotSet.remove((HIV_TEST_RES_DATE_POS));
		} else if (resultConceptId == 165194) {
			insertStatement.setInt(HIV_TEST_RES_POS, 664);
			insertStatement.setTimestamp(HIV_TEST_RES_DATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(HIV_TEST_RES_POS);
			positionsNotSet.remove((HIV_TEST_RES_DATE_POS));
		} else if (resultConceptId == 165289 && encounterType == 80) {
			insertStatement.setInt(ACCEPT_PREP_POS, valueCoded);
			positionsNotSet.remove(ACCEPT_PREP_POS);
		} else if (resultConceptId == 165308 || resultConceptId == 23987) {
			insertStatement.setInt(ACCEPT_PREP_POS, valueCoded);
			positionsNotSet.remove(ACCEPT_PREP_POS);
		} else if (resultConceptId == 790) {
			insertStatement.setDouble(CREATINE_RES_POS, scrollableResultSet.getDouble("value_numeric"));
			insertStatement.setTimestamp(CREATINE_DATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(CREATINE_RES_POS);
			positionsNotSet.remove(CREATINE_DATE_POS);
		} else if (resultConceptId == 1322 && encounterType == 80) {
			insertStatement.setInt(HEPB_RES_POS, valueCoded);
			positionsNotSet.remove(HEPB_RES_POS);
		} else if (resultConceptId == 6258) {
			insertStatement.setInt(STI_SCREEN_POS, valueCoded);
			positionsNotSet.remove(STI_SCREEN_POS);
		} else if (resultConceptId == 299 && encounterType == 80) {
			insertStatement.setInt(SIFILIS_RES_POS, valueCoded);
			positionsNotSet.remove(SIFILIS_RES_POS);
		} else if (resultConceptId == 1080 && encounterType == 80) {
			switch (valueCoded) {
				case 5993:
					insertStatement.setInt(STI_LEU_POS, valueCoded);
					positionsNotSet.remove(STI_LEU_POS);
					break;
				case 5995:
					insertStatement.setInt(STI_URE_POS, valueCoded);
					positionsNotSet.remove(STI_URE_POS);
					break;
				case 864:
					insertStatement.setInt(STI_GEN_POS, valueCoded);
					positionsNotSet.remove(STI_GEN_POS);
					break;
				case 902:
					insertStatement.setInt(STI_PEL_POS, valueCoded);
					positionsNotSet.remove(STI_PEL_POS);
					break;
				case 12611:
					insertStatement.setInt(STI_INF_POS, valueCoded);
					positionsNotSet.remove(STI_INF_POS);
					break;
				case 6747:
					insertStatement.setInt(STI_GRA_POS, valueCoded);
					positionsNotSet.remove(STI_GRA_POS);
					break;
				case 165208:
					insertStatement.setInt(STI_OUT_POS, valueCoded);
					positionsNotSet.remove(STI_OUT_POS);
					break;
			}
		} else if (resultConceptId == 165210 && encounterType == 80) {
			insertStatement.setTimestamp(STI_START_DATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(STI_START_DATE_POS);
		} else if (resultConceptId == 165213) {
			insertStatement.setInt(REG_PROPHY_PREP_POS, valueCoded);
			positionsNotSet.remove(REG_PROPHY_PREP_POS);
			// MOZ2-205
			if (encounterType == 80 && valueCoded == 165214 || valueCoded == 165215) {
				try {
					double noOfUnits = Double.parseDouble(scrollableResultSet.getString("comments"));
					insertStatement.setDouble(NO_UNITS_POS, noOfUnits);
					positionsNotSet.remove(NO_UNITS_POS);
				}
				catch (NumberFormatException|NullPointerException e) {
					// Ignore
				}
			}
		} else if (resultConceptId == 165217) {
			insertStatement.setDouble(NO_UNITS_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(NO_UNITS_POS);
		} else if (resultConceptId == 165174) {
			insertStatement.setInt(DSD_PREP_POS, valueCoded);
			positionsNotSet.remove(DSD_PREP_POS);
		} else if (resultConceptId == 165196 || resultConceptId == 23703) {
			switch (valueCoded) {
				case 1377:
					insertStatement.setInt(KEY_MSM_POS, valueCoded);
					positionsNotSet.remove(KEY_MSM_POS);
					break;
				case 20454:
					insertStatement.setInt(KEY_DRUG_POS, valueCoded);
					positionsNotSet.remove(KEY_DRUG_POS);
					break;
				case 165205:
					insertStatement.setInt(KEY_TG_POS, valueCoded);
					positionsNotSet.remove(KEY_TG_POS);
					break;
				case 1901:
					insertStatement.setInt(KEY_SW_POS, valueCoded);
					positionsNotSet.remove(KEY_SW_POS);
					break;
				case 20426:
					insertStatement.setInt(KEY_PRISON_POS, valueCoded);
					positionsNotSet.remove(KEY_PRISON_POS);
					break;
				case 165287:
					insertStatement.setInt(KEY_AYR_POS, valueCoded);
					positionsNotSet.remove(KEY_AYR_POS);
					break;
				case 1902:
					insertStatement.setInt(KEY_POLICE_POS, valueCoded);
					positionsNotSet.remove(KEY_POLICE_POS);
					break;
				case 1908:
					insertStatement.setInt(KEY_MINER_POS, valueCoded);
					positionsNotSet.remove(KEY_MINER_POS);
					break;
				case 1903:
					insertStatement.setInt(KEY_DRIVER_POS, valueCoded);
					positionsNotSet.remove(KEY_DRIVER_POS);
					break;
				case 1995:
					insertStatement.setInt(KEY_SERODISC_POS, valueCoded);
					positionsNotSet.remove(KEY_SERODISC_POS);
					break;
				case 1982:
					insertStatement.setInt(KEY_PREG_POS, valueCoded);
					positionsNotSet.remove(KEY_PREG_POS);
					break;
				case 6332:
					insertStatement.setInt(KEY_LACT_POS, valueCoded);
					positionsNotSet.remove(KEY_LACT_POS);
					break;
				case 5622:
					insertStatement.setInt(KEY_OTHER_POS, valueCoded);
					positionsNotSet.remove(KEY_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 165294) {
			insertStatement.setInt(RENAL_FAILURE_POS, valueCoded);
			positionsNotSet.remove(RENAL_FAILURE_POS);
		} else if (resultConceptId == 1713) {
			insertStatement.setDouble(PILLS_LEFTOVER_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(PILLS_LEFTOVER_POS);
		} else if (resultConceptId == 165221) {
			insertStatement.setInt(ADH_COUNSELING_POS, valueCoded);
			positionsNotSet.remove(ADH_COUNSELING_POS);
		} else if (resultConceptId == 6317) {
			insertStatement.setInt(CONDOMS_POS, valueCoded);
			positionsNotSet.remove(CONDOMS_POS);
		} else if (resultConceptId == 23771) {
			insertStatement.setInt(LUBS_POS, valueCoded);
			positionsNotSet.remove(LUBS_POS);
		} else if (resultConceptId == 165223 && encounterType == 81) {
			if (valueCoded == 1982) {
				insertStatement.setInt(PREGNANT_POS, 1065);
				positionsNotSet.remove(PREGNANT_POS);
			} else if (valueCoded == 6332) {
				insertStatement.setInt(LACTANT_POS, 1065);
				positionsNotSet.remove(LACTANT_POS);
			}
		} else if (resultConceptId == 1982 && encounterType == 80) {
			insertStatement.setInt(PREGNANT_POS, valueCoded);
			positionsNotSet.remove(PREGNANT_POS);
		} else if (resultConceptId == 6332 && encounterType == 80) {
			insertStatement.setInt(LACTANT_POS, valueCoded);
			positionsNotSet.remove(LACTANT_POS);
		} else if (resultConceptId == 165225) {
			switch (valueCoded) {
				case 1169:
					insertStatement.setInt(PREP_HIVPOS_POS, valueCoded);
					positionsNotSet.remove(PREP_HIVPOS_POS);
					break;
				case 2015:
					insertStatement.setInt(PREP_SIDEEFFECT_POS, valueCoded);
					positionsNotSet.remove(PREP_SIDEEFFECT_POS);
					break;
				case 165226:
					insertStatement.setInt(PREP_NORISK_POS, valueCoded);
					positionsNotSet.remove(PREP_NORISK_POS);
					break;
				case 165227:
					insertStatement.setInt(PREP_USERREQ_POS, valueCoded);
					positionsNotSet.remove(PREP_USERREQ_POS);
					break;
				case 5622:
					insertStatement.setInt(PREP_OTHER_POS, valueCoded);
					positionsNotSet.remove(PREP_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 165228) {
			insertStatement.setTimestamp(NEXT_CONSULT_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(NEXT_CONSULT_POS);
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case HIV_TEST_RES_DATE_POS:
					case CREATINE_DATE_POS:
					case STI_START_DATE_POS:
					case NEXT_CONSULT_POS:
						insertStatement.setNull(pos, Types.DATE);
						break;
					case CREATINE_RES_POS:
					case NO_UNITS_POS:
					case PILLS_LEFTOVER_POS:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
