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
public class CCRTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "ccr.sql";
	
	public static final Integer[] CCR_CONCEPT_IDS = new Integer[] { 1874, 5916, 6400, 6394, 1836, 5630, 1151, 985, 1873,
	        5090, 5089, 1998, 1030, 5526, 5254, 6046, 5633 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 92, 93 };
	
	public static final Integer NID_CCR_IDENTIFIER_TYPE = 9;
	
	public static final Integer RELATIONSHIP_TYPE_ID = 6;
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int FORM_ID_POS = 3;
	
	protected final int ENC_TYPE_POS = 4;
	
	protected final int PATIENT_UUID_POS = 5;
	
	protected final int ENC_CREATED_DATE_POS = 6;
	
	protected final int ENC_CHANGE_DATE_POS = 7;
	
	protected final int LOC_UUID_POS = 8;
	
	protected final int VR_PREMATURE_POS = 9;
	
	protected final int VR_LOWBIRTHWEIGHT_POS = 10;
	
	protected final int VR_FAILTOTHRIVE_POS = 11;
	
	protected final int VR_MALNUTRITION_POS = 12;
	
	protected final int VR_HIVEXPOSED_POS = 13;
	
	protected final int VR_NOMOTHER_POS = 14;
	
	protected final int VR_TB_POS = 15;
	
	protected final int VR_TWINS_POS = 16;
	
	protected final int VR_WEANING_POS = 17;
	
	protected final int VR_FAMILYMOVE_POS = 18;
	
	protected final int VR_OTHER_POS = 19;
	
	protected final int MOTHER_UUID_POS = 37;
	
	protected final int BIRTHWEIGHT_POS = 20;
	
	protected final int GESTATIONAL_AGE_POS = 21;
	
	protected final int ARV_MOTHER_POS = 22;
	
	protected final int ARV_CHILD_POS = 23;
	
	protected final int BIRTH_TYPE_POS = 24;
	
	protected final int INFANT_FEEDING_POS = 25;
	
	protected final int CURRENT_FEEDING_POS = 26;
	
	protected final int CCR_DISCHARGE_POS = 27;
	
	protected final int CCR_DISCHARGEDATE_POS = 28;
	
	protected final int CHILD_HEIGHT_POS = 29;
	
	protected final int CHILD_WEIGHT_POS = 30;
	
	protected final int PCR_SAMPLECOLLDATE_POS = 31;
	
	protected final int PCR_RESULTS_POS = 32;
	
	protected final int EXCL_BRSTFEEDING_POS = 33;
	
	protected final int FORMULAR_FEEDING_POS = 34;
	
	protected final int MIXED_FEEDING_POS = 35;
	
	protected final int COMP_FEEDING_POS = 36;
	
	protected final int SRC_DB_POS = 38;
	
	@Override
	public String getTable() {
		return "ccr";
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
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(CCR_CONCEPT_IDS)).toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.*, p.patient_uuid, pe.uuid as mother_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(CCR_CONCEPT_IDS)).append(" LEFT JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".relationship r ON e.patient_id = r.person_a AND !r.voided AND r.relationship = ")
		        .append(RELATIONSHIP_TYPE_ID).append(" LEFT JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".person pe ON pe.person_id = r.person_b ORDER BY e.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".ccr (encounter_uuid, encounter_date, form_id, encounter_type, patient_uuid, ")
		        .append("encounter_created_date, encounter_change_date, location_uuid, ")
		        .append("visit_reason_premature, visit_reason_low_birthweight, visit_reason_failure_to_thrive, ")
		        .append("visit_reason_malnutrition, visit_reason_hiv_exposed, visit_reason_no_mother, ")
		        .append("visit_reason_tb, visit_reason_twins, ")
		        .append("visit_reason_weaning, visit_reason_family_move, visit_reason_other, birthweight, ")
		        .append("gestational_age, arvs_mother, arvs_child, ").append("birth_type, infant_feeding, ")
		        .append("current_feeding, ccr_discharge, ccr_discharge_date, ")
		        .append("child_height, child_weight, pcr_sample_collection_date, pcr_result, ")
		        .append("exclusive_breastfeeding, formula_feeding, ")
		        .append("mixed_feeding, complementary_feeding, mother_uuid, source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
		        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(VR_PREMATURE_POS, VR_LOWBIRTHWEIGHT_POS, VR_FAILTOTHRIVE_POS, VR_MALNUTRITION_POS,
				VR_HIVEXPOSED_POS, VR_NOMOTHER_POS, VR_TB_POS, VR_TWINS_POS, VR_WEANING_POS,
				VR_FAMILYMOVE_POS, VR_OTHER_POS, BIRTHWEIGHT_POS, GESTATIONAL_AGE_POS,
				ARV_MOTHER_POS, ARV_CHILD_POS, BIRTH_TYPE_POS, INFANT_FEEDING_POS,
				CURRENT_FEEDING_POS, CCR_DISCHARGE_POS, CCR_DISCHARGEDATE_POS, CHILD_HEIGHT_POS, CHILD_WEIGHT_POS,
				PCR_SAMPLECOLLDATE_POS, PCR_RESULTS_POS, EXCL_BRSTFEEDING_POS, FORMULAR_FEEDING_POS,
				MIXED_FEEDING_POS, COMP_FEEDING_POS));
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
		insertStatement.setString(LOC_UUID_POS,
		    Mozart2Properties.getInstance().getLocationUuidById(scrollableResultSet.getInt("location_id")));
		insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
		int resultConceptId = scrollableResultSet.getInt("concept_id");
		int valueCoded = scrollableResultSet.getInt("value_coded");
		String motherUuidValue = scrollableResultSet.getString("mother_uuid");
		if (scrollableResultSet.wasNull()) {
			insertStatement.setNull(MOTHER_UUID_POS, Types.VARCHAR);
		} else {
			insertStatement.setString(MOTHER_UUID_POS, motherUuidValue);
		}
		if (resultConceptId == 1874) {
			switch (valueCoded) {
				case 1842:
					insertStatement.setInt(VR_PREMATURE_POS, valueCoded);
					positionsNotSet.remove(VR_PREMATURE_POS);
					break;
				case 6397:
					insertStatement.setInt(VR_LOWBIRTHWEIGHT_POS, valueCoded);
					positionsNotSet.remove(VR_LOWBIRTHWEIGHT_POS);
					break;
				case 5050:
					insertStatement.setInt(VR_FAILTOTHRIVE_POS, valueCoded);
					positionsNotSet.remove(VR_FAILTOTHRIVE_POS);
					break;
				case 1844:
					insertStatement.setInt(VR_MALNUTRITION_POS, valueCoded);
					positionsNotSet.remove(VR_MALNUTRITION_POS);
					break;
				case 1586:
					insertStatement.setInt(VR_HIVEXPOSED_POS, valueCoded);
					positionsNotSet.remove(VR_HIVEXPOSED_POS);
					break;
				case 1847:
					insertStatement.setInt(VR_NOMOTHER_POS, valueCoded);
					positionsNotSet.remove(VR_NOMOTHER_POS);
					break;
				case 1845:
					insertStatement.setInt(VR_TB_POS, valueCoded);
					positionsNotSet.remove(VR_TB_POS);
					break;
				case 1846:
					insertStatement.setInt(VR_TWINS_POS, valueCoded);
					positionsNotSet.remove(VR_TWINS_POS);
					break;
				case 1843:
					insertStatement.setInt(VR_WEANING_POS, valueCoded);
					positionsNotSet.remove(VR_WEANING_POS);
					break;
				case 6409:
					insertStatement.setInt(VR_FAMILYMOVE_POS, valueCoded);
					positionsNotSet.remove(VR_FAMILYMOVE_POS);
					break;
				case 5622:
					insertStatement.setInt(VR_OTHER_POS, valueCoded);
					positionsNotSet.remove(VR_OTHER_POS);
					break;
			}
		} else if (resultConceptId == 5916) {
			insertStatement.setDouble(BIRTHWEIGHT_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(BIRTHWEIGHT_POS);
		} else if (resultConceptId == 6400) {
			insertStatement.setDouble(GESTATIONAL_AGE_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(GESTATIONAL_AGE_POS);
		} else if (resultConceptId == 6394) {
			insertStatement.setInt(ARV_MOTHER_POS, valueCoded);
			positionsNotSet.remove(ARV_MOTHER_POS);
		} else if (resultConceptId == 1836) {
			insertStatement.setInt(ARV_CHILD_POS, valueCoded);
			positionsNotSet.remove(ARV_CHILD_POS);
		} else if (resultConceptId == 5630) {
			insertStatement.setInt(BIRTH_TYPE_POS, valueCoded);
			positionsNotSet.remove(BIRTH_TYPE_POS);
		} else if (resultConceptId == 1151) {
			insertStatement.setInt(INFANT_FEEDING_POS, valueCoded);
			positionsNotSet.remove(INFANT_FEEDING_POS);
		} else if (resultConceptId == 985) {
			insertStatement.setInt(CURRENT_FEEDING_POS, valueCoded);
			positionsNotSet.remove(CURRENT_FEEDING_POS);
		} else if (resultConceptId == 1873) {
			insertStatement.setInt(CCR_DISCHARGE_POS, valueCoded);
			insertStatement.setTimestamp(CCR_DISCHARGEDATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
			positionsNotSet.remove(CCR_DISCHARGE_POS);
			positionsNotSet.remove(CCR_DISCHARGEDATE_POS);
		} else if (resultConceptId == 5090) {
			insertStatement.setDouble(CHILD_HEIGHT_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(CHILD_HEIGHT_POS);
		} else if (resultConceptId == 5089) {
			insertStatement.setDouble(CHILD_WEIGHT_POS, scrollableResultSet.getDouble("value_numeric"));
			positionsNotSet.remove(CHILD_WEIGHT_POS);
		} else if (resultConceptId == 1998) {
			insertStatement.setTimestamp(PCR_SAMPLECOLLDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(PCR_SAMPLECOLLDATE_POS);
		} else if (resultConceptId == 1030) {
			insertStatement.setInt(PCR_RESULTS_POS, valueCoded);
			positionsNotSet.remove(PCR_RESULTS_POS);
		} else if (resultConceptId == 5526) {
			insertStatement.setInt(EXCL_BRSTFEEDING_POS, valueCoded);
			positionsNotSet.remove(EXCL_BRSTFEEDING_POS);
		} else if (resultConceptId == 5254) {
			insertStatement.setInt(FORMULAR_FEEDING_POS, valueCoded);
			positionsNotSet.remove(FORMULAR_FEEDING_POS);
		} else if (resultConceptId == 6046) {
			insertStatement.setInt(MIXED_FEEDING_POS, valueCoded);
			positionsNotSet.remove(MIXED_FEEDING_POS);
		} else if (resultConceptId == 5633) {
			insertStatement.setInt(COMP_FEEDING_POS, valueCoded);
			positionsNotSet.remove(COMP_FEEDING_POS);
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case BIRTHWEIGHT_POS:
					case GESTATIONAL_AGE_POS:
					case CHILD_HEIGHT_POS:
					case CHILD_WEIGHT_POS:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case CCR_DISCHARGEDATE_POS:
					case PCR_SAMPLECOLLDATE_POS:
						insertStatement.setNull(pos, Types.TIMESTAMP);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
