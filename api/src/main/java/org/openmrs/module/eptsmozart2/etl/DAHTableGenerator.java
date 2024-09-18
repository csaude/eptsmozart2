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
public class DAHTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "dah.sql";
	
	public static final Integer[] DAH_CONCEPT_IDS = new Integer[] { 1255, 21151, 1087, 5356, 165381, 1413, 507 };
	
	public static final Integer ENCOUNTER_TYPE_ID = 90;
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int STATUS_TARV_POS = 3;
	
	protected final int TARV_LINE_POS = 4;
	
	protected final int TARV_REGIMEN_POS = 5;
	
	protected final int WHO_STAGE_POS = 6;
	
	protected final int EXIT_NOCONDITION_POS = 7;
	
	protected final int EXIT_CVSUPRESSED_POS = 8;
	
	protected final int EXIT_CD4_POS = 9;
	
	protected final int EXIT_NOFLUCONAZOL_POS = 10;
	
	protected final int ENC_TYPE_POS = 11;
	
	protected final int ENC_CREATED_DATE_POS = 12;
	
	protected final int ENC_CHANGE_DATE_POS = 13;
	
	protected final int FORM_ID_POS = 14;
	
	protected final int PATIENT_UUID_POS = 15;
	
	protected final int LOC_UUID_POS = 16;
	
	protected final int SARCK_DIAGDATE_POS = 17;
	
	protected final int SARCK_STAGE_POS = 18;
	
	protected final int SRC_DB_POS = 19;
	
	@Override
	public String getTable() {
		return "dah";
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
		        .append(" AND e.concept_id IN ").append(inClause(DAH_CONCEPT_IDS)).toString();
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
		        .append(" AND e.concept_id IN ").append(inClause(DAH_CONCEPT_IDS)).append(" ORDER BY e.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".dah (encounter_uuid, encounter_date, status_tarv, tarv_line, tarv_regimen, who_stage, ")
		        .append("exit_criteria_nocondition, exit_criteria_cvsupressed, ")
		        .append("exit_criteria_cd4, exit_criteria_nofluconazol, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, form_id, ")
		        .append("patient_uuid, location_uuid, sarcoma_kaposi_diagnosis_date, ")
		        .append("sarcoma_kaposi_stage, source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(STATUS_TARV_POS, TARV_LINE_POS, TARV_REGIMEN_POS, WHO_STAGE_POS,
											 EXIT_NOCONDITION_POS, EXIT_CVSUPRESSED_POS, EXIT_CD4_POS,
											 EXIT_NOFLUCONAZOL_POS, SARCK_DIAGDATE_POS, SARCK_STAGE_POS));
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
		if (resultConceptId == 1255) {
			insertStatement.setInt(STATUS_TARV_POS, valueCoded);
			positionsNotSet.remove(STATUS_TARV_POS);
		} else if (resultConceptId == 21151) {
			insertStatement.setInt(TARV_LINE_POS, valueCoded);
			positionsNotSet.remove(TARV_LINE_POS);
		} else if (resultConceptId == 1087) {
			insertStatement.setInt(TARV_REGIMEN_POS, valueCoded);
			positionsNotSet.remove(TARV_REGIMEN_POS);
		} else if (resultConceptId == 5356) {
			insertStatement.setInt(WHO_STAGE_POS, valueCoded);
			positionsNotSet.remove(WHO_STAGE_POS);
		} else if (resultConceptId == 1413) {
			insertStatement.setTimestamp(SARCK_DIAGDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(SARCK_DIAGDATE_POS);
		} else if (resultConceptId == 507) {
			insertStatement.setInt(SARCK_STAGE_POS, valueCoded);
			positionsNotSet.remove(SARCK_STAGE_POS);
		} else if (resultConceptId == 165381) {
			switch (valueCoded) {
				case 165384:
					insertStatement.setInt(EXIT_NOFLUCONAZOL_POS, valueCoded);
					positionsNotSet.remove(EXIT_NOFLUCONAZOL_POS);
					break;
				case 165414:
					insertStatement.setInt(EXIT_NOCONDITION_POS, valueCoded);
					positionsNotSet.remove(EXIT_NOCONDITION_POS);
					break;
				case 165382:
					insertStatement.setInt(EXIT_CVSUPRESSED_POS, valueCoded);
					positionsNotSet.remove(EXIT_CVSUPRESSED_POS);
					break;
				case 165383:
					insertStatement.setInt(EXIT_CD4_POS, valueCoded);
					positionsNotSet.remove(EXIT_CD4_POS);
					break;
			}
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				Integer pos = iter.next();
				if (pos == SARCK_DIAGDATE_POS) {
					insertStatement.setNull(pos, Types.TIMESTAMP);
				} else {
					insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
