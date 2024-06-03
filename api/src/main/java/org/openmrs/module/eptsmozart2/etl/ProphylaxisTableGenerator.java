package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ProphylaxisTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ProphylaxisTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "prophylaxis.sql";
	
	public static final Integer[] PROPHYLAXIS_CONCEPT_IDS = new Integer[] { 23985, 6121, 165308, 23987, 23762, 23763, 23986,
	        23988 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 60 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int PROPHY_TPT_POS = 3;
	
	protected final int PROPHY_CTX_POS = 4;
	
	protected final int PROPHY_STATUS_POS = 5;
	
	protected final int INH_SECEFFECTS_POS = 6;
	
	protected final int CTZ_SECEFFECTS_POS = 7;
	
	protected final int DISP_TYPE_POS = 8;
	
	protected final int NEXT_PDATE_POS = 9;
	
	protected final int ENC_TYPE_POS = 10;
	
	protected final int ENC_CREATED_DATE_POS = 11;
	
	protected final int ENC_CHANGE_DATE_POS = 12;
	
	protected final int FORM_ID_POS = 13;
	
	protected final int PATIENT_UUID_POS = 14;
	
	protected final int LOC_UUID_POS = 15;
	
	protected final int SRC_DB_POS = 16;
	
	@Override
	public String getTable() {
		return "prophylaxis";
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
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(PROPHYLAXIS_CONCEPT_IDS)).toString();
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
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(PROPHYLAXIS_CONCEPT_IDS)).append(" ORDER BY o.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".prophylaxis (encounter_uuid, encounter_date, regimen_prophylaxis_tpt, ")
		        .append("regimen_prophylaxis_ctx, prophylaxis_status, secondary_effects_tpt, ")
		        .append("secondary_effects_ctz, dispensation_type, next_pickup_date, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, ")
		        .append("form_id, patient_uuid, location_uuid, source_database) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(PROPHY_TPT_POS, PROPHY_CTX_POS,PROPHY_STATUS_POS, INH_SECEFFECTS_POS,
											 CTZ_SECEFFECTS_POS, DISP_TYPE_POS, NEXT_PDATE_POS));
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
		if (resultConceptId == 23985) {
			insertStatement.setInt(PROPHY_TPT_POS, valueCoded);
			positionsNotSet.remove(PROPHY_TPT_POS);
		} else if (resultConceptId == 6121) {
			insertStatement.setInt(PROPHY_CTX_POS, valueCoded);
			positionsNotSet.remove(PROPHY_CTX_POS);
		} else if (resultConceptId == 165308 || resultConceptId == 23987) {
			insertStatement.setInt(PROPHY_STATUS_POS, valueCoded);
			positionsNotSet.remove(PROPHY_STATUS_POS);
		} else if (resultConceptId == 23762) {
			insertStatement.setInt(INH_SECEFFECTS_POS, valueCoded);
			positionsNotSet.remove(INH_SECEFFECTS_POS);
		} else if (resultConceptId == 23763) {
			insertStatement.setInt(CTZ_SECEFFECTS_POS, valueCoded);
			positionsNotSet.remove(CTZ_SECEFFECTS_POS);
		} else if (resultConceptId == 23986) {
			insertStatement.setInt(DISP_TYPE_POS, valueCoded);
			positionsNotSet.remove(DISP_TYPE_POS);
		} else if (resultConceptId == 23988) {
			insertStatement.setTimestamp(NEXT_PDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
			positionsNotSet.remove(NEXT_PDATE_POS);
		}
	}
	
	@Override
	protected void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case NEXT_PDATE_POS:
						insertStatement.setNull(pos, Types.DATE);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
