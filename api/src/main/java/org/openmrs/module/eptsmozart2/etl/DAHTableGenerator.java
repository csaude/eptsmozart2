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
public class DAHTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final String CREATE_TABLE_FILE_NAME = "dah.sql";
	
	public static final Integer[] DAH_CONCEPT_IDS = new Integer[] { 1255, 21151, 1087, 5356, 165381 };
	
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
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" WHERE !o.voided AND o.concept_id IN ").append(inClause(DAH_CONCEPT_IDS))
		        .toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.encounter_datetime, e.uuid as encounter_uuid, ")
		        .append("e.encounter_id as e_encounter_id, o.* FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" WHERE !o.voided AND o.concept_id IN ").append(inClause(DAH_CONCEPT_IDS))
		        .append(" ORDER BY o.encounter_id");
		return sb.toString();
	}
	
	@Override
	protected String insertSql() {
		return new StringBuilder("INSERT INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".dah (encounter_uuid, encounter_date, status_tarv, tarv_line, tarv_regimen, who_stage, ")
		        .append(
		            "exit_criteria_nocondition, exit_criteria_cvsupressed, exit_criteria_cd4, exit_criteria_nofluconazol) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
	}
	
	@Override
    protected Set<Integer> getAllPositionsNotSet() {
        Set<Integer> positionsNotSet = new HashSet<>();
        positionsNotSet.addAll(Arrays.asList(STATUS_TARV_POS, TARV_LINE_POS, TARV_REGIMEN_POS, WHO_STAGE_POS,
											 EXIT_NOCONDITION_POS, EXIT_CVSUPRESSED_POS, EXIT_CD4_POS,
											 EXIT_NOFLUCONAZOL_POS));
        return positionsNotSet;
    }
	
	@Override
	protected void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException {
		insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
		insertStatement.setDate(ENCOUNTER_DATE_POS, scrollableResultSet.getDate("encounter_datetime"));
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
				insertStatement.setNull(iter.next(), Types.INTEGER);
			}
		}
	}
}
