package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class TBDataTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TBDataTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "tb_data.sql";
	
	public static final Integer[] TB_CONCEPT_IDS = new Integer[] { 23758, 1766, 23761, 1268 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9 };
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int TB_SYMPTOM_POS = 3;
	
	protected final int SYMPTOM_FEVER_POS = 4;
	
	protected final int SYMPTOM_WLOSS_POS = 5;
	
	protected final int SYMPTOM_NSWEAT_POS = 6;
	
	protected final int SYMPTOM_COUGH_POS = 7;
	
	protected final int SYMPTOM_ASTHENIA_POS = 8;
	
	protected final int SYMPTOM_TBCONTACT_POS = 9;
	
	protected final int SYMPTOM_ADENOPATHY_POS = 10;
	
	protected final int TB_DIAGNOSE_POS = 11;
	
	protected final int TB_TREATMENT_POS = 12;
	
	protected final int TB_TREATMENTDATE_POS = 13;
	
	private boolean thereIsNext = false;
	
	private Integer encounterId = null;
	
	@Override
    protected int etl(Integer batchSize) throws SQLException {
        if (batchSize == null)
            batchSize = Integer.MAX_VALUE;
        String insertSql = new StringBuilder("INSERT INTO ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".tb_data (encounter_uuid, encounter_date, tb_symptom, symptom_fever, symptom_weight_loss, ")
                .append("symptom_night_sweat, symptom_cough, symptom_asthenia, symptom_tb_contact, symptom_adenopathy, ")
                .append("tb_diagnose, tb_treatment, tb_treatment_date) ")
                .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

        try {
            if (insertStatement == null) {
                insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
            } else {
                insertStatement.clearParameters();
            }
            int count = 0;
            Set<Integer> positionsNotSet = new HashSet<>();
            if(!thereIsNext) {
                thereIsNext = scrollableResultSet.next();
            }
            Integer prevEncounterId = null;
            positionsNotSet.addAll(Arrays.asList(TB_SYMPTOM_POS, SYMPTOM_FEVER_POS, SYMPTOM_WLOSS_POS, SYMPTOM_NSWEAT_POS,
                                                 SYMPTOM_COUGH_POS, SYMPTOM_ASTHENIA_POS, SYMPTOM_TBCONTACT_POS,
                                                 SYMPTOM_ADENOPATHY_POS, TB_DIAGNOSE_POS, TB_TREATMENT_POS, TB_TREATMENTDATE_POS));
            while (thereIsNext && (count < batchSize || Objects.equals(prevEncounterId, encounterId))) {
                encounterId = scrollableResultSet.getInt("encounter_id");
                insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
                insertStatement.setTimestamp(ENCOUNTER_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
                int resultConceptId = scrollableResultSet.getInt("concept_id");
                int valueCoded = scrollableResultSet.getInt("value_coded");
                if(resultConceptId == 23758) {
                    insertStatement.setInt(TB_SYMPTOM_POS, valueCoded);
                    positionsNotSet.remove(TB_SYMPTOM_POS);
                } else if(resultConceptId == 1766) {
                    switch (valueCoded) {
                        case 161:
                            insertStatement.setInt(SYMPTOM_ADENOPATHY_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_ADENOPATHY_POS);
                            break;
                        case 1760:
                            insertStatement.setInt(SYMPTOM_COUGH_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_COUGH_POS);
                            break;
                        case 1762:
                            insertStatement.setInt(SYMPTOM_NSWEAT_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_NSWEAT_POS);
                            break;
                        case 1763:
                            insertStatement.setInt(SYMPTOM_FEVER_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_FEVER_POS);
                            break;
                        case 1764:
                            insertStatement.setInt(SYMPTOM_WLOSS_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_WLOSS_POS);
                            break;
                        case 1765:
                            insertStatement.setInt(SYMPTOM_TBCONTACT_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_TBCONTACT_POS);
                            break;
                        case 23760:
                            insertStatement.setInt(SYMPTOM_ASTHENIA_POS, valueCoded);
                            positionsNotSet.remove(SYMPTOM_ASTHENIA_POS);
                            break;
                    }
                } else if(resultConceptId == 23761) {
                    insertStatement.setInt(TB_DIAGNOSE_POS, valueCoded);
                    positionsNotSet.remove(TB_DIAGNOSE_POS);
                } else if(resultConceptId == 1268) {
                    insertStatement.setInt(TB_TREATMENT_POS, valueCoded);
                    insertStatement.setTimestamp(TB_TREATMENTDATE_POS, scrollableResultSet.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(TB_TREATMENT_POS);
                    positionsNotSet.remove(TB_TREATMENTDATE_POS);
                }

                thereIsNext = scrollableResultSet.next();
                if(thereIsNext) {
                    prevEncounterId = encounterId;
                    encounterId = scrollableResultSet.getInt("encounter_id");
                    if(!Objects.equals(prevEncounterId, encounterId)) {
                        setEmptyPositions(positionsNotSet);
                        insertStatement.addBatch();
                        positionsNotSet.addAll(Arrays.asList(TB_SYMPTOM_POS, SYMPTOM_FEVER_POS, SYMPTOM_WLOSS_POS, SYMPTOM_NSWEAT_POS,
                                                             SYMPTOM_COUGH_POS, SYMPTOM_ASTHENIA_POS, SYMPTOM_TBCONTACT_POS,
                                                             SYMPTOM_ADENOPATHY_POS, TB_DIAGNOSE_POS, TB_TREATMENT_POS, TB_TREATMENTDATE_POS));
                        ++count;
                    }
                } else {
                    setEmptyPositions(positionsNotSet);
                    insertStatement.addBatch();
                    encounterId = null;
                    ++count;
                }
            }
            int[] outcomes = insertStatement.executeBatch();
            return outcomes.length;
        }
        catch (SQLException e) {
            LOGGER.error("Error preparing insert statement for table {}", getTable());
            this.setChanged();
            Utils.notifyObserversAboutException(this, e);
            throw e;
        }
    }
	
	@Override
	public String getTable() {
		return "tb_data";
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
		        .append(inClause(TB_CONCEPT_IDS)).toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.encounter_datetime, e.uuid as encounter_uuid, o.* FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(TB_CONCEPT_IDS)).append(" ORDER BY o.encounter_id");
		return sb.toString();
	}
	
	private void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case TB_TREATMENTDATE_POS:
						insertStatement.setNull(pos, Types.DATE);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
