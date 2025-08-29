package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class ProphylaxisTableGenerator extends AbstractScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ProphylaxisTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "prophylaxis.sql";
	
	public static final Integer[] PROPHYLAXIS_CONCEPT_IDS = new Integer[]
										{ 23985, 6121, 165308, 23987, 23762, 23763, 23986, 23988 };
	
	public static final int PROPHYLAXIS_STATUS_CONCEPT = 165308;
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 60 };
	public static final Integer[] ALL_ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 60, 53 };

	public static final int FICHA_RESUMO_ENCTYPE = 53;
	
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
	
	protected int currentEncounterType;
	protected boolean batchAlreadyAdded = false;
	protected Map<Integer, Map<Integer, Object>> encType53Map = new HashMap<>();

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
		return new StringBuilder("SELECT SUM(conta) FROM (SELECT COUNT(DISTINCT e.encounter_id) AS conta FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(PROPHYLAXIS_CONCEPT_IDS)).append(" UNION ")
		        .append("SELECT COUNT(DISTINCT e.encounter_id) AS conta FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(FICHA_RESUMO_ENCTYPE).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id = 23985 AND NOT EXISTS ")
				.append("(SELECT 1 FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
				.append(".encounter_obs e1 WHERE e.encounter_id = e1.encounter_id AND e1.concept_id = ")
				.append(PROPHYLAXIS_STATUS_CONCEPT).append(") UNION ")
				.append("SELECT COUNT(e.encounter_id) AS conta FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND e.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(FICHA_RESUMO_ENCTYPE).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id = ").append(PROPHYLAXIS_STATUS_CONCEPT).append(") AS record_count").toString();
	}
	
	@Override
	protected String fetchQuery() {
		StringBuilder sb = new StringBuilder("SELECT e.*, p.patient_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON e.patient_id = p.patient_id AND ").append("CASE WHEN e.encounter_type = ")
		        .append(FICHA_RESUMO_ENCTYPE).append(" AND e.concept_id = ").append(PROPHYLAXIS_STATUS_CONCEPT)
		        .append(" THEN e.obs_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("' ELSE e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' END AND e.encounter_type IN ")
		        .append(inClause(ALL_ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
		        .append(" AND e.concept_id IN ").append(inClause(PROPHYLAXIS_CONCEPT_IDS))
		        .append(" ORDER BY e.encounter_id");
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
		int valueCoded = scrollableResultSet.getInt("value_coded");
		int conceptId = scrollableResultSet.getInt("concept_id");
		if(currentEncounterType == FICHA_RESUMO_ENCTYPE) {
			if(!encType53Map.containsKey(encounterId)) {
				Map<Integer, Object> currentEncounterData = new HashMap<>();
				currentEncounterData.put(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
				currentEncounterData.put(ENCOUNTER_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
				currentEncounterData.put(ENC_CREATED_DATE_POS, scrollableResultSet.getTimestamp("e_date_created"));
				currentEncounterData.put(ENC_CHANGE_DATE_POS, scrollableResultSet.getTimestamp("e_date_changed"));
				currentEncounterData.put(FORM_ID_POS, scrollableResultSet.getInt("form_id"));
				currentEncounterData.put(PATIENT_UUID_POS, scrollableResultSet.getString("patient_uuid"));
				currentEncounterData.put(LOC_UUID_POS,
						Mozart2Properties.getInstance().getLocationUuidById(scrollableResultSet.getInt("location_id")));
				encType53Map.put(encounterId, currentEncounterData);
			}
			if (conceptId == 23985) {
				encType53Map.get(encounterId).put(PROPHY_TPT_POS, valueCoded);
			} else if(conceptId == 165308) {
				if(!encType53Map.get(encounterId).containsKey(PROPHYLAXIS_STATUS_CONCEPT)) {
					encType53Map.get(encounterId).put(PROPHYLAXIS_STATUS_CONCEPT, new HashMap<Integer, List<Timestamp>>());
				}
				Map<Integer, List<Timestamp>> statusTimestampMap =
						((Map<Integer, List<Timestamp>>)encType53Map.get(encounterId).get(PROPHYLAXIS_STATUS_CONCEPT));
				if(!statusTimestampMap.containsKey(valueCoded)) {
					statusTimestampMap.put(valueCoded, new ArrayList<>());
				}
				statusTimestampMap.get(valueCoded).add(scrollableResultSet.getTimestamp("obs_datetime"));
			}
		} else {
			batchAlreadyAdded = false;
			insertStatement.setString(ENCOUNTER_UUID_POS, scrollableResultSet.getString("encounter_uuid"));
			insertStatement.setTimestamp(ENCOUNTER_DATE_POS, scrollableResultSet.getTimestamp("encounter_datetime"));
			insertStatement.setInt(ENC_TYPE_POS, currentEncounterType);
			insertStatement.setTimestamp(ENC_CREATED_DATE_POS, scrollableResultSet.getTimestamp("e_date_created"));
			insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, scrollableResultSet.getTimestamp("e_date_changed"));
			insertStatement.setInt(FORM_ID_POS, scrollableResultSet.getInt("form_id"));
			insertStatement.setString(PATIENT_UUID_POS, scrollableResultSet.getString("patient_uuid"));
			insertStatement.setString(LOC_UUID_POS,
					Mozart2Properties.getInstance().getLocationUuidById(scrollableResultSet.getInt("location_id")));
			insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());

			if (conceptId == 23985) {
				insertStatement.setInt(PROPHY_TPT_POS, valueCoded);
				positionsNotSet.remove(PROPHY_TPT_POS);
			} else if (conceptId == 6121) {
				insertStatement.setInt(PROPHY_CTX_POS, valueCoded);
				positionsNotSet.remove(PROPHY_CTX_POS);
			} else if (conceptId == 165308 || conceptId == 23987) {
				insertStatement.setInt(PROPHY_STATUS_POS, valueCoded);
				positionsNotSet.remove(PROPHY_STATUS_POS);
			} else if (conceptId == 23762) {
				insertStatement.setInt(INH_SECEFFECTS_POS, valueCoded);
				positionsNotSet.remove(INH_SECEFFECTS_POS);
			} else if (conceptId == 23763) {
				insertStatement.setInt(CTZ_SECEFFECTS_POS, valueCoded);
				positionsNotSet.remove(CTZ_SECEFFECTS_POS);
			} else if (conceptId == 23986) {
				insertStatement.setInt(DISP_TYPE_POS, valueCoded);
				positionsNotSet.remove(DISP_TYPE_POS);
			} else if (conceptId == 23988) {
				insertStatement.setTimestamp(NEXT_PDATE_POS, scrollableResultSet.getTimestamp("value_datetime"));
				positionsNotSet.remove(NEXT_PDATE_POS);
			}
		}
	}

	@Override
	protected int etl(Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = insertSql();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			Set<Integer> positionsNotSet = getAllPositionsNotSet();
			if (!thereIsNext) {
				thereIsNext = scrollableResultSet.next();
				if (thereIsNext) {
					encounterId = scrollableResultSet.getInt("encounter_id");
					currentEncounterType = scrollableResultSet.getInt("encounter_type");
				}
			}
			Integer prevEncounterId = null;
			Integer prevEncounterType;
			while (thereIsNext && (count < batchSize || Objects.equals(prevEncounterId, encounterId))) {
				setInsertSqlParameters(positionsNotSet);
				thereIsNext = scrollableResultSet.next();
				if (thereIsNext) {
					prevEncounterId = encounterId;
					prevEncounterType = currentEncounterType;
					encounterId = scrollableResultSet.getInt("encounter_id");
					if (!Objects.equals(prevEncounterId, encounterId) &&
							!Objects.equals(prevEncounterType,FICHA_RESUMO_ENCTYPE)) {
						if(!batchAlreadyAdded) {
							setEmptyPositions(positionsNotSet);
							insertStatement.addBatch();
							positionsNotSet = getAllPositionsNotSet();
							++count;
							batchAlreadyAdded = true;
						}
					}
					currentEncounterType = scrollableResultSet.getInt("encounter_type");
				} else {
					if(!batchAlreadyAdded) {
						setEmptyPositions(positionsNotSet);
						insertStatement.addBatch();
						encounterId = null;
						++count;
						batchAlreadyAdded = true;
					}
				}
			}
			addEncType53FichaResumoRecords();
			int[] outcomes = insertStatement.executeBatch();
			encType53Map.clear();
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

	protected void addEncType53FichaResumoRecords() throws SQLException {
		try {
			for (Map.Entry<Integer, Map<Integer, Object>> entry : encType53Map.entrySet()) {
				final Set<Integer> positionsNotSet = getAllPositionsNotSet();
				final Map encounterData = entry.getValue();
				insertStatement.setString(ENCOUNTER_UUID_POS, (String) encounterData.get(ENCOUNTER_UUID_POS));
				insertStatement.setTimestamp(ENCOUNTER_DATE_POS, (Timestamp) encounterData.get(ENCOUNTER_DATE_POS));
				insertStatement.setInt(ENC_TYPE_POS, FICHA_RESUMO_ENCTYPE);
				insertStatement.setTimestamp(ENC_CREATED_DATE_POS, (Timestamp) encounterData.get(ENC_CREATED_DATE_POS));
				insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, (Timestamp) encounterData.get(ENC_CHANGE_DATE_POS));
				insertStatement.setInt(FORM_ID_POS, (Integer) encounterData.get(FORM_ID_POS));
				insertStatement.setString(PATIENT_UUID_POS, (String) encounterData.get(PATIENT_UUID_POS));
				insertStatement.setString(LOC_UUID_POS, (String) encounterData.get(LOC_UUID_POS));
				insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());

				final Integer tptValue = (Integer) encounterData.get(PROPHY_TPT_POS);
				if (tptValue != null) {
					insertStatement.setInt(PROPHY_TPT_POS, tptValue);
					positionsNotSet.remove(PROPHY_TPT_POS);
				}
				// Handle concept status (tpt prophylaxis status on ficha resumo)
				if (encounterData.containsKey(PROPHYLAXIS_STATUS_CONCEPT)) {
					for (Map.Entry<Integer, Timestamp> statusEntry :
							((Map<Integer, Timestamp>) encounterData.get(PROPHYLAXIS_STATUS_CONCEPT)).entrySet()) {
						for(Timestamp obsDatetime: (List<Timestamp>)statusEntry.getValue()) {
							insertStatement.setInt(PROPHY_STATUS_POS, statusEntry.getKey());
							insertStatement.setTimestamp(ENCOUNTER_DATE_POS, obsDatetime);
							positionsNotSet.remove(PROPHY_STATUS_POS);
							setEmptyPositions(positionsNotSet);
							insertStatement.addBatch();
						}
					}
				} else {
					setEmptyPositions(positionsNotSet);
					insertStatement.addBatch();
				}
			}
		} catch (Exception e) {
			LOGGER.error("An error has occured while inserting records to {} table", getTable(), e);
			throw new SQLException(e);
		}
	}
}
