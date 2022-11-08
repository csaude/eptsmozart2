package org.openmrs.module.eptsmozart2.etl;

import org.apache.commons.collections.map.HashedMap;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/15/22.
 */
public class MedicationTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "medication.sql";
	
	public static final Integer[] REGIMEN_CONCEPT_IDS = new Integer[] { 1087, 1088, 21187, 21188, 21190, 23893 };
	
	public static final Integer[] REGIMEN_CONCEPT_IDS_ENCTYPE_53 = new Integer[] { 1088, 21187, 21188, 21190, 23893 };
	
	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 18, 52, 53 };

	public static final Map<Integer, Map<Integer, String>> ENC_TYPE5_MED_LINES = new HashMap<>(5);

	static {
		Map<Integer, String> firstLine = new HashMap<>();
		firstLine.put(21150, "FIRST LINE");

		Map<Integer, String> secondLine = new HashMap<>();
		secondLine.put(21148, "SECOND LINE");

		Map<Integer, String> thirdLine = new HashMap<>();
		thirdLine.put(21149, "THIRD LINE");

		Map<Integer, String> alternativeLine = new HashMap<>();
		alternativeLine.put(23741, "ALTERNATIVE 1st LINE OF THE ART");

		ENC_TYPE5_MED_LINES.put(23893, firstLine);
		ENC_TYPE5_MED_LINES.put(21187, secondLine);
		ENC_TYPE5_MED_LINES.put(21188, thirdLine);
		ENC_TYPE5_MED_LINES.put(21190, alternativeLine);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException {
		return prepareInsertStatement(resultSet, null);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".medication (encounter_uuid, medication_pickup_date, regimen_concept, ")
		        .append("formulation_id, formulation_drug, quantity, dosage, next_pickup_date, ")
				.append("mode_dispensation_id,med_line_id, type_dispensation_id, ")
		        .append("alternative_line_id, reason_change_regimen_id, ")
				.append("arv_side_effects_id, adherence_id, ")
				.append("regimen_id, medication_uuid) ")
		        .append("VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

		Set<Integer> positionsNotSet = new HashSet<>();
		PreparedStatement fetchMedsDetailsStatement = null;
		ResultSet medObsResults = null;
		try (Connection connection = ConnectionPool.getConnection()) {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			String fetchMoreObsSql = new StringBuilder("SELECT o.* FROM ")
					.append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o ")
					.append("WHERE o.encounter_id = ? AND o.concept_id IN (1711, 1715, 165256, 23740, 5096, 165174, 21151, 23742, 21190, 21187,")
					.append("21188, 23739, 23742, 1792, 2015, 6223, 1190)").toString();
			
			fetchMedsDetailsStatement = connection.prepareStatement(fetchMoreObsSql);
			int count = 0;
			Map<Integer, Object> parameterCache = new HashedMap();
			while (results.next() && count < batchSize) {
				positionsNotSet.addAll(Arrays.asList(4,5,6,7,8,9,10,11,12,13,14,15));
				parameterCache.clear();
				final int currentEncounterId = results.getInt("encounter_id");
				final int currentEncounterTypeId = results.getInt("encounter_type");
				final int currentConceptId = results.getInt("concept_id");

				insertStatement.setString(1, results.getString("encounter_uuid"));

				//MOZ2-41
				if(currentEncounterTypeId == 52) {
					insertStatement.setDate(2, results.getDate("value_datetime"));
					insertStatement.setString(17, results.getString("medication_uuid"));
					insertStatement.setNull(16, Types.INTEGER);
					insertStatement.setNull(4, Types.VARCHAR);
					insertStatement.setNull(3, Types.INTEGER);
					setEmptyPositions(positionsNotSet);
					insertStatement.addBatch();
					++count;
					continue;
				}

				// MOZ2-43
				if(currentEncounterTypeId == 53) {
					if(Arrays.asList( 21187, 21188, 21190).contains(currentConceptId)) {
						parameterCache.put(2, results.getDate("obs_datetime"));
						insertStatement.setDate(2, results.getDate("obs_datetime"));
					} else if(currentConceptId == 23893) {
						// MOZ2-48
						insertStatement.setNull(2, Types.DATE);
					}
				} else {
					parameterCache.put(2, results.getDate("encounter_date"));
					insertStatement.setDate(2, results.getDate("encounter_date"));
				}

				// MOZ2-49
				if(currentEncounterTypeId == 53 && Arrays.asList( 21187, 21188, 21190, 23893).contains(currentConceptId)) {
					Map.Entry<Integer, String> entry = ENC_TYPE5_MED_LINES.get(currentConceptId).entrySet().iterator().next();

					parameterCache.put(10, entry.getKey());
					insertStatement.setInt(10, entry.getKey());
					positionsNotSet.remove(10);
				}

				insertStatement.setInt(3, results.getInt("value_coded"));

				insertStatement.setInt(16, currentConceptId);
				insertStatement.setString(17, results.getString("medication_uuid"));

				fetchMedsDetailsStatement.clearParameters();
				fetchMedsDetailsStatement.setInt(1, currentEncounterId);
				medObsResults = fetchMedsDetailsStatement.executeQuery();
				Map<Integer, Map<Integer, Object>> formulationsDosagesQuantities = new HashedMap();
				while(medObsResults.next()) {
					int conceptId = medObsResults.getInt("concept_id");
					Integer obsGrouper = medObsResults.getInt("obs_group_id");
					obsGrouper = obsGrouper == 0 ? null : obsGrouper;
					switch(conceptId) {
						case 1190:
							// MOZ2-43
							if(currentConceptId == 23893 && currentEncounterTypeId == 53) {
								// Overwrite the medication_pickup_date.
								parameterCache.put(2, medObsResults.getDate("value_datetime"));
								insertStatement.setDate(2, medObsResults.getDate("value_datetime"));
							}
							break;
						case 165256:
							// formulation
							if(obsGrouper != null) {
								if(!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
								}
								formulationsDosagesQuantities.get(obsGrouper).put(4,  medObsResults.getInt("value_coded"));
								formulationsDosagesQuantities.get(obsGrouper).put(5, medObsResults.getInt("value_drug"));
							} else {
								insertStatement.setInt(4, medObsResults.getInt("value_coded"));
								insertStatement.setInt(5, medObsResults.getInt("value_drug"));
								positionsNotSet.removeAll(Arrays.asList(4, 5));
							}
							break;
						case 1715:case 23740:
							// quantity
							final Integer QUANTITY_POS = 6;
							if(obsGrouper != null) {
								if (!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
								}
								formulationsDosagesQuantities.get(obsGrouper).put(QUANTITY_POS, medObsResults.getDouble("value_numeric"));
							} else {
								insertStatement.setDouble(QUANTITY_POS, medObsResults.getDouble("value_numeric"));
								positionsNotSet.remove(QUANTITY_POS);
							}
							break;
						case 1711:
							// dosage
							final Integer DOSAGE_POS = 7;
							if(obsGrouper != null) {
								if (!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
								}
								formulationsDosagesQuantities.get(obsGrouper).put(DOSAGE_POS, medObsResults.getString("value_text"));
							} else {
								insertStatement.setString(DOSAGE_POS, medObsResults.getString("value_text"));
								positionsNotSet.remove(DOSAGE_POS);
							}
							break;
						case 5096:
							// next_pickup_date
							insertStatement.setDate(8, medObsResults.getDate("value_datetime"));
							parameterCache.put(8, medObsResults.getDate("value_datetime"));
							positionsNotSet.remove(8);
							break;
						case 165174:
							// mode_dispensation
							parameterCache.put(9, medObsResults.getInt("value_coded"));
							insertStatement.setInt(9, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(9);
							break;
						case 21151:case 23893:case 21190:case 21187:case 21188:
							// med_line
							if(!(currentEncounterTypeId == 53 && Arrays.asList( 21187, 21188, 21190, 23893).contains(conceptId))) {
								parameterCache.put(10, medObsResults.getInt("value_coded"));
								insertStatement.setInt(10, medObsResults.getInt("value_coded"));
								positionsNotSet.remove(10);
							}
							break;
						case 23739:
							// type_dispensation
							parameterCache.put(11, medObsResults.getInt("value_coded"));
							insertStatement.setInt(11, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(11);
							break;
						case 23742:
							// alternative_line
							parameterCache.put(12, medObsResults.getInt("value_coded"));
							insertStatement.setInt(12, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(12);
							break;
						case 1792:
							// reason_change_regimen
							parameterCache.put(13, medObsResults.getInt("value_coded"));
							insertStatement.setInt(13, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(13);
							break;
						case 2015:
							// arv_side_effects
							parameterCache.put(14, medObsResults.getInt("value_coded"));
							insertStatement.setInt(14, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(14);
							break;
						case 6223:
							// adherence
							parameterCache.put(15, medObsResults.getInt("value_coded"));
							insertStatement.setInt(15, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(15);
							break;
					}
				}

				if(!formulationsDosagesQuantities.isEmpty()) {
					Iterator<Integer> obsGroupers = formulationsDosagesQuantities.keySet().iterator();
					Map<Integer, Object> group = formulationsDosagesQuantities.get(obsGroupers.next());
					setFormulationDosageQuantity(group, positionsNotSet);
					setEmptyPositions(positionsNotSet);
					insertStatement.addBatch();

					while(obsGroupers.hasNext()) {
						group = formulationsDosagesQuantities.get(obsGroupers.next());
						insertStatement.setString(1, results.getString("encounter_uuid"));

						if(parameterCache.containsKey(2)) {
							insertStatement.setDate(2, (Date) parameterCache.get(2));
						} else {
							insertStatement.setNull(2, Types.DATE);
						}
						
						insertStatement.setInt(3, results.getInt("value_coded"));
						insertStatement.setInt(16, results.getInt("concept_id"));

						if(parameterCache.containsKey(8)) {
							insertStatement.setDate(8, (Date) parameterCache.get(8));
						} else {
							positionsNotSet.add(8);
						}

						if(parameterCache.containsKey(9)) {
							insertStatement.setInt(9, (int) parameterCache.get(9));
						} else {
							positionsNotSet.add(9);
						}

						if(parameterCache.containsKey(10)) {
							insertStatement.setInt(10, (int) parameterCache.get(10));
						} else {
							positionsNotSet.add(10);
						}

						if(parameterCache.containsKey(11)) {
							insertStatement.setInt(11, (int) parameterCache.get(11));
						} else {
							positionsNotSet.add(11);
						}

						if(parameterCache.containsKey(12)) {
							insertStatement.setInt(12, (int) parameterCache.get(12));
						} else {
							positionsNotSet.add(12);
						}

						if(parameterCache.containsKey(13)) {
							insertStatement.setInt(13, (int) parameterCache.get(13));
						} else {
							positionsNotSet.add(13);
						}

						if(parameterCache.containsKey(14)) {
							insertStatement.setInt(14, (int) parameterCache.get(14));
						} else {
							positionsNotSet.add(14);
						}

						if(parameterCache.containsKey(15)) {
							insertStatement.setInt(15, (int) parameterCache.get(15));
						} else {
							positionsNotSet.add(15);
						}

						setFormulationDosageQuantity(group, positionsNotSet);
						setEmptyPositions(positionsNotSet);
						insertStatement.addBatch();
					}
				} else {
					setEmptyPositions(positionsNotSet);
					insertStatement.addBatch();
				}
				++count;
			}
			return insertStatement;
		}
		catch (SQLException e) {
			LOGGER.error("Error preparing insert statement for table {}", getTable());
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		} finally {
			if(medObsResults != null) {
				medObsResults.close();
			}
			if(fetchMedsDetailsStatement != null) {
				fetchMedsDetailsStatement.close();
			}
		}
	}
	
	@Override
	public String getTable() {
		return "medication";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS))
		        .append(" WHERE !o.voided AND CASE WHEN e.encounter_type = 52 THEN o.concept_id = 23866")
		        .append(" WHEN e.encounter_type = 53 THEN o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS_ENCTYPE_53))
		        .append(" ELSE o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
		        .append(" END AND CASE WHEN o.concept_id = 23866 THEN o.value_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' ELSE o.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' END");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder(
		        "SELECT o.obs_id, o.obs_datetime, o.uuid as medication_uuid, o.obs_group_id, o.concept_id, ")
		        .append(
		            "o.value_coded, o.value_datetime, o.encounter_id, e.uuid as encounter_uuid, o.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_type, e.encounter_datetime as encounter_date FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS))
		        .append(" WHERE !o.voided AND CASE WHEN e.encounter_type = 52 THEN o.concept_id = 23866 ")
		        .append("WHEN e.encounter_type = 53 THEN o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS_ENCTYPE_53))
		        .append(" ELSE o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
		        .append(" END AND CASE WHEN o.concept_id = 23866 THEN o.value_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' ELSE o.obs_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' END ORDER BY o.obs_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
	
	private void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case 6:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case 8:
						insertStatement.setNull(pos, Types.DATE);
						break;
					case 7:
						insertStatement.setNull(pos, Types.VARCHAR);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
	
	private void setFormulationDosageQuantity(Map<Integer, Object> group, Set<Integer> positionsNotSet) throws SQLException {
		if (group.containsKey(4)) {
			insertStatement.setInt(4, (int) group.get(4));
			insertStatement.setInt(5, (int) group.get(5));
			positionsNotSet.removeAll(Arrays.asList(4, 5));
		} else {
			insertStatement.setNull(4, Types.INTEGER);
			insertStatement.setNull(5, Types.INTEGER);
		}
		
		if (group.containsKey(6)) {
			insertStatement.setDouble(6, (double) group.get(6));
			positionsNotSet.remove(6);
		} else {
			insertStatement.setNull(6, Types.DOUBLE);
		}
		
		if (group.containsKey(7)) {
			insertStatement.setString(7, (String) group.get(7));
			positionsNotSet.remove(7);
		} else {
			insertStatement.setNull(7, Types.VARCHAR);
		}
	}
}
