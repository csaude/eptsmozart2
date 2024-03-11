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
public class MedicationTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "medication.sql";

	public static final Integer[] REGIMEN_CONCEPT_IDS = new Integer[] { 1087, 1088, 21187, 21188, 21190, 23893 };
	
	public static final Integer[] REGIMEN_CONCEPT_IDS_ENCTYPE_53 = new Integer[] { 1088, 21187, 21188, 21190, 23893 };

	public static final Integer ENCTYPE_52_VALUEDATETIME_CONCEPT_ID = 23866;

	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 18, 52, 53 };

	public static final Map<Integer, Map<Integer, String>> ENC_TYPE5_MED_LINES = new HashMap<>(5);

	protected final int ENCOUNTER_UUID_POS = 1;
	protected final int REGIMEN_ID_POS = 2;
	protected final int FORMULATION_ID_POS = 3;
	protected final int QUANTITY_POS = 4;
	protected final int DOSAGE_POS = 5;
	protected final int NEXT_PICKUP_DATE_POS = 6;
	protected final int MODE_DISPENSATION_ID_POS = 7;
	protected final int MED_LINE_ID_POS = 8;
	protected final int TYPE_DISPENSATION_ID_POS = 9;
	protected final int ALTERNATIVE_LINE_ID_POS = 10;
	protected final int REASON_CHANGE_REGIMEN_ID_POS = 11;
	protected final int ARV_SIDE_EFFECT_ID_POS = 12;
	protected final int ADHERENCE_ID_POS = 13;
	protected final int MEDICATION_UUID_POS = 14;
	protected final int MEDICATION_PICKUP_DATE_POS = 15;

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
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT IGNORE INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".medication (encounter_uuid, regimen_id, formulation_id, quantity_prescribed, dosage, next_pickup_date, ")
				.append("mode_dispensation_id, med_sequence_id, type_dispensation_id, alternative_line_id, reason_change_regimen_id, ")
				.append("med_side_effects_id, adherence_id, medication_uuid, medication_pickup_date) ")
		        .append("VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

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
					.append("21188, 23739, 23742, 1792, 2015, 6223, 1190) AND NOT voided").toString();
			
			fetchMedsDetailsStatement = connection.prepareStatement(fetchMoreObsSql);
			int count = 0;
			Map<Integer, Object> parameterCache = new HashedMap();
			while (results.next() && count < batchSize) {
				positionsNotSet.addAll(Arrays.asList(FORMULATION_ID_POS, QUANTITY_POS, DOSAGE_POS, NEXT_PICKUP_DATE_POS,
													 MODE_DISPENSATION_ID_POS, MED_LINE_ID_POS, TYPE_DISPENSATION_ID_POS,
													 ALTERNATIVE_LINE_ID_POS, REASON_CHANGE_REGIMEN_ID_POS, ARV_SIDE_EFFECT_ID_POS,
													 ADHERENCE_ID_POS));
				parameterCache.clear();
				final int currentEncounterId = results.getInt("encounter_id");
				final int currentEncounterTypeId = results.getInt("encounter_type");
				final int currentConceptId = results.getInt("concept_id");

				insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
				insertStatement.setTimestamp(MEDICATION_PICKUP_DATE_POS, results.getTimestamp("encounter_datetime"));

				//MOZ2-41
				if(currentEncounterTypeId == 52) {
					if(currentConceptId == ENCTYPE_52_VALUEDATETIME_CONCEPT_ID) {
						insertStatement.setTimestamp(MEDICATION_PICKUP_DATE_POS, results.getTimestamp("value_datetime"));
					}
					insertStatement.setString(MEDICATION_UUID_POS, results.getString("medication_uuid"));
					insertStatement.setNull(REGIMEN_ID_POS, Types.INTEGER);
					setEmptyPositions(positionsNotSet);
					insertStatement.addBatch();
					++count;
					continue;
				}

				// MOZ2-49
				if(currentEncounterTypeId == 53 && Arrays.asList( 21187, 21188, 21190, 23893).contains(currentConceptId)) {
					Map.Entry<Integer, String> entry = ENC_TYPE5_MED_LINES.get(currentConceptId).entrySet().iterator().next();

					parameterCache.put(MED_LINE_ID_POS, entry.getKey());
					insertStatement.setInt(MED_LINE_ID_POS, entry.getKey());
					positionsNotSet.remove(MED_LINE_ID_POS);
				}

				insertStatement.setInt(REGIMEN_ID_POS, results.getInt("value_coded"));
				insertStatement.setString(MEDICATION_UUID_POS, results.getString("medication_uuid"));

				fetchMedsDetailsStatement.clearParameters();
				fetchMedsDetailsStatement.setInt(1, currentEncounterId);
				medObsResults = fetchMedsDetailsStatement.executeQuery();
				Map<Integer, Map<Integer, Object>> formulationsDosagesQuantities = new HashedMap();
				while(medObsResults.next()) {
					int conceptId = medObsResults.getInt("concept_id");
					Integer obsGrouper = medObsResults.getInt("obs_group_id");
					obsGrouper = obsGrouper == 0 ? null : obsGrouper;
					switch(conceptId) {
						case 165256:
							// formulation
							if(obsGrouper != null) {
								if(!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
								}
								formulationsDosagesQuantities.get(obsGrouper).put(FORMULATION_ID_POS, medObsResults.getInt("value_drug"));
								formulationsDosagesQuantities.get(obsGrouper).put(MEDICATION_UUID_POS, medObsResults.getString("uuid"));
							} else {
								insertStatement.setInt(FORMULATION_ID_POS, medObsResults.getInt("value_drug"));
								insertStatement.setString(MEDICATION_UUID_POS, medObsResults.getString("uuid"));
								positionsNotSet.removeAll(Arrays.asList(FORMULATION_ID_POS));
							}
							break;
						case 1715:case 23740:
							// quantity_prescribed
							if(obsGrouper != null) {
								if (!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
									formulationsDosagesQuantities.get(obsGrouper).put(MEDICATION_UUID_POS, medObsResults.getString("uuid"));
								}
								formulationsDosagesQuantities.get(obsGrouper).put(QUANTITY_POS, medObsResults.getDouble("value_numeric"));
							} else {
								insertStatement.setDouble(QUANTITY_POS, medObsResults.getDouble("value_numeric"));
								positionsNotSet.remove(QUANTITY_POS);
							}
							break;
						case 1711:
							// dosage
							if(obsGrouper != null) {
								if (!formulationsDosagesQuantities.containsKey(obsGrouper)) {
									formulationsDosagesQuantities.put(obsGrouper, new HashedMap());
									formulationsDosagesQuantities.get(obsGrouper).put(MEDICATION_UUID_POS, medObsResults.getString("uuid"));
								}
								formulationsDosagesQuantities.get(obsGrouper).put(DOSAGE_POS, medObsResults.getString("value_text"));
							} else {
								insertStatement.setString(DOSAGE_POS, medObsResults.getString("value_text"));
								positionsNotSet.remove(DOSAGE_POS);
							}
							break;
						case 5096:
							// next_pickup_date
							insertStatement.setDate(NEXT_PICKUP_DATE_POS, medObsResults.getDate("value_datetime"));
							parameterCache.put(NEXT_PICKUP_DATE_POS, medObsResults.getDate("value_datetime"));
							positionsNotSet.remove(NEXT_PICKUP_DATE_POS);
							break;
						case 165174:
							// mode_dispensation
							if(currentEncounterTypeId == 18) {
								parameterCache.put(MODE_DISPENSATION_ID_POS, medObsResults.getInt("value_coded"));
								insertStatement.setInt(MODE_DISPENSATION_ID_POS, medObsResults.getInt("value_coded"));
								positionsNotSet.remove(MODE_DISPENSATION_ID_POS);
							}
							break;
						case 21151:case 23893:case 21190:case 21187:case 21188:
							// med_line
							if(!(currentEncounterTypeId == 53 && Arrays.asList( 21187, 21188, 21190, 23893).contains(conceptId))) {
								parameterCache.put(MED_LINE_ID_POS, medObsResults.getInt("value_coded"));
								insertStatement.setInt(MED_LINE_ID_POS, medObsResults.getInt("value_coded"));
								positionsNotSet.remove(MED_LINE_ID_POS);
							}
							break;
						case 23739:
							// type_dispensation
							parameterCache.put(TYPE_DISPENSATION_ID_POS, medObsResults.getInt("value_coded"));
							insertStatement.setInt(TYPE_DISPENSATION_ID_POS, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(TYPE_DISPENSATION_ID_POS);
							break;
						case 23742:
							// alternative_line
							parameterCache.put(ALTERNATIVE_LINE_ID_POS, medObsResults.getInt("value_coded"));
							insertStatement.setInt(ALTERNATIVE_LINE_ID_POS, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(ALTERNATIVE_LINE_ID_POS);
							break;
						case 1792:
							// reason_change_regimen
							parameterCache.put(REASON_CHANGE_REGIMEN_ID_POS, medObsResults.getInt("value_coded"));
							insertStatement.setInt(REASON_CHANGE_REGIMEN_ID_POS, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(REASON_CHANGE_REGIMEN_ID_POS);
							break;
						case 2015:
							// arv_side_effects
							parameterCache.put(ARV_SIDE_EFFECT_ID_POS, medObsResults.getInt("value_coded"));
							insertStatement.setInt(ARV_SIDE_EFFECT_ID_POS, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(ARV_SIDE_EFFECT_ID_POS);
							break;
						case 6223:
							// adherence
							parameterCache.put(ADHERENCE_ID_POS, medObsResults.getInt("value_coded"));
							insertStatement.setInt(ADHERENCE_ID_POS, medObsResults.getInt("value_coded"));
							positionsNotSet.remove(ADHERENCE_ID_POS);
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
						insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
						insertStatement.setTimestamp(MEDICATION_PICKUP_DATE_POS, results.getTimestamp("encounter_datetime"));
						
						insertStatement.setInt(REGIMEN_ID_POS, results.getInt("value_coded"));
						insertStatement.setString(MEDICATION_UUID_POS, results.getString("medication_uuid"));

						if(parameterCache.containsKey(NEXT_PICKUP_DATE_POS)) {
							insertStatement.setDate(NEXT_PICKUP_DATE_POS, (Date) parameterCache.get(NEXT_PICKUP_DATE_POS));
						} else {
							positionsNotSet.add(NEXT_PICKUP_DATE_POS);
						}

						if(parameterCache.containsKey(MODE_DISPENSATION_ID_POS)) {
							insertStatement.setInt(MODE_DISPENSATION_ID_POS, (int) parameterCache.get(MODE_DISPENSATION_ID_POS));
						} else {
							positionsNotSet.add(MODE_DISPENSATION_ID_POS);
						}

						if(parameterCache.containsKey(MED_LINE_ID_POS)) {
							insertStatement.setInt(MED_LINE_ID_POS, (int) parameterCache.get(MED_LINE_ID_POS));
						} else {
							positionsNotSet.add(MED_LINE_ID_POS);
						}

						if(parameterCache.containsKey(TYPE_DISPENSATION_ID_POS)) {
							insertStatement.setInt(TYPE_DISPENSATION_ID_POS, (int) parameterCache.get(TYPE_DISPENSATION_ID_POS));
						} else {
							positionsNotSet.add(TYPE_DISPENSATION_ID_POS);
						}

						if(parameterCache.containsKey(ALTERNATIVE_LINE_ID_POS)) {
							insertStatement.setInt(ALTERNATIVE_LINE_ID_POS, (int) parameterCache.get(ALTERNATIVE_LINE_ID_POS));
						} else {
							positionsNotSet.add(ALTERNATIVE_LINE_ID_POS);
						}

						if(parameterCache.containsKey(REASON_CHANGE_REGIMEN_ID_POS)) {
							insertStatement.setInt(REASON_CHANGE_REGIMEN_ID_POS, (int) parameterCache.get(REASON_CHANGE_REGIMEN_ID_POS));
						} else {
							positionsNotSet.add(REASON_CHANGE_REGIMEN_ID_POS);
						}

						if(parameterCache.containsKey(ARV_SIDE_EFFECT_ID_POS)) {
							insertStatement.setInt(ARV_SIDE_EFFECT_ID_POS, (int) parameterCache.get(ARV_SIDE_EFFECT_ID_POS));
						} else {
							positionsNotSet.add(ARV_SIDE_EFFECT_ID_POS);
						}

						if(parameterCache.containsKey(ADHERENCE_ID_POS)) {
							insertStatement.setInt(ADHERENCE_ID_POS, (int) parameterCache.get(ADHERENCE_ID_POS));
						} else {
							positionsNotSet.add(ADHERENCE_ID_POS);
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
		return new StringBuilder("SELECT COUNT(*) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
				.append(".encounter e on o.encounter_id = e.encounter_id AND !o.voided AND !e.voided ")
				.append("WHERE (e.encounter_type = 52 AND o.concept_id = ")
				.append(ENCTYPE_52_VALUEDATETIME_CONCEPT_ID).append(" AND o.value_datetime <= '")
				.append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
				.append("') OR (e.encounter_type = 53 AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS_ENCTYPE_53))
				.append(" AND o.obs_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
				.append("') OR (e.encounter_type IN (6,9,18) AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
				.append(" AND o.obs_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("')").toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder(
		        "SELECT o.obs_id, o.obs_datetime, o.uuid as medication_uuid, o.obs_group_id, o.concept_id, ")
		        .append(
		            "o.value_coded, o.value_datetime, o.encounter_id, e.uuid as encounter_uuid, o.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_type, e.encounter_datetime FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
				.append(".encounter e on o.encounter_id = e.encounter_id AND !o.voided AND !e.voided ")
				.append("WHERE (e.encounter_type = 52 AND o.concept_id = ")
				.append(ENCTYPE_52_VALUEDATETIME_CONCEPT_ID).append(" AND o.value_datetime <= '")
				.append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
		        .append("') OR (e.encounter_type = 53 AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS_ENCTYPE_53))
		        .append(" AND o.obs_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
				.append("') OR (e.encounter_type IN (6,9,18) AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
		        .append(" AND o.obs_datetime <= '").append(Date.valueOf(Mozart2Properties.getInstance().getEndDate()))
				.append("') ORDER BY o.obs_id");
		
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
		insertStatement.setString(MEDICATION_UUID_POS, (String) group.get(MEDICATION_UUID_POS));
		if (group.containsKey(FORMULATION_ID_POS)) {
			insertStatement.setInt(FORMULATION_ID_POS, (int) group.get(FORMULATION_ID_POS));
			positionsNotSet.remove(FORMULATION_ID_POS);
		} else {
			insertStatement.setNull(FORMULATION_ID_POS, Types.INTEGER);
		}
		
		if (group.containsKey(QUANTITY_POS)) {
			insertStatement.setDouble(QUANTITY_POS, (double) group.get(QUANTITY_POS));
			positionsNotSet.remove(QUANTITY_POS);
		} else {
			insertStatement.setNull(QUANTITY_POS, Types.DOUBLE);
		}
		
		if (group.containsKey(DOSAGE_POS)) {
			insertStatement.setString(DOSAGE_POS, (String) group.get(DOSAGE_POS));
			positionsNotSet.remove(DOSAGE_POS);
		} else {
			insertStatement.setNull(DOSAGE_POS, Types.VARCHAR);
		}
	}
}
