package org.openmrs.module.eptsmozart2.etl;

import org.apache.commons.collections.map.HashedMap;
import org.openmrs.module.eptsmozart2.AppProperties;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/15/22.
 */
public class MedicationsTableGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "medications.sql";
	
	private static final Integer[] REGIMEN_CONCEPT_IDS = new Integer[] { 1087, 1088, 21187, 21188, 21190, 23893 };
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 18, 53 };
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException {
		return prepareInsertStatement(resultSet, null);
	}
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;
		String insertSql = new StringBuilder("INSERT INTO ")
		        .append(AppProperties.getInstance().getNewDatabaseName())
		        .append(".medications (encounter_id, encounter_uuid, patient_id, patient_uuid, encounter_date, regimen, regimen_concept, ")
		        .append("formulation, formulation_concept, formulation_drug, quantity, dosage, next_pickup_date, ")
				.append("mode_dispensation, mode_dispensation_concept, med_line, med_line_concept, type_dispensation, type_dispensation_concept, ")
		        .append("alternative_line, alternative_line_concept, regimen_change_reason, regimen_change_reason_concept, ")
				.append("arv_side_effects, arv_side_effects_concept, adherence, adherence_concept, source_database, regimen_id, medication_uuid) ")
		        .append("VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

		Set<Integer> positionsNotSet = new HashSet<>();
		positionsNotSet.addAll(Arrays.asList(8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27));
		PreparedStatement fetchMedsDetailsStatement = null;
		ResultSet medObsResults = null;
		try (Connection connection = ConnectionPool.getConnection()) {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			String fetchMoreObsSql = new StringBuilder("SELECT o.*, d.name as formulation, d.combination, cn.name as concept_name ")
					.append("FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".obs o LEFT JOIN ")
					.append(AppProperties.getInstance().getDatabaseName()).append(".drug d ON o.value_drug = d.drug_id LEFT JOIN ")
					.append(AppProperties.getInstance().getDatabaseName()).append(".concept_name cn ON o.value_coded = cn.concept_id ")
					.append("AND !cn.voided AND cn.locale='en' AND cn.locale_preferred ")
					.append("WHERE o.encounter_id = ? AND o.concept_id IN (1711, 1715, 165256, 23740, 5096, 165174, 21151, 23742, 21190, 21187,")
					.append("21188, 23739, 23742, 1792, 2015, 6223)").toString();
			
			fetchMedsDetailsStatement = connection.prepareStatement(fetchMoreObsSql);
			int count = 0;
			Map<Integer, Object> parameterCache = new HashedMap();
			while (results.next() && count < batchSize) {
				parameterCache.clear();
				int currentEncounterId = results.getInt("encounter_id");
				insertStatement.setInt(1, currentEncounterId);
				insertStatement.setString(2, results.getString("encounter_uuid"));
				insertStatement.setInt(3, results.getInt("patient_id"));
				insertStatement.setString(4, results.getString("patient_uuid"));

				parameterCache.put(5, results.getDate("encounter_date"));
				insertStatement.setDate(5, results.getDate("encounter_date"));

				parameterCache.put(6, results.getString("regimen"));
				insertStatement.setString(6, results.getString("regimen"));
				insertStatement.setInt(7, results.getInt("value_coded"));

				insertStatement.setString(28, AppProperties.getInstance().getDatabaseName());
				insertStatement.setInt(29, results.getInt("concept_id"));
				insertStatement.setString(30, results.getString("medication_uuid"));

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
								formulationsDosagesQuantities.get(obsGrouper).put(8, medObsResults.getString("formulation"));
								formulationsDosagesQuantities.get(obsGrouper).put(9,  medObsResults.getInt("value_coded"));
								formulationsDosagesQuantities.get(obsGrouper).put(10, medObsResults.getInt("value_drug"));
							} else {
								insertStatement.setString(8, medObsResults.getString("formulation"));
								insertStatement.setInt(9, medObsResults.getInt("value_coded"));
								insertStatement.setInt(10, medObsResults.getInt("value_drug"));
								positionsNotSet.removeAll(Arrays.asList(8, 9, 10));
							}
							break;
						case 1715:case 23740:
							// quantity
							final Integer QUANTITY_POS = 11;
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
							final Integer DOSAGE_POS = 12;
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
							insertStatement.setDate(13, medObsResults.getDate("value_datetime"));
							parameterCache.put(13, medObsResults.getDate("value_datetime"));
							positionsNotSet.remove(13);
							break;
						case 165174:
							// mode_dispensation
							parameterCache.put(14, medObsResults.getString("concept_name"));
							insertStatement.setString(14, medObsResults.getString("concept_name"));

							parameterCache.put(15, medObsResults.getInt("value_coded"));
							insertStatement.setInt(15, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(14, 15));
							break;
						case 21151:case 23893:case 21190:case 21187:case 21188:
							// med_line
							parameterCache.put(16, medObsResults.getString("concept_name"));
							insertStatement.setString(16, medObsResults.getString("concept_name"));

							parameterCache.put(17, medObsResults.getInt("value_coded"));
							insertStatement.setInt(17, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(16, 17));
							break;
						case 23739:
							// type_dispensation
							parameterCache.put(18, medObsResults.getString("concept_name"));
							insertStatement.setString(18, medObsResults.getString("concept_name"));

							parameterCache.put(19, medObsResults.getInt("value_coded"));
							insertStatement.setInt(19, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(18, 19));
							break;
						case 23742:
							// alternative_line
							parameterCache.put(20, medObsResults.getString("concept_name"));
							insertStatement.setString(20, medObsResults.getString("concept_name"));

							parameterCache.put(21, medObsResults.getInt("value_coded"));
							insertStatement.setInt(21, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(20, 21));
							break;
						case 1792:
							// reason_change_regimen
							parameterCache.put(22, medObsResults.getString("concept_name"));
							insertStatement.setString(22, medObsResults.getString("concept_name"));
							parameterCache.put(23, medObsResults.getInt("value_coded"));
							insertStatement.setInt(23, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(22, 23));
							break;
						case 2015:
							// arv_side_effects
							parameterCache.put(24, medObsResults.getString("concept_name"));
							insertStatement.setString(24, medObsResults.getString("concept_name"));
							parameterCache.put(25, medObsResults.getInt("value_coded"));
							insertStatement.setInt(25, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(24, 25));
							break;
						case 6223:
							// adherence
							parameterCache.put(26, medObsResults.getString("concept_name"));
							insertStatement.setString(26, medObsResults.getString("concept_name"));
							parameterCache.put(27, medObsResults.getInt("value_coded"));
							insertStatement.setInt(27, medObsResults.getInt("value_coded"));
							positionsNotSet.removeAll(Arrays.asList(26, 27));
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
						insertStatement.setInt(1, currentEncounterId);
						insertStatement.setString(2, results.getString("encounter_uuid"));
						insertStatement.setInt(3, results.getInt("patient_id"));
						insertStatement.setString(4, results.getString("patient_uuid"));
						insertStatement.setDate(5, results.getDate("encounter_date"));
						insertStatement.setString(6, results.getString("regimen"));
						insertStatement.setInt(7, results.getInt("value_coded"));
						insertStatement.setInt(29, results.getInt("concept_id"));
						insertStatement.setString(28, AppProperties.getInstance().getDatabaseName());

						if(parameterCache.containsKey(13)) {
							insertStatement.setDate(13, (Date) parameterCache.get(13));
						} else {
							positionsNotSet.add(13);
						}

						if(parameterCache.containsKey(14)) {
							insertStatement.setString(14, (String) parameterCache.get(14));
						} else {
							positionsNotSet.add(14);
						}

						if(parameterCache.containsKey(15)) {
							insertStatement.setInt(15, (int) parameterCache.get(15));
						} else {
							positionsNotSet.add(15);
						}

						if(parameterCache.containsKey(16)) {
							insertStatement.setString(16, (String) parameterCache.get(16));
						} else {
							positionsNotSet.add(16);
						}

						if(parameterCache.containsKey(17)) {
							insertStatement.setInt(17, (int) parameterCache.get(17));
						} else {
							positionsNotSet.add(17);
						}

						if(parameterCache.containsKey(18)) {
							insertStatement.setString(18, (String) parameterCache.get(18));
						} else {
							positionsNotSet.add(18);
						}

						if(parameterCache.containsKey(19)) {
							insertStatement.setInt(19, (int) parameterCache.get(19));
						} else {
							positionsNotSet.add(19);
						}

						if(parameterCache.containsKey(20)) {
							insertStatement.setString(20, (String) parameterCache.get(20));
						} else {
							positionsNotSet.add(20);
						}

						if(parameterCache.containsKey(21)) {
							insertStatement.setInt(21, (int) parameterCache.get(21));
						} else {
							positionsNotSet.add(21);
						}

						if(parameterCache.containsKey(22)) {
							insertStatement.setString(22, (String) parameterCache.get(22));
						} else {
							positionsNotSet.add(22);
						}

						if(parameterCache.containsKey(23)) {
							insertStatement.setInt(23, (int) parameterCache.get(23));
						} else {
							positionsNotSet.add(23);
						}

						if(parameterCache.containsKey(24)) {
							insertStatement.setString(24, (String) parameterCache.get(24));
						} else {
							positionsNotSet.add(24);
						}

						if(parameterCache.containsKey(25)) {
							insertStatement.setInt(25, (int) parameterCache.get(25));
						} else {
							positionsNotSet.add(25);
						}

						if(parameterCache.containsKey(26)) {
							insertStatement.setString(26, (String) parameterCache.get(26));
						} else {
							positionsNotSet.add(26);
						}

						if(parameterCache.containsKey(27)) {
							insertStatement.setInt(27, (int) parameterCache.get(27));
						} else {
							positionsNotSet.add(27);
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
		return "medications";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(AppProperties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN (")
		        .append(AppProperties.getInstance().getLocationsIdsString()).append(")")
		        .append(" WHERE !o.voided AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
		        .append(" AND o.obs_datetime <= '").append(Date.valueOf(AppProperties.getInstance().getEndDate()))
		        .append("'");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT o.obs_id, o.uuid as medication_uuid, o.obs_group_id, o.concept_id, ")
		        .append("o.value_coded, o.encounter_id, e.uuid as encounter_uuid, o.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_datetime as encounter_date, cn.name as regimen FROM ")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(AppProperties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN (")
		        .append(AppProperties.getInstance().getLocationsIdsString()).append(") JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".concept_name cn on cn.concept_id = o.value_coded AND !cn.voided AND cn.locale = 'en' AND ")
		        .append("cn.locale_preferred  WHERE !o.voided AND o.concept_id IN ").append(inClause(REGIMEN_CONCEPT_IDS))
		        .append(" AND o.obs_datetime <= '").append(Date.valueOf(AppProperties.getInstance().getEndDate()))
		        .append("' ORDER BY o.obs_id");
		
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
					case 11:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case 13:
						insertStatement.setNull(pos, Types.DATE);
						break;
					case 8:
					case 12:
					case 14:
					case 16:
					case 18:
					case 20:
					case 22:
					case 24:
					case 26:
						insertStatement.setNull(pos, Types.VARCHAR);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
	
	private void setFormulationDosageQuantity(Map<Integer, Object> group, Set<Integer> positionsNotSet) throws SQLException {
		if (group.containsKey(8)) {
			insertStatement.setString(8, (String) group.get(8));
			insertStatement.setInt(9, (int) group.get(9));
			insertStatement.setInt(10, (int) group.get(10));
			positionsNotSet.removeAll(Arrays.asList(8, 9, 10));
		} else {
			insertStatement.setNull(8, Types.VARCHAR);
			insertStatement.setNull(9, Types.INTEGER);
			insertStatement.setNull(10, Types.INTEGER);
		}
		
		if (group.containsKey(11)) {
			insertStatement.setDouble(11, (double) group.get(11));
			positionsNotSet.remove(11);
		} else {
			insertStatement.setNull(11, Types.DOUBLE);
		}
		
		if (group.containsKey(12)) {
			insertStatement.setString(12, (String) group.get(12));
			positionsNotSet.remove(12);
		} else {
			insertStatement.setNull(12, Types.VARCHAR);
		}
	}
}
