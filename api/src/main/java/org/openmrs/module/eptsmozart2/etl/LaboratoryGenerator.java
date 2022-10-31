package org.openmrs.module.eptsmozart2.etl;

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
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class LaboratoryGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "laboratory.sql";

	// Map of encounterId to values related with obs 856 & 1305 for MOZ2-83 implementation
	private Map<Integer, Map<Integer, Object>> eight561305 = new HashMap<>();
	
	public static final Integer[] LAB_CONCEPT_IDS = new Integer[] { 730, 856, 1305, 1695, 5497, 22772, 23896 };

	public static final Integer[] FICHA_CLINICA_LAB_ANSWERS = new Integer[] { 856, 1695 };

	public static final Integer FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID = 23722;

	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 13, 51, 53 };

	public static final Map<Integer, String> CONCEPT_UNITS = new HashMap<>();

	static {
	    CONCEPT_UNITS.put(730, "%");
	    CONCEPT_UNITS.put(856,"copies/ml");
	    CONCEPT_UNITS.put(5497,"cells/dL");
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
                .append(".laboratory (encounter_id, encounter_uuid, encounter_date, encounter_type, patient_id, patient_uuid, ")
                .append("concept_id, concept_name, request, order_date, sample_collection_date, result_report_date, result_qualitative_id, ")
                .append("result_qualitative_name, result_numeric, result_units, result_comment, date_created, source_database, ")
                .append("specimen_type_id, specimen_type, labtest_uuid) ")
                .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

        PreparedStatement orderDateSpecimenStatement = null;
        ResultSet orderDateSpecimenTypeResults = null;
        try {
            if (insertStatement == null) {
                insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
            } else {
                insertStatement.clearParameters();
            }
            int count = 0;
            String orderDateSpecimenQuery = new StringBuilder("SELECT o.*, cn.name as value_coded_name, ")
                    .append("cn.name as value_coded_name FROM ")
                    .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o LEFT JOIN ")
                    .append(Mozart2Properties.getInstance().getDatabaseName())
                    .append(".concept_name cn on cn.concept_id = o.value_coded AND !cn.voided AND cn.locale = 'en' AND ")
                    .append("cn.locale_preferred WHERE !o.voided and o.concept_id IN (6246, 23821, 23832) and encounter_id = ?").toString();

            orderDateSpecimenStatement = ConnectionPool.getConnection().prepareStatement(orderDateSpecimenQuery);
            Set<Integer> positionsNotSet = new HashSet<>();
            while (results.next() && count < batchSize) {
                Integer encounterType = results.getInt("encounter_type");
                Integer conceptId = results.getInt("concept_id");
                Integer encounterId = results.getInt("encounter_id");
                // Cache for MOZ2-83
                if(conceptId == 856 || conceptId == 1305) {
                    Map<Integer, Object> eighty561305Values = new HashMap<>();
                    if(eight561305.containsKey(encounterId)) {
                        eighty561305Values = eight561305.get(encounterId);
                    } else {
                        eight561305.put(encounterId, eighty561305Values);
                    }
                    eighty561305Values.put(-1, insertSql);
                    eighty561305Values.put(2, results.getString("encounter_uuid"));
                    eighty561305Values.put(3, results.getDate("encounter_date"));
                    eighty561305Values.put(4, encounterType);
                    eighty561305Values.put(5, results.getInt("patient_id"));
                    eighty561305Values.put(6, results.getString("patient_uuid"));

                    if(encounterType == 13 || encounterType == 51 || encounterType == 6) {
                        eighty561305Values.put(12, results.getTimestamp("obs_datetime"));
                    }

                    if(conceptId == 856) {
                        eighty561305Values.put(856, results.getString("concept_name"));
                        eighty561305Values.put(15, results.getDouble("value_numeric"));
                        eighty561305Values.put(16, CONCEPT_UNITS.get(conceptId));

                        // Common values
                        eighty561305Values.put(17, results.getString("comments"));
                        eighty561305Values.put(18, results.getTimestamp("date_created"));
                        eighty561305Values.put(19, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
                        eighty561305Values.put(22, results.getString("uuid"));
                    } else if(conceptId == 1305) {
                        eighty561305Values.put(1305, results.getString("concept_name"));
                        eighty561305Values.put(13, results.getInt("value_coded"));
                        eighty561305Values.put(14, results.getString("value_coded_name"));

                        // Common properties between 856 & 1305 (856 takes precedence, i.e. only set if they are not set.
                        if(!eighty561305Values.containsKey(856)) {
                            eighty561305Values.put(17, results.getString("comments"));
                            eighty561305Values.put(18, results.getTimestamp("date_created"));
                            eighty561305Values.put(19, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
                            eighty561305Values.put(22, results.getString("uuid"));
                        }
                    }

                    // Skip the row as it will inserted later as a combined row MOZ2-83
                    continue;
                }

                positionsNotSet.addAll(Arrays.asList(9, 10,11,12,13,14,15,16, 20, 21));
                insertStatement.setInt(1, results.getInt("encounter_id"));
                insertStatement.setString(2, results.getString("encounter_uuid"));
                insertStatement.setDate(3, results.getDate("encounter_date"));
                insertStatement.setInt(4, encounterType);
                insertStatement.setInt(5, results.getInt("patient_id"));
                insertStatement.setString(6, results.getString("patient_uuid"));

                String conceptName = results.getString("concept_name");
                if(conceptName == null) {
                    positionsNotSet.add(8);
                } else {
                    insertStatement.setString(8, conceptName);
                }

                insertStatement.setInt(7, conceptId);
                if(conceptId == 23722) {
                    // request and order_date
                    insertStatement.setInt(7, results.getInt("value_coded"));
                    conceptName = results.getString("value_coded_name");
                    if(conceptName == null) {
                        positionsNotSet.add(8);
                    } else {
                        insertStatement.setString(8, conceptName);
                    }
                    insertStatement.setInt(9, conceptId);
                    insertStatement.setTimestamp(10, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(9);
                    positionsNotSet.remove(10);
                }

                boolean orderResultDateSet = false;
                if(encounterType == 13 || encounterType == 51) {
                    orderDateSpecimenStatement.setInt(1, results.getInt("encounter_id"));
                    orderDateSpecimenTypeResults = orderDateSpecimenStatement.executeQuery();

                    while(orderDateSpecimenTypeResults.next()) {
                        int resultConceptId = orderDateSpecimenTypeResults.getInt("concept_id");
                        if(resultConceptId == 6246) {
                            insertStatement.setDate(10, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(10);
                            orderResultDateSet = true;
                        } else if(resultConceptId == 23821) {
                            //sample_collection_date
                            insertStatement.setDate(11, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(11);
                            orderResultDateSet = true;
                        } else if(resultConceptId == 23832) {
                            positionsNotSet.remove(20);
                            positionsNotSet.remove(21);
                            insertStatement.setInt(20, orderDateSpecimenTypeResults.getInt("value_coded"));
                            insertStatement.setString(21, orderDateSpecimenTypeResults.getString("value_coded_name"));
                        }
                    }
                }

                if(!orderResultDateSet && conceptId != 23722 && (encounterType == 13 || encounterType == 51 || encounterType == 6)) {
                    //result_report_date
                    insertStatement.setTimestamp(12, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(12);
                }

                if(Arrays.asList(1305, 22772).contains(conceptId)) {
                    //13. result_qualitative_id, 14. result_qualitative_name
                    insertStatement.setInt(13, results.getInt("value_coded"));
                    positionsNotSet.remove(13);
                    conceptName = results.getString("value_coded_name");
                    if(conceptName != null) {
                        insertStatement.setString(14, conceptName);
                        positionsNotSet.remove(14);
                    }

                }

                if(Arrays.asList(5497, 23896, 1695, 856, 730).contains(conceptId)) {
                    insertStatement.setDouble(15, results.getDouble("value_numeric"));
                    positionsNotSet.remove(15);

                    if(conceptId != 1695 && conceptId != 23896) {
                        insertStatement.setString(16, CONCEPT_UNITS.get(conceptId));
                        positionsNotSet.remove(16);
                    }
                }

                insertStatement.setString(17, results.getString("comments"));
                insertStatement.setTimestamp(18, results.getTimestamp("date_created"));
                insertStatement.setString(19, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
                insertStatement.setString(22, results.getString("uuid"));

                setEmptyPositions(positionsNotSet);
                insertStatement.addBatch();
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
            if(orderDateSpecimenTypeResults != null) {
                orderDateSpecimenTypeResults.close();
            }
            if(orderDateSpecimenStatement != null) {
                orderDateSpecimenStatement.close();
            }
        }
    }
	
	@Override
	public String getTable() {
		return "laboratory";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".patient p ON o.person_id = p.patient_id JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND ((o.concept_id = ")
                .append(FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID)
                .append(" AND o.value_coded IN ").append(inClause(FICHA_CLINICA_LAB_ANSWERS)).append(") OR o.concept_id IN ")
                .append(inClause(LAB_CONCEPT_IDS)).append(") AND o.obs_datetime <= '")
                .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("'");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT o.*, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, o.person_id as patient_id, p.patient_uuid, ")
		        .append("e.encounter_datetime as encounter_date, cn.name as concept_name, cn1.name as value_coded_name FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p ON o.person_id = p.patient_id JOIN ")
                .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ").append(inClause(ENCOUNTER_TYPE_IDS))
                .append(" LEFT JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".concept_name cn on cn.concept_id = o.concept_id AND !cn.voided AND cn.locale = 'en' AND ")
		        .append("cn.locale_preferred LEFT JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".concept_name cn1 on cn1.concept_id = o.value_coded AND !cn1.voided AND cn1.locale = 'en' AND ")
		        .append("cn1.locale_preferred  WHERE !o.voided AND ((o.concept_id = ").append(FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID)
                .append(" AND o.value_coded IN ").append(inClause(FICHA_CLINICA_LAB_ANSWERS)).append(") OR o.concept_id IN ")
                .append(inClause(LAB_CONCEPT_IDS)).append(") AND o.obs_datetime <= '")
                .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' ORDER BY o.obs_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}

    @Override
    public Void call() throws SQLException, IOException {
        ResultSet resultSet = null;
        long startTime = System.currentTimeMillis();
        currentlyGenerated = 0;
        try (Connection connection = ConnectionPool.getConnection(); Statement statement = connection.createStatement()) {
            createTable();
            resultSet = statement.executeQuery(countQuery());
            resultSet.next();
            toBeGenerated = resultSet.getInt(1);
            resultSet.close();
            int batchSize = Mozart2Properties.getInstance().getBatchSize();
            if(toBeGenerated > batchSize) {
                LOGGER.debug("Generating {} records for table {} in batches of {}", toBeGenerated, getTable(), batchSize);
                int temp = toBeGenerated;
                int start = 0;
                int batchCount = 1;
                while (temp % batchSize > 0) {
                    if (temp / batchSize > 0) {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                     batchCount++, getTable(), currentlyGenerated, batchSize, toBeGenerated - currentlyGenerated);
                        etl(start, batchSize);
                        temp -= batchSize;
                        start += batchSize;
                        int eight561305Count = etl856_1305(false);
                        currentlyGenerated += batchSize + eight561305Count;
                    } else {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                     batchCount++, getTable(), currentlyGenerated, temp, toBeGenerated - currentlyGenerated);
                        etl(start, temp);
                        int eight561305Count = etl856_1305(true);
                        currentlyGenerated += temp + eight561305Count;
                        temp = 0;
                    }
                }
            } else {
                // few records to move.
                LOGGER.debug("Running ETL for {}", getTable());
                int[] inserted = etl(null, null);
                int eight561305Count = etl856_1305(true);
                currentlyGenerated += toBeGenerated + eight561305Count;
            }

            LOGGER.debug("Done inserting {} records for table {}", toBeGenerated, getTable());
            return null;
        } catch (Exception e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertStatement.getParameterMetaData().getParameterCount(), e);
            this.setChanged();
            Utils.notifyObserversAboutException(this, e);
            throw e;
        } finally {
            if(resultSet != null) {
                try {
                    resultSet.getStatement().close();
                    resultSet.close();
                } catch (SQLException e){}
            }

            if(insertStatement != null) {
                Connection connection = insertStatement.getConnection();
                if(connection != null) {
                    connection.close();
                }
                insertStatement.close();
            }

            if(selectStatement != null) {
                Connection connection = selectStatement.getConnection();
                if(connection != null) connection.close();
                selectStatement.close();
            }

            LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
        }
    }

    private int etl856_1305(boolean lastIteration) throws SQLException {
        if(!eight561305.isEmpty()) {
            String insertSql = (String) eight561305.values().iterator().next().get(-1);
            try {
                if (insertStatement == null) {
                    insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
                } else {
                    insertStatement.clearParameters();
                }
                boolean runInsert = false;
                int rowsToInserted = 0;
                Iterator<Map.Entry<Integer, Map<Integer, Object>>> entriesIterator = eight561305.entrySet().iterator();
                while(entriesIterator.hasNext()) {
                    Map.Entry<Integer, Map<Integer, Object>> entry = entriesIterator.next();
                    Set<Integer> emptyPositions = new HashSet<>(Arrays.asList(9, 10,11, 20, 21));
                    Map<Integer, Object> enc8561305 = entry.getValue();
                    if(!(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) && !lastIteration) continue;
                    Integer encounterId = entry.getKey();
                    int encounterType = (Integer) enc8561305.get(4);
                    insertStatement.setInt(1, encounterId);
                    insertStatement.setString(2, (String) enc8561305.get(2));
                    insertStatement.setDate(3, (Date) enc8561305.get(3));
                    insertStatement.setInt(4, encounterType);
                    insertStatement.setInt(5, (Integer) enc8561305.get(5));
                    insertStatement.setString(6, (String) enc8561305.get(6));
                    insertStatement.setInt(7, 856);


                    if(encounterType == 13 || encounterType == 51 || encounterType == 6) {
                        insertStatement.setTimestamp(12, (Timestamp) enc8561305.get(12));
                    } else {
                        emptyPositions.add(12);
                    }

                    if(enc8561305.get(17) == null) {
                        emptyPositions.add(17);
                    } else {
                        insertStatement.setString(17, (String) enc8561305.get(17));
                    }
                    
                    insertStatement.setTimestamp(18, (Timestamp) enc8561305.get(18));
                    insertStatement.setString(19, (String) enc8561305.get(19));
                    insertStatement.setString(22, (String) enc8561305.get(22));

                    if(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) {
                        //Combined
                        if(enc8561305.get(856) != null) {
                            insertStatement.setString(8, (String) enc8561305.get(856));
                        } else {
                            emptyPositions.add(8);
                        }

                        insertStatement.setInt(13, (Integer) enc8561305.get(13));
                        if(enc8561305.get(14) != null) {
                            insertStatement.setString(14, (String) enc8561305.get(14));
                        } else {
                            emptyPositions.add(14);
                        }

                        insertStatement.setDouble(15, (Double) enc8561305.get(15));
                        insertStatement.setString(16, (String) enc8561305.get(16));
                        entriesIterator.remove();
                        runInsert = true;
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        toBeGenerated--;
                        currentlyGenerated--;
                    } else if(enc8561305.containsKey(856) && lastIteration) {
                        if(enc8561305.get(856) != null) {
                            insertStatement.setString(8, (String) enc8561305.get(856));
                        } else {
                            emptyPositions.add(8);
                        }

                        insertStatement.setDouble(15, (Double) enc8561305.get(15));
                        insertStatement.setString(16, (String) enc8561305.get(16));
                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(13);
                        emptyPositions.add(14);
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        currentlyGenerated--;
                    } else if(enc8561305.containsKey(1305) && lastIteration) {
                        if(enc8561305.get(1305) != null) {
                            insertStatement.setString(8, (String) enc8561305.get(1305));
                        } else {
                            emptyPositions.add(8);
                        }

                        insertStatement.setInt(13, (Integer) enc8561305.get(13));
                        if(enc8561305.get(14) != null) {
                            insertStatement.setString(14, (String) enc8561305.get(14));
                        } else {
                            emptyPositions.add(14);
                        }

                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(15);
                        emptyPositions.add(16);
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        currentlyGenerated--;
                    }
                }
                if(runInsert) {
                    insertStatement.executeBatch();
                    return rowsToInserted;
                }
                return 0;
            }
            catch (SQLException e) {
                LOGGER.error("Error while running insert statement for table {}", getTable());
                this.setChanged();
                Utils.notifyObserversAboutException(this, e);
                throw e;
            }
        }
        return 0;
    }

    private void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case 15:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case 10:
					case 11:
					case 12:
						insertStatement.setNull(pos, Types.DATE);
						break;
                    case 8:
					case 9:
					case 14:
						insertStatement.setNull(pos, Types.VARCHAR);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
