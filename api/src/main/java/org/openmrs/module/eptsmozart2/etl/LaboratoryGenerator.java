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
                .append(".laboratory (encounter_uuid, ")
                .append("lab_test_id, request, order_date, sample_collection_date, result_report_date, result_qualitative_id, ")
                .append("result_numeric, result_units, result_comment, ")
                .append("specimen_type_id, labtest_uuid) ")
                .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

        PreparedStatement orderDateSpecimenStatement = null;
        ResultSet orderDateSpecimenTypeResults = null;
        try {
            if (insertStatement == null) {
                insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
            } else {
                insertStatement.clearParameters();
            }
            int count = 0;
            String orderDateSpecimenQuery = new StringBuilder("SELECT * FROM ")
                    .append(Mozart2Properties.getInstance().getDatabaseName())
                    .append(".obs WHERE !voided and concept_id IN (6246, 23821, 23832) and encounter_id = ?")
                    .toString();

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
                    eighty561305Values.put(-2, encounterType);
                    eighty561305Values.put(1, results.getString("encounter_uuid"));


                    if(encounterType == 13 || encounterType == 51 || encounterType == 6) {
                        eighty561305Values.put(6, results.getTimestamp("obs_datetime"));
                    }

                    if(conceptId == 856) {
                        eighty561305Values.put(856, "856");
                        eighty561305Values.put(8, results.getDouble("value_numeric"));
                        eighty561305Values.put(9, CONCEPT_UNITS.get(conceptId));

                        // Common values
                        eighty561305Values.put(10, results.getString("comments"));
                        eighty561305Values.put(12, results.getString("uuid"));
                    } else if(conceptId == 1305) {
                        eighty561305Values.put(1305, "1305");
                        eighty561305Values.put(7, results.getInt("value_coded"));

                        // Common properties between 856 & 1305 (856 takes precedence, i.e. only set if they are not set.
                        if(!eighty561305Values.containsKey(856)) {
                            eighty561305Values.put(10, results.getString("comments"));
                            eighty561305Values.put(12, results.getString("uuid"));
                        }
                    }

                    // Skip the row as it will inserted later as a combined row MOZ2-83
                    continue;
                }

                positionsNotSet.addAll(Arrays.asList(3, 4,5,6,7, 8,9, 11));
                insertStatement.setString(1, results.getString("encounter_uuid"));

                insertStatement.setInt(2, conceptId);
                if(conceptId == 23722) {
                    // request and order_date
                    insertStatement.setInt(2, results.getInt("value_coded"));
                    insertStatement.setInt(3, conceptId);
                    insertStatement.setTimestamp(4, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(3);
                    positionsNotSet.remove(4);
                }

                boolean orderResultDateSet = false;
                if(encounterType == 13 || encounterType == 51) {
                    orderDateSpecimenStatement.setInt(1, results.getInt("encounter_id"));
                    orderDateSpecimenTypeResults = orderDateSpecimenStatement.executeQuery();

                    while(orderDateSpecimenTypeResults.next()) {
                        int resultConceptId = orderDateSpecimenTypeResults.getInt("concept_id");
                        if(resultConceptId == 6246) {
                            insertStatement.setDate(4, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(4);
                            orderResultDateSet = true;
                        } else if(resultConceptId == 23821) {
                            //sample_collection_date
                            insertStatement.setDate(5, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(5);
                            orderResultDateSet = true;
                        } else if(resultConceptId == 23832) {
                            positionsNotSet.remove(11);
                            insertStatement.setInt(11, orderDateSpecimenTypeResults.getInt("value_coded"));
                        }
                    }
                }

                if(!orderResultDateSet && conceptId != 23722 && (encounterType == 13 || encounterType == 51 || encounterType == 6)) {
                    //result_report_date
                    insertStatement.setTimestamp(6, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(6);
                }

                if(Arrays.asList(1305, 22772).contains(conceptId)) {
                    //7. result_qualitative_id
                    insertStatement.setInt(7, results.getInt("value_coded"));
                    positionsNotSet.remove(7);
                }

                if(Arrays.asList(5497, 23896, 1695, 856, 730).contains(conceptId)) {
                    insertStatement.setDouble(8, results.getDouble("value_numeric"));
                    positionsNotSet.remove(8);

                    if(conceptId != 1695 && conceptId != 23896) {
                        insertStatement.setString(9, CONCEPT_UNITS.get(conceptId));
                        positionsNotSet.remove(9);
                    }
                }

                insertStatement.setString(10, results.getString("comments"));
                insertStatement.setString(12, results.getString("uuid"));

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
		StringBuilder sb = new StringBuilder("SELECT o.*, e.encounter_type, e.uuid as encounter_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName()).append(".patient p ON o.person_id = p.patient_id JOIN ")
                .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
                .append(inClause(ENCOUNTER_TYPE_IDS)).append(" WHERE !o.voided AND ((o.concept_id = ")
                .append(FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID)
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
                    Set<Integer> emptyPositions = new HashSet<>(Arrays.asList(3, 4,5, 11));
                    Map<Integer, Object> enc8561305 = entry.getValue();
                    if(!(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) && !lastIteration) continue;
                    int encounterType = (Integer) enc8561305.get(-2);
                    insertStatement.setString(1, (String) enc8561305.get(1));
                    insertStatement.setInt(2, 856);


                    if(encounterType == 13 || encounterType == 51 || encounterType == 6) {
                        insertStatement.setTimestamp(6, (Timestamp) enc8561305.get(6));
                    } else {
                        emptyPositions.add(6);
                    }

                    if(enc8561305.get(10) == null) {
                        emptyPositions.add(10);
                    } else {
                        insertStatement.setString(10, (String) enc8561305.get(10));
                    }
                    
                    insertStatement.setString(11, (String) enc8561305.get(11));
                    insertStatement.setString(12, (String) enc8561305.get(12));

                    if(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) {
                        //Combined
                        insertStatement.setInt(7, (Integer) enc8561305.get(7));

                        insertStatement.setDouble(8, (Double) enc8561305.get(8));
                        insertStatement.setString(9, (String) enc8561305.get(9));
                        entriesIterator.remove();
                        runInsert = true;
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        toBeGenerated--;
                        currentlyGenerated--;
                    } else if(enc8561305.containsKey(856) && lastIteration) {
                        insertStatement.setDouble(8, (Double) enc8561305.get(8));
                        insertStatement.setString(9, (String) enc8561305.get(9));
                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(7);
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        currentlyGenerated--;
                    } else if(enc8561305.containsKey(1305) && lastIteration) {
                        insertStatement.setInt(2, 1305);
                        insertStatement.setInt(7, (Integer) enc8561305.get(7));
                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(8);
                        emptyPositions.add(9);
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
					case 8:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case 4:
					case 5:
					case 6:
						insertStatement.setNull(pos, Types.DATE);
						break;
					case 3:
						insertStatement.setNull(pos, Types.VARCHAR);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
