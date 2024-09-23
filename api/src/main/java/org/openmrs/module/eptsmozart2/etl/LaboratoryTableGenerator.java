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
public class LaboratoryTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(LaboratoryTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "laboratory.sql";

	// Map of encounterId to values related with obs 856 & 1305 for MOZ2-83 implementation
	private Map<Integer, Map<Integer, Object>> eight561305 = new HashMap<>();
	
	public static final Integer[] LAB_CONCEPT_IDS = new Integer[] {
	        730, 856, 1305, 1695, 5497, 22772, 23896, 1692, 21, 1694, 654, 1693,
            653, 887, 1011, 790, 1299, 23723, 23774, 23951, 23952, 307, 12, 2094
    };

	public static final List<Integer> VALUE_NUMERIC_CONCEPT_IDS = Arrays.asList(new Integer[]{
            5497, 23896, 1695, 730, 1692, 21, 1694,654,1693, 653, 887, 1011, 790, 1299
    });

	public static final int POSITIVITY_LEVEL_CONCEPT_ID = 165185;

	public static final int POSITIVE_CONCEPT_ID = 703;

	public static final List<Integer> VALUE_CODED_CONCEPT_IDS = Arrays.asList(23723, 23774, 23951, 23952, 307, 12, 2094);

	public static final Integer[] FICHA_CLINICA_LAB_ANSWERS = new Integer[] { 856, 1695 };

	public static final Integer FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID = 23722;

	public static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 13, 51, 53, 90 };

	public static final Map<Integer, String> CONCEPT_UNITS = new HashMap<>();

	public static final Map<Integer, String> POSITIVITY_LEVELS = new HashMap<>();

	protected final int ENCOUNTER_UUID_POS = 1;
	protected final int LAB_TEST_ID_POS = 2;
	protected final int REQUEST_POS = 3;
	protected final int ORDER_DATE_POS = 4;
	protected final int SAMPLE_COLL_DATE_POS = 5;
	protected final int RESULT_REPORT_DATE_POS = 6;
	protected final int RESULT_QUAL_POS = 7;
	protected final int RESULT_NUM_POS = 8;
	protected final int RESULT_UNITS_POS = 9;
	protected final int RESULT_COMMENT_POS = 10;
	protected final int SPECIMEN_TYPE_POS = 11;
	protected final int LABTEST_UUID_POS = 12;
	protected final int ENCOUNTER_DATE_POS = 13;
    protected final int ENC_TYPE_POS = 14;

    protected final int ENC_CREATED_DATE_POS = 15;

    protected final int ENC_CHANGE_DATE_POS = 16;
    protected final int FORM_ID_POS = 17;
    protected final int PATIENT_UUID_POS = 18;
    protected final int LOC_UUID_POS = 19;
    protected final int SRC_DB_POS = 20;

	static {
	    CONCEPT_UNITS.put(730, "%");
	    CONCEPT_UNITS.put(856,"copies/ml");
	    CONCEPT_UNITS.put(5497,"cells/dL");
	    CONCEPT_UNITS.put(21,"g/dL");
	    CONCEPT_UNITS.put(653,"U/L");
	    CONCEPT_UNITS.put(654,"U/L");
	    CONCEPT_UNITS.put(1011,"U/L");
	    CONCEPT_UNITS.put(1299,"U/L");
	    CONCEPT_UNITS.put(790,"Ummol/L");
	    CONCEPT_UNITS.put(887,"Ummol/L");

	    POSITIVITY_LEVELS.put(165186, "1+");
	    POSITIVITY_LEVELS.put(165187, "2+");
	    POSITIVITY_LEVELS.put(165188, "3+");
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
                .append("specimen_type_id, labtest_uuid, encounter_date, ")
                .append("encounter_type, encounter_created_date, encounter_change_date, form_id, patient_uuid, location_uuid, source_database) ")
                .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

        PreparedStatement orderDateSpecimenStatement = null;
        ResultSet orderDateSpecimenTypeResults = null;
        PreparedStatement positivityStatement = null;
        ResultSet positivityStatementResults = null;
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

            String positivityQuery = new StringBuilder("SELECT * FROM ")
                    .append(Mozart2Properties.getInstance().getDatabaseName())
                    .append(".obs WHERE !voided and concept_id = ").append(POSITIVITY_LEVEL_CONCEPT_ID)
                    .append(" AND encounter_id = ?").toString();

            orderDateSpecimenStatement = ConnectionPool.getConnection().prepareStatement(orderDateSpecimenQuery);
            positivityStatement = ConnectionPool.getConnection().prepareStatement(positivityQuery);
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
                    eighty561305Values.put(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
                    eighty561305Values.put(ENCOUNTER_DATE_POS, results.getTimestamp("encounter_datetime"));
                    eighty561305Values.put(ENC_TYPE_POS, results.getInt("encounter_type"));
                    eighty561305Values.put(ENC_CREATED_DATE_POS, results.getTimestamp("e_date_created"));
                    eighty561305Values.put(ENC_CHANGE_DATE_POS, results.getTimestamp("e_date_changed"));
                    eighty561305Values.put(FORM_ID_POS, results.getInt("form_id"));
                    eighty561305Values.put(PATIENT_UUID_POS, results.getString("patient_uuid"));
                    eighty561305Values.put(LOC_UUID_POS,
                            Mozart2Properties.getInstance().getLocationUuidById(results.getInt("location_id")));

                    if(Arrays.asList(6, 9, 13, 51, 53).contains(encounterType)) {
                        eighty561305Values.put(RESULT_REPORT_DATE_POS, results.getTimestamp("obs_datetime"));
                    }

                    if(encounterType == 13 || encounterType == 51) {
                        orderDateSpecimenStatement.setInt(1, encounterId);
                        orderDateSpecimenTypeResults = orderDateSpecimenStatement.executeQuery();
                        while(orderDateSpecimenTypeResults.next()) {
                            int resultConceptId = orderDateSpecimenTypeResults.getInt("concept_id");
                            if(resultConceptId == 6246) {
                                // result_report_date
                                eighty561305Values.put(ORDER_DATE_POS, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            } else if(resultConceptId == 23821) {
                                //sample_collection_date
                                eighty561305Values.put(SAMPLE_COLL_DATE_POS, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            } else if(resultConceptId == 23832) {
                                //specimen_type_id
                                eighty561305Values.put(SPECIMEN_TYPE_POS, orderDateSpecimenTypeResults.getInt("value_coded"));
                            }
                        }
                    }

                    if(conceptId == 856) {
                        eighty561305Values.put(856, "856");
                        eighty561305Values.put(RESULT_NUM_POS, results.getDouble("value_numeric"));
                        eighty561305Values.put(RESULT_UNITS_POS, CONCEPT_UNITS.get(conceptId));

                        // Common values
                        eighty561305Values.put(RESULT_COMMENT_POS, results.getString("comments"));
                        eighty561305Values.put(LABTEST_UUID_POS, results.getString("obs_uuid"));
                    } else if(conceptId == 1305) {
                        eighty561305Values.put(1305, "1305");
                        eighty561305Values.put(RESULT_QUAL_POS, results.getInt("value_coded"));

                        // Common properties between 856 & 1305 (856 takes precedence, i.e. only set if they are not set.
                        if(!eighty561305Values.containsKey(856)) {
                            eighty561305Values.put(RESULT_COMMENT_POS, results.getString("comments"));
                            eighty561305Values.put(LABTEST_UUID_POS, results.getString("obs_uuid"));
                        }
                    }

                    // Skip the row as it will inserted later as a combined row MOZ2-83
                    continue;
                }

                positionsNotSet.addAll(Arrays.asList(REQUEST_POS, ORDER_DATE_POS, SAMPLE_COLL_DATE_POS,
                                                     RESULT_REPORT_DATE_POS, RESULT_QUAL_POS,
                                                     RESULT_NUM_POS, RESULT_UNITS_POS, SPECIMEN_TYPE_POS)
                );
                
                insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
                insertStatement.setTimestamp(ENCOUNTER_DATE_POS, results.getTimestamp("encounter_datetime"));
                insertStatement.setInt(ENC_TYPE_POS, results.getInt("encounter_type"));
                insertStatement.setTimestamp(ENC_CREATED_DATE_POS, results.getTimestamp("e_date_created"));
                insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, results.getTimestamp("e_date_changed"));
                insertStatement.setInt(FORM_ID_POS, results.getInt("form_id"));
                insertStatement.setString(PATIENT_UUID_POS, results.getString("patient_uuid"));
                insertStatement.setString(LOC_UUID_POS,
                        Mozart2Properties.getInstance().getLocationUuidById(results.getInt("location_id")));
                insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
                insertStatement.setInt(LAB_TEST_ID_POS, conceptId);
                insertStatement.setString(RESULT_COMMENT_POS, results.getString("comments"));
                if(conceptId == 23722) {
                    // request and order_date
                    if(encounterType == 6) {
                        insertStatement.setInt(LAB_TEST_ID_POS, results.getInt("value_coded"));
                    }
                    insertStatement.setInt(REQUEST_POS, conceptId);
                    insertStatement.setTimestamp(ORDER_DATE_POS, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(REQUEST_POS);
                    positionsNotSet.remove(ORDER_DATE_POS);
                }

                if(encounterType == 13 || encounterType == 51) {
                    orderDateSpecimenStatement.setInt(1, encounterId);
                    orderDateSpecimenTypeResults = orderDateSpecimenStatement.executeQuery();

                    while(orderDateSpecimenTypeResults.next()) {
                        int resultConceptId = orderDateSpecimenTypeResults.getInt("concept_id");
                        if(resultConceptId == 6246) {
                            insertStatement.setDate(ORDER_DATE_POS, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(ORDER_DATE_POS);
                        } else if(resultConceptId == 23821) {
                            //sample_collection_date
                            insertStatement.setDate(SAMPLE_COLL_DATE_POS, orderDateSpecimenTypeResults.getDate("value_datetime"));
                            positionsNotSet.remove(SAMPLE_COLL_DATE_POS);
                        } else if(resultConceptId == 23832) {
                            positionsNotSet.remove(SPECIMEN_TYPE_POS);
                            insertStatement.setInt(SPECIMEN_TYPE_POS, orderDateSpecimenTypeResults.getInt("value_coded"));
                        }
                    }
                }

                if(conceptId != 23722 && Arrays.asList(6, 9, 13, 51, 53).contains(encounterType)) {
                    //result_report_date
                    insertStatement.setTimestamp(RESULT_REPORT_DATE_POS, results.getTimestamp("obs_datetime"));
                    positionsNotSet.remove(RESULT_REPORT_DATE_POS);
                }

                if(VALUE_CODED_CONCEPT_IDS.contains(conceptId)) {
                    int RESULT_QUALTATIVE_ID = results.getInt("value_coded");
                    insertStatement.setInt(RESULT_QUAL_POS, RESULT_QUALTATIVE_ID);
                    positionsNotSet.remove(RESULT_QUAL_POS);

                    // Handle the positivity level if value_coded is 703 (POSITIVE_CONCEPT_ID
                    if(RESULT_QUALTATIVE_ID == POSITIVE_CONCEPT_ID) {
                        positivityStatement.setInt(1, encounterId);
                        positivityStatementResults = positivityStatement.executeQuery();
                        if(positivityStatementResults.next()) {
                            int positivityLevelConceptId = positivityStatementResults.getInt("value_coded");
                            String positivityLevel = "Unknown";
                            if(POSITIVITY_LEVELS.containsKey(positivityLevelConceptId)) {
                                positivityLevel = POSITIVITY_LEVELS.get(positivityLevelConceptId);
                            }
                            insertStatement.setString(RESULT_COMMENT_POS, positivityLevel);
                        }
                    }
                }
                
                if(22772 == conceptId) {
                    //7. result_qualitative_id
                    insertStatement.setInt(LAB_TEST_ID_POS, results.getInt("value_coded"));
                    insertStatement.setInt(RESULT_QUAL_POS, POSITIVE_CONCEPT_ID);
                    positionsNotSet.remove(RESULT_QUAL_POS);
                }

                if(VALUE_NUMERIC_CONCEPT_IDS.contains(conceptId)) {
                    insertStatement.setDouble(RESULT_NUM_POS, results.getDouble("value_numeric"));
                    positionsNotSet.remove(RESULT_NUM_POS);

                    if(CONCEPT_UNITS.containsKey(conceptId)) {
                        insertStatement.setString(RESULT_UNITS_POS, CONCEPT_UNITS.get(conceptId));
                        positionsNotSet.remove(RESULT_UNITS_POS);
                    }
                }

                insertStatement.setString(LABTEST_UUID_POS, results.getString("obs_uuid"));

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

            if(positivityStatementResults != null) {
                positivityStatementResults.close();
            }

            if(positivityStatement != null) {
                positivityStatement.close();
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
		        .append(".encounter_obs e JOIN ").append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
                .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
                .append(" AND ((e.concept_id = ")
                .append(FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID)
                .append(" AND e.value_coded IN ").append(inClause(FICHA_CLINICA_LAB_ANSWERS)).append(") OR e.concept_id IN ")
                .append(inClause(LAB_CONCEPT_IDS)).append(") AND e.obs_datetime <= '")
                .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("'");
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT e.*, p.patient_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".encounter_obs e JOIN ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".patient p ON e.patient_id = p.patient_id AND e.encounter_type IN ")
                .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
                .append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
                .append(" AND ((e.concept_id = ")
                .append(FICHA_CLINICA_LAB_REQUEST_CONCEPT_ID)
                .append(" AND e.value_coded IN ").append(inClause(FICHA_CLINICA_LAB_ANSWERS)).append(") OR e.concept_id IN ")
                .append(inClause(LAB_CONCEPT_IDS)).append(") AND e.obs_datetime <= '")
                .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("'")
                .append(" ORDER BY e.obs_id");
		
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
            if(toBeGenerated == 0) {
                hasRecords = Boolean.FALSE;
            }
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
                    Set<Integer> emptyPositions = new HashSet<>(Arrays.asList(3));
                    Map<Integer, Object> enc8561305 = entry.getValue();
                    if(!(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) && !lastIteration) continue;
                    int encounterType = (Integer) enc8561305.get(-2);
                    insertStatement.setString(ENCOUNTER_UUID_POS, (String) enc8561305.get(ENCOUNTER_UUID_POS));
                    insertStatement.setTimestamp(ENCOUNTER_DATE_POS, (Timestamp) enc8561305.get(ENCOUNTER_DATE_POS));
                    insertStatement.setInt(ENC_TYPE_POS, (int) enc8561305.get(ENC_TYPE_POS));
                    insertStatement.setTimestamp(ENC_CREATED_DATE_POS, (Timestamp) enc8561305.get(ENC_CREATED_DATE_POS));
                    insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, (Timestamp) enc8561305.get(ENC_CHANGE_DATE_POS));
                    insertStatement.setInt(FORM_ID_POS, (int) enc8561305.get(FORM_ID_POS));
                    insertStatement.setString(PATIENT_UUID_POS, (String) enc8561305.get(PATIENT_UUID_POS));
                    insertStatement.setString(LOC_UUID_POS, (String) enc8561305.get(LOC_UUID_POS));
                    insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
                    insertStatement.setInt(LAB_TEST_ID_POS, 856);

                    if(enc8561305.containsKey(ORDER_DATE_POS)) {
                        insertStatement.setDate(ORDER_DATE_POS, (Date) enc8561305.get(ORDER_DATE_POS));
                    } else {
                        emptyPositions.add(ORDER_DATE_POS);
                    }

                    if(enc8561305.containsKey(SAMPLE_COLL_DATE_POS)) {
                        insertStatement.setDate(SAMPLE_COLL_DATE_POS, (Date) enc8561305.get(SAMPLE_COLL_DATE_POS));
                    } else {
                        emptyPositions.add(SAMPLE_COLL_DATE_POS);
                    }

                    if(Arrays.asList(6, 9, 13, 51, 53).contains(encounterType)) {
                        insertStatement.setTimestamp(RESULT_REPORT_DATE_POS, (Timestamp) enc8561305.get(RESULT_REPORT_DATE_POS));
                    } else {
                        emptyPositions.add(RESULT_REPORT_DATE_POS);
                    }

                    if(enc8561305.containsKey(RESULT_COMMENT_POS)) {
                        insertStatement.setString(RESULT_COMMENT_POS, (String) enc8561305.get(RESULT_COMMENT_POS));
                    } else {
                        emptyPositions.add(RESULT_COMMENT_POS);
                    }

                    if(enc8561305.containsKey(SPECIMEN_TYPE_POS)) {
                        insertStatement.setInt(SPECIMEN_TYPE_POS, (Integer) enc8561305.get(SPECIMEN_TYPE_POS));
                    } else {
                        emptyPositions.add(SPECIMEN_TYPE_POS);
                    }

                    insertStatement.setString(LABTEST_UUID_POS, (String) enc8561305.get(LABTEST_UUID_POS));

                    if(enc8561305.containsKey(856) && enc8561305.containsKey(1305)) {
                        //Combined
                        insertStatement.setInt(RESULT_QUAL_POS, (Integer) enc8561305.get(RESULT_QUAL_POS));

                        insertStatement.setDouble(RESULT_NUM_POS, (Double) enc8561305.get(RESULT_NUM_POS));
                        insertStatement.setString(RESULT_UNITS_POS, (String) enc8561305.get(RESULT_UNITS_POS));
                        entriesIterator.remove();
                        runInsert = true;
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        toBeGenerated--;
                        currentlyGenerated -= 2;
                    } else if(enc8561305.containsKey(856) && lastIteration) {
                        insertStatement.setDouble(RESULT_NUM_POS, (Double) enc8561305.get(RESULT_NUM_POS));
                        insertStatement.setString(RESULT_UNITS_POS, (String) enc8561305.get(RESULT_UNITS_POS));
                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(RESULT_QUAL_POS);
                        setEmptyPositions(emptyPositions);
                        insertStatement.addBatch();
                        rowsToInserted++;
                        currentlyGenerated--;
                    } else if(enc8561305.containsKey(1305) && lastIteration) {
                        insertStatement.setInt(LAB_TEST_ID_POS, 1305);
                        insertStatement.setInt(RESULT_QUAL_POS, (Integer) enc8561305.get(RESULT_QUAL_POS));
                        entriesIterator.remove();
                        runInsert = true;
                        emptyPositions.add(RESULT_NUM_POS);
                        emptyPositions.add(RESULT_UNITS_POS);
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
