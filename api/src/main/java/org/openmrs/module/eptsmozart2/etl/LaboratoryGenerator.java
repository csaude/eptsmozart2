package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class LaboratoryGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "laboratory.sql";
	
	private static final Integer[] LAB_CONCEPT_IDS = new Integer[] { 730, 856, 1305, 1695, 5497, 22772, 23722 };
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 9, 13, 51, 53 };

	private static final Map<Integer, String> CONCEPT_UNITS = new HashMap<>();

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
                .append(AppProperties.getInstance().getNewDatabaseName())
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
                    .append("cn1.name as value_coded_name FROM ")
                    .append(AppProperties.getInstance().getDatabaseName()).append(".obs o LEFT JOIN ")
                    .append(AppProperties.getInstance().getDatabaseName())
                    .append(".concept_name cn on cn.concept_id = o.value_coded AND !cn.voided AND cn.locale = 'en' AND ")
                    .append("cn.locale_preferred WHERE !o.voided and o.concept_id IN (6246, 23821, 23832) and encounter_id = ?").toString();

            orderDateSpecimenStatement = ConnectionPool.getConnection().prepareStatement(orderDateSpecimenQuery);
            Set<Integer> positionsNotSet = new HashSet<>();
            while (results.next() && count < batchSize) {
                positionsNotSet.addAll(Arrays.asList(10,11,12,13,14,15,16, 20, 21));
                Integer encounterType = results.getInt("encounter_type");
                insertStatement.setInt(1, results.getInt("encounter_id"));
                insertStatement.setString(2, results.getString("encounter_uuid"));
                insertStatement.setDate(3, results.getDate("encounter_date"));
                insertStatement.setInt(4, encounterType);
                insertStatement.setInt(5, results.getInt("patient_id"));
                insertStatement.setString(6, results.getString("patient_uuid"));
                insertStatement.setString(8, results.getString("concept_name"));

                Integer conceptId = results.getInt("concept_id");
                insertStatement.setInt(7, conceptId);
                insertStatement.setBoolean(9, false);
                if(conceptId == 23722) {
                    insertStatement.setBoolean(9, true);
                    insertStatement.setDate(10, results.getDate("obs_datetime"));
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
                    insertStatement.setDate(12, results.getDate("encounter_date"));
                    positionsNotSet.remove(12);
                }

                if(Arrays.asList(856, 1305, 23722, 22772).contains(conceptId)) {
                    insertStatement.setInt(13, results.getInt("value_coded"));
                    insertStatement.setString(14, results.getString("value_coded_name"));
                    positionsNotSet.remove(13);
                    positionsNotSet.remove(14);
                }

                if(Arrays.asList(5497, 1695,856,730).contains(conceptId)) {
                    insertStatement.setDouble(15, results.getDouble("value_numeric"));
                    positionsNotSet.remove(15);

                    if(conceptId != 1695) {
                        insertStatement.setString(16, CONCEPT_UNITS.get(conceptId));
                        positionsNotSet.remove(16);
                    }
                }

                insertStatement.setString(17, results.getString("comments"));
                insertStatement.setDate(18, results.getDate("date_created"));
                insertStatement.setString(19, AppProperties.getInstance().getDatabaseName());
                insertStatement.setString(22, results.getString("uuid"));

                setEmptyPositions(positionsNotSet);
                insertStatement.addBatch();
                ++count;
            }
            return insertStatement;
        }
        catch (SQLException e) {
            LOGGER.error("Error preparing insert statement for table {}", getTable());
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
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.patient_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(LAB_CONCEPT_IDS));
		return sb.toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT o.*, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, o.person_id as patient_id, ")
		        .append("pe.uuid as patient_uuid, e.encounter_datetime as encounter_date, cn.name as concept_name, ")
		        .append("cn1.name as value_coded_name FROM ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS))
                .append(" AND e.location_id IN (").append(AppProperties.getInstance().getLocationsIdsString())
                .append(") AND e.patient_id IN (SELECT patient_id FROM ")
                .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)")
                .append(" JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".person pe on o.person_id = pe.person_id LEFT JOIN ")
		        .append(AppProperties.getInstance().getDatabaseName())
		        .append(".concept_name cn on cn.concept_id = o.concept_id AND !cn.voided AND cn.locale = 'en' AND ")
		        .append("cn.locale_preferred LEFT JOIN ").append(AppProperties.getInstance().getDatabaseName())
		        .append(".concept_name cn1 on cn1.concept_id = o.value_coded AND !cn1.voided AND cn1.locale = 'en' AND ")
		        .append("cn1.locale_preferred  WHERE !o.voided AND o.concept_id IN ").append(inClause(LAB_CONCEPT_IDS))
		        .append(" ORDER BY o.obs_id");
		
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
					case 15:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case 10:
					case 11:
					case 12:
						insertStatement.setNull(pos, Types.DATE);
						break;
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
