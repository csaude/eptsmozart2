package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
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

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 11/1/22.
 */
public class DSDSupportGroupTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DSDSupportGroupTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "dsd_supportgroup.sql";
	
	private static final Integer[] ENCOUNTER_TYPE_IDS = new Integer[] { 6, 35 };
	
	public static final Integer[] SUPPORT_GROUP_CONCEPTS = new Integer[] { 23753, 23755, 23757, 23759, 24031, 165324, 165325 };
	
	private Boolean etlSupportGroupHasBeenCalledAtLeastOnce = false;

	@Override
	public Void call() throws SQLException, IOException {
		ResultSet rs1 = null, rs2 = null;
		long startTime = System.currentTimeMillis();
		currentlyGenerated = 0;
		try (Connection connection = ConnectionPool.getConnection(); Statement statement = connection.createStatement()) {
			createTable();
			rs1 = statement.executeQuery(countQuery());
			rs1.next();
			toBeGenerated = rs1.getInt(1);
			rs2 = statement.executeQuery(countQuerySupportGroup());
			rs2.next();
			int supportGroupRecords = rs2.getInt(1);
			toBeGenerated += supportGroupRecords;
			if(toBeGenerated == 0) {
				hasRecords = Boolean.FALSE;
				return null;
			}
			int batchSize = Mozart2Properties.getInstance().getBatchSize();
			int dsdRecords = toBeGenerated - supportGroupRecords;
			int batchCount = 1;
			if(dsdRecords > batchSize) {
				LOGGER.debug("Generating {} DSD records for table {} in batches of {}", dsdRecords, getTable(), batchSize);
				int temp = dsdRecords;
				int start = 0;
				while (temp % batchSize > 0) {
					if (temp / batchSize > 0) {
						LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
								batchCount++, getTable(), currentlyGenerated, batchSize, toBeGenerated - currentlyGenerated);
						etl(start, batchSize);
						temp -= batchSize;
						start += batchSize;
						currentlyGenerated += batchSize;
					} else {
						LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
								batchCount++, getTable(), currentlyGenerated, temp, toBeGenerated - currentlyGenerated);
						etl(start, temp);
						currentlyGenerated += temp;
						temp = 0;
					}
				}
			} else {
				// few records to move.
				LOGGER.debug("Running ETL for {}", getTable());
				etl(null, null);
				currentlyGenerated += dsdRecords;
			}

			if(supportGroupRecords > batchSize) {
				LOGGER.debug("Generating {} support group records for table {} in batches of {}",
						supportGroupRecords, getTable(), batchSize);
				int temp = supportGroupRecords;
				int start = 0;
				while (temp % batchSize > 0) {
					if (temp / batchSize > 0) {
						LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
								batchCount++, getTable(), currentlyGenerated, batchSize, toBeGenerated - currentlyGenerated);
						etlSupportGroup(start, batchSize);
						temp -= batchSize;
						start += batchSize;
						currentlyGenerated += batchSize;
					} else {
						LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
								batchCount++, getTable(), currentlyGenerated, temp, toBeGenerated - currentlyGenerated);
						etlSupportGroup(start, temp);
						currentlyGenerated += temp;
						temp = 0;
					}
				}
			} else {
				// few records to move.
				LOGGER.debug("Running ETL for {}", getTable());
				etlSupportGroup(null, null);
				currentlyGenerated += supportGroupRecords;
			}
			LOGGER.debug("Done inserting {} records for table {}", toBeGenerated, getTable());
			return null;
		} catch (SQLException e) {
			LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(),
					insertStatement.getParameterMetaData().getParameterCount(), e);
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		} finally {
			if(rs1 != null) {
				try {
					rs1.getStatement().close();
					rs1.close();
				} catch (SQLException e){}
			}
			if(rs2 != null) {
				try {
					rs2.getStatement().close();
					rs2.close();
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
	
	@Override
	protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
		if (batchSize == null)
			batchSize = Integer.MAX_VALUE;

		String insertSql = new StringBuilder("INSERT IGNORE INTO ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(
		            ".dsd_supportgroup (encounter_uuid, dsd_supportgroup_id, dsd_supportgroup_state, dsd_supportgroup_uuid, ")
		        .append("encounter_type, encounter_created_date, encounter_change_date, ")
		        .append("form_id, patient_uuid, location_uuid, source_database, encounter_date) ")
		        .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();
		try {
			if (insertStatement == null) {
				insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
			} else {
				insertStatement.clearParameters();
			}
			int count = 0;
			while (results.next() && count < batchSize) {
				insertStatement.setString(1, results.getString("encounter_uuid"));
				insertStatement.setTimestamp(12, results.getTimestamp("encounter_datetime"));
				insertStatement.setInt(2, results.getInt("dsd_supportgroup_id"));
				insertStatement.setInt(3, results.getInt("dsd_supportgroup_state"));
				insertStatement.setString(4, results.getString("dsd_supportgroup_uuid"));
				insertStatement.setInt(5, results.getInt("encounter_type"));
				insertStatement.setTimestamp(6, results.getTimestamp("e_date_created"));
				insertStatement.setTimestamp(7, results.getTimestamp("e_date_changed"));
				insertStatement.setInt(8, results.getInt("form_id"));
				insertStatement.setString(9, results.getString("patient_uuid"));
				insertStatement.setString(10, results.getString("loc_uuid"));
				insertStatement.setString(11, Mozart2Properties.getInstance().getSourceOpenmrsInstance());
				
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
		}
	}
	
	@Override
	public String getTable() {
		return "dsd_supportgroup";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o1 JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o2 on o1.obs_group_id = o2.obs_group_id AND o1.encounter_id = o2.encounter_id AND ")
		        .append("o1.concept_id = 165174 AND o2.concept_id = 165322 AND !o1.voided AND !o2.voided JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o1.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o1.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(endDate).append("'");
		return sb.toString();
	}
	
	private String countQuerySupportGroup() {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		return new StringBuilder("SELECT COUNT(*) FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id ")
		        .append("WHERE !o.voided AND !e.voided AND o.concept_id IN ").append(inClause(SUPPORT_GROUP_CONCEPTS))
		        .append(" AND e.encounter_type IN ").append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(endDate).append("'").toString();
	}
	
	private int[] etlSupportGroup(Integer start, Integer batchSize) throws SQLException {
		String query = fetchQuerySupportGroup(start, batchSize);
		ResultSet resultSet = null;
		try {
			if (selectStatement == null || !etlSupportGroupHasBeenCalledAtLeastOnce) {
				etlSupportGroupHasBeenCalledAtLeastOnce = true;
				if (selectStatement != null)
					selectStatement.close();
				selectStatement = ConnectionPool.getConnection().prepareStatement(query);
			} else {
				selectStatement.clearParameters();
			}
			
			if (start != null && batchSize != null) {
				selectStatement.setInt(1, start);
				selectStatement.setInt(2, batchSize);
				selectStatement.addBatch();
			}
			LOGGER.debug("Running query: {}", query);
			resultSet = selectStatement.executeQuery();
			prepareInsertStatement(resultSet, batchSize);
			return insertStatement.executeBatch();
		}
		catch (SQLException e) {
			LOGGER.error("Error while running query: {}", query);
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		}
		finally {
			if (resultSet != null) {
				resultSet.close();
			}
		}
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT o1.encounter_id, o1.value_coded as dsd_supportgroup_id, ")
		        .append("o2.value_coded as dsd_supportgroup_state, o1.date_created, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, e.date_created as e_date_created, ")
		        .append("e.date_changed as e_date_changed, o1.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_datetime, o1.uuid as dsd_supportgroup_uuid, ")
		        .append("e.form_id, l.uuid as loc_uuid FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o1 JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o2 on o1.obs_group_id = o2.obs_group_id AND o1.encounter_id = o2.encounter_id AND ")
		        .append("o1.concept_id = 165174 AND o2.concept_id = 165322 AND !o1.voided AND !o2.voided JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o1.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o1.encounter_id = e.encounter_id AND e.encounter_type IN ")
		        .append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(endDate).append("' JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id ORDER BY o1.obs_group_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
	
	private String fetchQuerySupportGroup(Integer start, Integer batchSize) {
		Date endDate = Date.valueOf(Mozart2Properties.getInstance().getEndDate());
		StringBuilder sb = new StringBuilder("SELECT o.encounter_id, o.concept_id as dsd_supportgroup_id, ")
		        .append("o.value_coded as dsd_supportgroup_state, o.date_created, ")
		        .append("e.uuid as encounter_uuid, e.encounter_type, e.date_created as e_date_created, ")
		        .append("e.date_changed as e_date_changed, o.person_id as patient_id, ")
		        .append("p.patient_uuid, e.encounter_datetime, o.uuid as dsd_supportgroup_uuid, ")
		        .append("e.form_id, l.uuid as loc_uuid FROM ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".obs o JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id ")
		        .append("WHERE !o.voided AND !e.voided AND o.concept_id IN ").append(inClause(SUPPORT_GROUP_CONCEPTS))
		        .append(" AND e.encounter_type IN ").append(inClause(ENCOUNTER_TYPE_IDS)).append(" AND e.location_id IN ")
		        .append(inClause(Mozart2Properties.getInstance().getLocationsIds().toArray(new Integer[0])))
		        .append(" AND e.encounter_datetime <= '").append(endDate).append("'");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		return sb.toString();
	}
}
