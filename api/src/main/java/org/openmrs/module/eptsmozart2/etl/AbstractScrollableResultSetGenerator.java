package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Set;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public abstract class AbstractScrollableResultSetGenerator extends AbstractGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScrollableResultSetGenerator.class);
	
	protected PreparedStatement insertStatement;
	
	protected PreparedStatement selectStatement;
	
	protected Statement scrollableStatement;
	
	protected ResultSet scrollableResultSet;
	
	protected boolean thereIsNext = false;
	
	protected Integer encounterId = null;
	
	@Override
    public Void call() throws SQLException, IOException {
        ResultSet resultSet = null;
        long startTime = System.currentTimeMillis();
        String query = fetchQuery();
        currentlyGenerated = 0;
        try (Connection connection = ConnectionPool.getConnection(); Statement statement = connection.createStatement()) {
            createTable();
            resultSet = statement.executeQuery(countQuery());
            resultSet.next();
            toBeGenerated = resultSet.getInt(1);
            if(toBeGenerated == 0) {
                hasRecords = Boolean.FALSE;
                return null;
            }
            resultSet.close();
            if (scrollableStatement == null) {
                scrollableStatement = ConnectionPool.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                                                     ResultSet.CONCUR_READ_ONLY);
                scrollableStatement.setFetchSize(Integer.MIN_VALUE);
                scrollableResultSet = scrollableStatement.executeQuery(query);
            }
            int batchSize = Mozart2Properties.getInstance().getBatchSize();
            if(toBeGenerated > batchSize) {
                LOGGER.debug("Generating {} records for table {} in batches of {}", toBeGenerated, getTable(), batchSize);
                int temp = toBeGenerated;
                int batchCount = 1;
                while (temp % batchSize > 0) {
                    if (temp / batchSize > 0) {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                     batchCount++, getTable(), currentlyGenerated, batchSize, toBeGenerated - currentlyGenerated);
                        int generatedNow = etl(batchSize);
                        temp -= generatedNow;
                        currentlyGenerated += generatedNow;
                    } else {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                     batchCount++, getTable(), currentlyGenerated, temp, toBeGenerated - currentlyGenerated);
                        int generated = etl(temp);;
                        currentlyGenerated += generated;
                        temp = 0;
                    }
                }
            } else {
                // few records to move.
                LOGGER.debug("Running ETL for {}", getTable());
                etl(null);
                currentlyGenerated += toBeGenerated;
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

            if(scrollableStatement != null) {
                Connection connection = scrollableStatement.getConnection();
                if(connection != null) connection.close();
                scrollableStatement.close();
            }

            if(scrollableResultSet != null) {
                scrollableResultSet.close();
            }
            LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
        }
    }
	
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
				}
			}
			Integer prevEncounterId = null;
			while (thereIsNext && (count < batchSize || Objects.equals(prevEncounterId, encounterId))) {
				setInsertSqlParameters(positionsNotSet);
				thereIsNext = scrollableResultSet.next();
				if (thereIsNext) {
					prevEncounterId = encounterId;
					encounterId = scrollableResultSet.getInt("encounter_id");
					if (!Objects.equals(prevEncounterId, encounterId)) {
						setEmptyPositions(positionsNotSet);
						insertStatement.addBatch();
						positionsNotSet = getAllPositionsNotSet();
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
	public void cancel() throws SQLException {
		if (selectStatement != null) {
			selectStatement.cancel();
		}
		
		if (insertStatement != null) {
			insertStatement.cancel();
		}
	}
	
	protected abstract String fetchQuery();
	
	protected abstract String insertSql();
	
	protected abstract Set<Integer> getAllPositionsNotSet();
	
	protected abstract void setInsertSqlParameters(Set<Integer> positionsNotSet) throws SQLException;
	
	protected abstract void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException;
}
