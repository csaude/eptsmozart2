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

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public abstract class AbstractGenerator extends ObservableGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	protected PreparedStatement insertStatement;
	
	protected PreparedStatement selectStatement;
	
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
                int[] inserted = etl(null, null);
                currentlyGenerated += toBeGenerated;
            }

            LOGGER.debug("Done inserting {} records for table {}", toBeGenerated, getTable());
            return null;
        } catch (SQLException e) {
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
	
	@Override
	public void cancel() throws SQLException {
		if (selectStatement != null) {
			selectStatement.cancel();
		}
		
		if (insertStatement != null) {
			insertStatement.cancel();
		}
	}
	
	protected abstract PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException;
	
	protected abstract PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException;
	
	protected abstract String getCreateTableSql() throws IOException;
	
	protected abstract String countQuery() throws IOException;
	
	protected abstract String fetchQuery(Integer start, Integer batchSize);
	
	protected void createTable() throws IOException {
        String createSql = getCreateTableSql();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            statement.addBatch("USE ".concat(Mozart2Properties.getInstance().getNewDatabaseName()));
            statement.addBatch("DROP TABLE IF EXISTS ".concat(getTable()));
            statement.addBatch(createSql);
            statement.executeBatch();
        } catch (SQLException e) {
            this.setChanged();
            Utils.notifyObserversAboutException(this, e);
            e.printStackTrace();
        }
    }
	
	protected int[] etl(Integer start, Integer batchSize) throws SQLException {
		String query = fetchQuery(start, batchSize);
		ResultSet resultSet = null;
		try {
			if (selectStatement == null) {
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
}
