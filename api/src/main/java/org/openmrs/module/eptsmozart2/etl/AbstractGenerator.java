package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public abstract class AbstractGenerator implements Callable<Void> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	protected PreparedStatement insertStatement;
	
	protected PreparedStatement selectStatement;
	
	@Override
    public Void call() throws SQLException, IOException {
        ResultSet resultSet = null;
        long startTime = System.currentTimeMillis();
        try (Connection connection = ConnectionPool.getConnection(); Statement statement = connection.createStatement()) {
            createTable();
            resultSet = statement.executeQuery(countQuery());
            resultSet.next();
            int countToMove = resultSet.getInt(1);
            resultSet.close();
            int batchSize = AppProperties.getInstance().getBatchSize();
            if(countToMove > batchSize) {
                LOGGER.debug("Generating {} records for table {} in batches of {}", countToMove, getTable(), batchSize);
                int temp = countToMove;
                int start = 0;
                int batchCount = 1;
                int totalCopied = 0;
                while (temp % batchSize > 0) {
                    if (temp / batchSize > 0) {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                batchCount++, getTable(), totalCopied, batchSize, countToMove - totalCopied);
                        etl(start, batchSize);
                        temp -= batchSize;
                        start += batchSize;
                        totalCopied += batchSize;
                    } else {
                        LOGGER.debug("Inserting batch # {} of {} table, inserted:  {}, inserting: {}, remaining: {}",
                                batchCount++, getTable(), totalCopied, temp, countToMove - totalCopied);
                        etl(start, temp);
                        temp = 0;
                        totalCopied += temp;
                    }
                }
            } else {
                // few records to move.
                LOGGER.debug("Running ETL for {}", getTable());
                int[] inserted = etl(null, null);
            }

            LOGGER.debug("Done inserting {} records for table {}", countToMove, getTable());
            return null;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertStatement.getParameterMetaData().getParameterCount(), e);
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
	
	protected abstract PreparedStatement prepareInsertStatement(ResultSet resultSet) throws SQLException;
	
	protected abstract PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException;
	
	protected abstract String getTable();
	
	protected abstract String getCreateTableSql() throws IOException;
	
	protected abstract String countQuery() throws IOException;
	
	protected abstract String fetchQuery(Integer start, Integer batchSize);
	
	private void createTable() throws IOException {
        String createSql = getCreateTableSql();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            statement.addBatch("USE ".concat(AppProperties.getInstance().getNewDatabaseName()));
            statement.addBatch("DROP TABLE IF EXISTS ".concat(getTable()));
            statement.addBatch(createSql);
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
	
	private int[] etl(Integer start, Integer batchSize) throws SQLException {
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
			throw e;
		}
		finally {
			if (resultSet != null) {
				resultSet.close();
			}
		}
	}
}
