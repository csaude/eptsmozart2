package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/14/22.
 */
public interface Generator extends Callable<Void> {
	Logger LOGGER = LoggerFactory.getLogger(Generator.class);

	String getTable();
	
	Integer getCurrentlyGenerated();
	
	Integer getToBeGenerated();

	default void createTable(String createTableSql) throws IOException, SQLException {
		try(Connection connection = ConnectionPool.getConnection();
			Statement statement = connection.createStatement()) {
			statement.addBatch("USE ".concat(Mozart2Properties.getInstance().getNewDatabaseName()));
			statement.addBatch("DROP TABLE IF EXISTS ".concat(getTable()));
			statement.addBatch(createTableSql);
			statement.executeBatch();
		} catch (SQLException e) {
			LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), createTableSql, e);
			throw e;
		}
	}
}
