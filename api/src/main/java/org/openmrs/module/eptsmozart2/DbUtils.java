package org.openmrs.module.eptsmozart2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/10/22.
 */
public class DbUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);
	
	public static void createNewDatabase() throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + AppProperties.getInstance().getNewDatabaseName();
        try (Connection connection = ConnectionPool.getConnection();
             Statement s = connection.createStatement()) {
            s.execute(sql);
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sql: {}", sql, sqle);
            throw sqle;
        }
    }
}
