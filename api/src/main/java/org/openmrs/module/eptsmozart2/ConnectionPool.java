package org.openmrs.module.eptsmozart2;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/9/22.
 */
public class ConnectionPool {
	
	public static int MAX_CONNECTIONS = 100;
	
	private static PooledDataSource pooledDataSource;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);
	
	static {
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(Mozart2Properties.getInstance().getJdbcUrl());
		cpds.setUser(Mozart2Properties.getInstance().getDbUsername());
		cpds.setPassword(Mozart2Properties.getInstance().getDbPassword());
		cpds.setInitialPoolSize(10);
		cpds.setMinPoolSize(10);
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(MAX_CONNECTIONS);
		cpds.setTestConnectionOnCheckin(true);
		cpds.setTestConnectionOnCheckout(true);
		cpds.setIdleConnectionTestPeriod(60);
		cpds.setPreferredTestQuery("SELECT 1");
		cpds.setAcquireRetryAttempts(10);
		cpds.setAcquireRetryDelay(1000);
		cpds.setBreakAfterAcquireFailure(false);
		cpds.setMaxConnectionAge(3600);
		cpds.setMaxIdleTime(1800);
		pooledDataSource = cpds;
	}
	
	public static synchronized Connection getConnection() throws SQLException {
		Connection connection = null;
		try {
			connection = pooledDataSource.getConnection();
			if (!connection.isValid(1)) {
				LOGGER.warn("Connection is invalid, attempting to reconnect...");
				connection.close();
				connection = pooledDataSource.getConnection();
			}
			return connection;
		}
		catch (SQLException sqle) {
			Throwable cause = sqle.getCause();
			if (cause instanceof InterruptedException) {
				if (connection != null) {
					return connection;
				} else {
					LOGGER.error("Error connecting to the database using url: {}, username: {} and password: {}",
					    Mozart2Properties.getInstance().getJdbcUrl(), Mozart2Properties.getInstance().getDbUsername(),
					    Mozart2Properties.getInstance().getDbPassword());
					throw sqle;
				}
			}
			LOGGER.error("Error connecting to the database using url: {}, username: {} and password: {}", Mozart2Properties
			        .getInstance().getJdbcUrl(), Mozart2Properties.getInstance().getDbUsername(), Mozart2Properties
			        .getInstance().getDbPassword());
			throw sqle;
		}
	}
}
