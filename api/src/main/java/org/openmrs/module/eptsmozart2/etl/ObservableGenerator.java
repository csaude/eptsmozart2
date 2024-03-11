package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Observable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/31/22.
 */
public abstract class ObservableGenerator extends Observable implements Generator {
	
	protected Integer toBeGenerated = 0;
	
	protected Integer currentlyGenerated = 0;
	
	protected Boolean hasRecords = Boolean.TRUE;
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	@Override
	public Boolean getHasRecords() {
		return hasRecords;
	}
	
	@Override
	public void incrementCurrentlyGenerated(Integer increment) {
		currentlyGenerated += increment;
	}
	
	@Override
	public void incrementToBeGenerated(Integer increment) {
		toBeGenerated += increment;
	}
	
	protected abstract String getCreateTableSql() throws IOException;
	
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
}
