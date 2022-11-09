package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Observable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/31/22.
 */
public abstract class ObservableGenerator extends Observable implements Generator {
	
	protected Integer toBeGenerated = 0;
	
	protected Integer currentlyGenerated = 0;
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	@Override
	public void incrementCurrentlyGenerated(Integer increment) {
		currentlyGenerated += increment;
	}
	
	@Override
	public void incrementToBeGenerated(Integer increment) {
		toBeGenerated += increment;
	}
	
	@Override
	public synchronized void setChanged() {
		super.setChanged();
	}
	
	protected  void runSql(String sql, Map<Integer, Object> params) throws SQLException {
		try(Connection connection = ConnectionPool.getConnection();
			PreparedStatement ps = connection.prepareStatement(sql)) {
			if(params != null) {
				for(Map.Entry<Integer, Object> entry: params.entrySet()) {
					setParams(ps, entry.getKey(), entry.getValue());
				}
			}
			int moreToGo = ps.executeUpdate();
			incrementToBeGenerated(moreToGo);
			incrementCurrentlyGenerated(moreToGo);
		} catch (SQLException e) {
			LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), sql, e);
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
			throw e;
		}
	}
	
	private void setParams(PreparedStatement ps, Integer parameterIndex, Object value) throws SQLException {
		if (value.getClass().getName().equals(Date.class.getName())) {
			ps.setDate(parameterIndex, (Date) value);
		} else if (value.getClass().getName().equals(Timestamp.class.getName())) {
			ps.setTimestamp(parameterIndex, (Timestamp) value);
		} else if (value.getClass().getName().equals(String.class.getName())) {
			ps.setString(parameterIndex, (String) value);
		} else if (value.getClass().getName().equals(Integer.class.getName())) {
			ps.setInt(parameterIndex, (int) value);
		} else if (value.getClass().getName().equals(Double.class.getName())) {
			ps.setDouble(parameterIndex, (double) value);
		} else if (value.getClass().getName().equals(Boolean.class.getName())) {
			ps.setBoolean(parameterIndex, (boolean) value);
		}
	}
}
