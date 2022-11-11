package org.openmrs.module.eptsmozart2.etl;

import java.io.IOException;
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
}
