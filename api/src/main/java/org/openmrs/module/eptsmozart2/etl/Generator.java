package org.openmrs.module.eptsmozart2.etl;

import java.util.concurrent.Callable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/14/22.
 */
public interface Generator extends Callable<Void> {
	
	String getTable();
	
	Integer getCurrentlyGenerated();
	
	Integer getToBeGenerated();
}
