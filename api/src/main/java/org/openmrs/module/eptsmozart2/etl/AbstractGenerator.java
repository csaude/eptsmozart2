package org.openmrs.module.eptsmozart2.etl;

import java.io.IOException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 3/11/24.
 */
public abstract class AbstractGenerator extends ObservableGenerator {
	
	protected abstract String countQuery() throws IOException;
}
