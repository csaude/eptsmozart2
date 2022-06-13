package org.openmrs.module.eptsmozart2;

import org.junit.Test;

import java.io.IOException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/13/22.
 */
public class UtilsTest {
	
	@Test
	public void readFileToStringShouldWork() throws IOException {
		Utils.readFileToString("form.sql");
	}
}
