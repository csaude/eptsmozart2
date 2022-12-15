/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.eptsmozart2;

import org.springframework.stereotype.Component;

/**
 * Contains module's config.
 */
@Component("eptsmozart2.EPTSMozART2Config")
public class EPTSMozART2Config {
	
	public final static String MODULE_PRIVILEGE = "Generate MozART2";
	
	public final static String DATETIME_DISPLAY_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	public final static String DATE_DISPLAY_PATTERN = "yyyy-MM-dd";
	
	public final static String MOZART2_DUMP_FILENAME_GP_NAME = "eptsmozart2.mozart2.dump.file.name";
}
