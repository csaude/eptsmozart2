/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.eptsmozart2.api;

import org.openmrs.User;
import org.openmrs.annotation.Authorized;
import org.openmrs.api.APIException;
import org.openmrs.api.OpenmrsService;
import org.openmrs.module.eptsmozart2.EPTSMozART2Config;
import org.openmrs.module.eptsmozart2.Mozart2Generation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * The main service of this module, which is exposed for other modules. See
 * moduleApplicationContext.xml on how it is wired up.
 */
public interface EPTSMozART2GenerationService extends OpenmrsService {
	
	/**
	 * Returns an item by uuid. It can be called by any authenticated user. It is fetched in read
	 * only transaction.
	 * 
	 * @param id
	 * @return Mozart2Generation
	 * @throws APIException
	 */
	@Authorized()
	@Transactional(readOnly = true)
	Mozart2Generation getMozart2GenerationById(Integer id) throws APIException;
	
	/**
	 * Saves an item. Sets the owner to superuser, if it is not set. It can be called by users with
	 * this module's privilege. It is executed in a transaction.
	 * 
	 * @param item
	 * @return
	 * @throws APIException
	 */
	@Transactional
	Mozart2Generation saveMozartGeneration(Mozart2Generation item) throws APIException;
	
	/**
	 * @return
	 */
	@Transactional
	Integer getCountOfAllMozart2Generations();
	
	/**
	 * @return
	 */
	@Transactional
	List<Mozart2Generation> getAllMozart2Generations();
	
	/**
	 * @param startIndex
	 * @param pageSize
	 * @return
	 * @throws APIException
	 */
	@Transactional
	List<Mozart2Generation> getMozart2Generations(Integer startIndex, Integer pageSize) throws APIException;
	
	/**
	 * @param executor
	 * @param startIndex
	 * @param pageSize
	 * @return
	 * @throws APIException
	 */
	@Transactional
	List<Mozart2Generation> getMozart2GenerationsByExecutor(final User executor, Integer startIndex, Integer pageSize)
	        throws APIException;
}
