/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.eptsmozart2.api.impl;

import org.openmrs.User;
import org.openmrs.api.APIException;
import org.openmrs.api.UserService;
import org.openmrs.api.context.Context;
import org.openmrs.api.context.UserContext;
import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.eptsmozart2.Mozart2Generation;
import org.openmrs.module.eptsmozart2.api.EPTSMozART2GenerationService;
import org.openmrs.module.eptsmozart2.api.dao.Mozart2GenerationDao;

import javax.validation.constraints.NotNull;
import java.util.List;

public class EPTSMozART2GenerationServiceImpl extends BaseOpenmrsService implements EPTSMozART2GenerationService {
	
	Mozart2GenerationDao dao;
	
	/**
	 * Injected in moduleApplicationContext.xml
	 */
	public void setDao(Mozart2GenerationDao dao) {
		this.dao = dao;
	}
	
	@Override
	public Mozart2Generation getMozart2GenerationById(Integer id) throws APIException {
		return dao.getMozart2GenerationById(id);
	}
	
	@Override
	public Mozart2Generation saveMozartGeneration(@NotNull final Mozart2Generation item) throws APIException {
		if (item.getExecutor() == null) {
			item.setExecutor(Context.getAuthenticatedUser());
		}
		return dao.save(item);
	}
	
	@Override
	public Integer getCountOfAllMozart2Generations() throws APIException {
		return dao.getMozart2GenerationCount(null);
	}
	
	@Override
	public List<Mozart2Generation> getAllMozart2Generations() throws APIException {
		return dao.getMozart2Generations(null, null, null);
	}
	
	@Override
	public List<Mozart2Generation> getMozart2Generations(Integer startIndex, Integer pageSize) throws APIException {
		return dao.getMozart2Generations(null, startIndex, pageSize);
	}
	
	@Override
	public List<Mozart2Generation> getMozart2GenerationsByExecutor(User executor, Integer startIndex, Integer pageSize)
	        throws APIException {
		return dao.getMozart2Generations(executor, startIndex, pageSize);
	}
}
