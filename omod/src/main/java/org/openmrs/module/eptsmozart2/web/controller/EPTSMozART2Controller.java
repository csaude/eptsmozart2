/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.eptsmozart2.web.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.eptsmozart2.GeneratorTask;
import org.openmrs.module.eptsmozart2.StatusInfo;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class configured as controller using annotation and mapped with the URL of
 * 'module/eptsmozart2/eptsmozart2Link.form'.
 */
@Controller("eptsmozart2.EPTSMozART2Controller")
public class EPTSMozART2Controller {
	
	/** Logger for this class and subclasses */
	protected final Log log = LogFactory.getLog(getClass());
	
	/** Success form view name */
	private final String VIEW = "/module/eptsmozart2/eptsmozart2";

	private static final GeneratorTask GENERATOR_TASK = new GeneratorTask();
	private static final Map<String, StatusInfo> INITIAL_STATUSES = new LinkedHashMap<>();
	private static final ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();

	static {
		INITIAL_STATUSES.put("patient", new StatusInfo("patient", 0, 0));
		INITIAL_STATUSES.put("patient_state", new StatusInfo("patient_state", 0, 0));
		INITIAL_STATUSES.put("form", new StatusInfo("form", 0, 0));
		INITIAL_STATUSES.put("identifier", new StatusInfo("identifier", 0, 0));
		INITIAL_STATUSES.put("observation", new StatusInfo("observation", 0, 0));
		INITIAL_STATUSES.put("medications", new StatusInfo("medications", 0, 0));
		INITIAL_STATUSES.put("laboratory", new StatusInfo("laboratory", 0, 0));
		INITIAL_STATUSES.put("clinical_consultation", new StatusInfo("clinical_consultation", 0, 0));
	}

	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.form", method = RequestMethod.GET)
	public ModelAndView onGet() {
		ModelAndView modalAndView = new ModelAndView(VIEW);
		if(GENERATOR_TASK.GENERATORS.isEmpty()) {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(INITIAL_STATUSES));
		} else {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(generateStatusInfo()));
		}
		return modalAndView;
	}

	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.json", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> onPost() {
		if(!GENERATOR_TASK.isExecuting()) {
		    log.debug("Submitting Mozart2 Generation Task to Executor thread.");
			SINGLE_THREAD_EXECUTOR.submit(GENERATOR_TASK);
		} else {
			log.debug("Generator Task already running");
		}
		return getListOfStatuses(generateStatusInfo());
	}

	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2cancel.json", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> cancelMozartGeneration() {
	    Map<String, StatusInfo> statusInfoMap = generateStatusInfo();
		if(GENERATOR_TASK.isExecuting()) {
			log.debug("Stopping Mozart2 generation");
			GENERATOR_TASK.shutdown();
			SINGLE_THREAD_EXECUTOR.shutdownNow();
		} else {
			log.debug("Mozart2 generation already finished, can't stop it");
		}
		return getListOfStatuses(statusInfoMap);
	}

    @RequestMapping(value = "/module/eptsmozart2/eptsmozart2status.json", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<StatusInfo> getMozartStatus() {
	    return getListOfStatuses(generateStatusInfo());
    }

	private Map<String, StatusInfo> generateStatusInfo() {
		if(!GENERATOR_TASK.GENERATORS.isEmpty()) {
            Map<String, StatusInfo> progressStatuses = new LinkedHashMap<>(INITIAL_STATUSES);
			GENERATOR_TASK.GENERATORS.stream().forEach(generator -> {
				progressStatuses.replace(generator.getTable(),
						new StatusInfo(generator.getTable(), generator.getCurrentlyGenerated(), generator.getToBeGenerated()));
			});
            return progressStatuses;
		}
		return INITIAL_STATUSES;
	}

	private static List<StatusInfo> getListOfStatuses(Map<String, StatusInfo> statusesMap) {
		List<StatusInfo> list = new ArrayList<>();
		for(Map.Entry<String, StatusInfo> entry: statusesMap.entrySet()) {
			list.add(entry.getValue());
		}
		return list;
	}
}
