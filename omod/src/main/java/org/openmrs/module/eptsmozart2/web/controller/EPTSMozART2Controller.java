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
import org.openmrs.module.eptsmozart2.GenerationCoordinator;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.StatusInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.openmrs.module.eptsmozart2.GenerationCoordinator.GENERATOR_TASK;
import static org.openmrs.module.eptsmozart2.GenerationCoordinator.INITIAL_STATUSES;

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
	
	@Autowired
	protected GenerationCoordinator generationCoordinator;
	
	private static ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.form", method = RequestMethod.GET)
	public ModelAndView onGet() {
		ModelAndView modalAndView = new ModelAndView(VIEW);
		if (GENERATOR_TASK.GENERATORS.isEmpty()) {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(INITIAL_STATUSES));
		} else {
			modalAndView.getModelMap().addAttribute("statuses",
			    getListOfStatuses(generationCoordinator.generateStatusInfo()));
		}
		return modalAndView;
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.json", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> onPost(@RequestParam(value = "endDate", required = false) String endDateString) {
		if (!GENERATOR_TASK.isExecuting()) {
			log.debug("Submitting Mozart2 Generation Task to Executor thread.");
			LocalDate endDate = null;
			if (StringUtils.hasText(endDateString)) {
				endDate = LocalDate.parse(endDateString, DateTimeFormatter.ISO_DATE);
			}
			
			if (endDate != null) {
				log.debug(String.format("Using end date %s for mozart2 generation", endDate.toString()));
				Mozart2Properties.getInstance().setEndDate(endDate);
			}
		}
		return getListOfStatuses(generationCoordinator.runGeneration());
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2cancel.json", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> cancelMozartGeneration() {
		Map<String, StatusInfo> statusInfoMap = generationCoordinator.generateStatusInfo();
		if (GENERATOR_TASK.isExecuting()) {
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
		return getListOfStatuses(generationCoordinator.generateStatusInfo());
	}
	
	private static List<StatusInfo> getListOfStatuses(Map<String, StatusInfo> statusesMap) {
		List<StatusInfo> list = new ArrayList<>();
		for(Map.Entry<String, StatusInfo> entry: statusesMap.entrySet()) {
			list.add(entry.getValue());
		}
		return list;
	}
}
