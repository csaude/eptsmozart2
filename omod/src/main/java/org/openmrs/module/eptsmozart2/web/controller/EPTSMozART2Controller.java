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
	private static final Map<String, StatusInfo> statuses = new LinkedHashMap<>();
	private static final ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();

	static {
		statuses.put("patient", new StatusInfo("patient", 0, 0));
		statuses.put("patient_state", new StatusInfo("patient_state", 0, 0));
		statuses.put("form", new StatusInfo("form", 0, 0));
		statuses.put("identifier", new StatusInfo("identifier", 0, 0));
		statuses.put("observation", new StatusInfo("observation", 0, 0));
		statuses.put("medications", new StatusInfo("medications", 0, 0));
		statuses.put("laboratory", new StatusInfo("laboratory", 0, 0));
		statuses.put("clinical_consultation", new StatusInfo("clinical_consultation", 0, 0));
	}

	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.form", method = RequestMethod.GET)
	public ModelAndView onGet() {
		ModelAndView modalAndView = new ModelAndView(VIEW);
		if(GENERATOR_TASK.GENERATORS.isEmpty()) {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(statuses));
		} else {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(generateStatusInfo()));
		}
		return modalAndView;
	}
	
	/**
	 * All the parameters are optional based on the necessity
	 * 
	 * @param httpSession
	 * @param anyRequestObject
	 * @param errors
	 * @return
	 */
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.json", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> onPost(HttpSession httpSession, @ModelAttribute("anyRequestObject") Object anyRequestObject,
	        BindingResult errors) {
		if(!GENERATOR_TASK.isExecuting()) {
			SINGLE_THREAD_EXECUTOR.submit(GENERATOR_TASK);
			try {
				// Wait 7 seconds
				Thread.sleep(7000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			log.debug("Generator Task already running");
		}
		return getListOfStatuses(generateStatusInfo());
	}

	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2cancel.json", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public List<StatusInfo> cancelMozartGeneration() {
		if(GENERATOR_TASK.isExecuting()) {
			log.debug("Stopping Mozart2 generation");
			GENERATOR_TASK.shutdown();
			SINGLE_THREAD_EXECUTOR.shutdownNow();
		} else {
			log.debug("Mozart2 generation already finished, can't stop it");
		}
		return getListOfStatuses(generateStatusInfo());
	}

	private Map<String, StatusInfo> generateStatusInfo() {
		if(!GENERATOR_TASK.GENERATORS.isEmpty()) {
			GENERATOR_TASK.GENERATORS.stream().forEach(generator -> {
				statuses.put(generator.getTable(),
						new StatusInfo(generator.getTable(), generator.getCurrentlyGenerated(), generator.getToBeGenerated()));
			});
		}
		return statuses;
	}

	private static List<StatusInfo> getListOfStatuses(Map<String, StatusInfo> statusesMap) {
		List<StatusInfo> list = new ArrayList<>();
		for(Map.Entry<String, StatusInfo> entry: statusesMap.entrySet()) {
			list.add(entry.getValue());
		}
		return list;
	}
}
