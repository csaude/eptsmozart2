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

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.eptsmozart2.EPTSMozART2Config;
import org.openmrs.module.eptsmozart2.GenerationCoordinator;
import org.openmrs.module.eptsmozart2.Mozart2Generation;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.StatusInfo;
import org.openmrs.module.eptsmozart2.api.EPTSMozART2GenerationService;
import org.openmrs.module.eptsmozart2.web.Mozart2GenerationDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	@Autowired
	protected EPTSMozART2GenerationService moz2GenService;
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.form", method = RequestMethod.GET)
	public ModelAndView onGet() {
		ModelAndView modalAndView = new ModelAndView(VIEW);
		if (GENERATOR_TASK.GENERATORS.isEmpty()) {
			modalAndView.getModelMap().addAttribute("statuses", getListOfStatuses(INITIAL_STATUSES));
		} else {
			modalAndView.getModelMap().addAttribute("statuses",
			    getListOfStatuses(generationCoordinator.generateStatusInfo()));
		}
		
		modalAndView.getModelMap().addAttribute("generations", moz2GenService.getAllMozart2Generations());
		modalAndView.getModelMap().addAttribute("formatter",
		    DateTimeFormatter.ofPattern(EPTSMozART2Config.DATETIME_DISPLAY_PATTERN));
		modalAndView.getModelMap().addAttribute("dateFormatter",
		    DateTimeFormatter.ofPattern(EPTSMozART2Config.DATE_DISPLAY_PATTERN));
		modalAndView.getModelMap().addAttribute("lastGeneration", moz2GenService.getLastMozart2Generation());
		return modalAndView;
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2.json", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public Map<String, Object> onPost(@RequestParam(value = "endDate", required = false) String endDateString) {
		Mozart2Properties.reInitializeMozart2Properties();
		List<String> globalPropertyErrors = Mozart2Properties.getInstance().validateProperties();
		Map<String, Object> errors = new HashMap<>();
		if(!globalPropertyErrors.isEmpty()) {
			errors.put("globalPropertyErrors", globalPropertyErrors);
			return errors;
		}

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
		Map<String, StatusInfo> statusInfoMap = generationCoordinator.runGeneration();
		return getDTO(statusInfoMap, moz2GenService.getLastMozart2Generation(), Boolean.TRUE);
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2cancel.json", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public Map<String, Object> cancelMozartGeneration() {
		Map<String, StatusInfo> statusInfoMap = generationCoordinator.generateStatusInfo();
		generationCoordinator.cancelGeneration();
		return getDTO(statusInfoMap, moz2GenService.getLastMozart2Generation(), Boolean.FALSE);
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2status.json", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public Map<String, Object> getMozartStatus() {
		Map<String, Object> statusDetails = getDTO(generationCoordinator.generateStatusInfo(),
		    moz2GenService.getLastMozart2Generation(), generationCoordinator.GENERATOR_TASK.isExecuting());
		if (generationCoordinator.getGenerationException() != null) {
			statusDetails.put("errorMessage", generationCoordinator.getGenerationException().getMessage());
		}
		return statusDetails;
	}
	
	@RequestMapping(value = "/module/eptsmozart2/eptsmozart2download.json")
	public void getSqlDump(@RequestParam("id") Integer id, HttpServletResponse response) {
		Mozart2Generation toDownload = moz2GenService.getMozart2GenerationById(id);
		try {
			InputStream is = new FileInputStream(toDownload.getSqlDumpPath());
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
			    "attachment; filename=".concat(toDownload.getSqlDumpFilename()));
			IOUtils.copy(is, response.getOutputStream());
			response.flushBuffer();
		}
		catch (IOException ex) {
			log.info(String.format("Error writing SQL dump '%s' file to output stream.", toDownload.getSqlDumpFilename()),
			    ex);
			throw new RuntimeException(ex);
		}
	}
	
	private static List<StatusInfo> getListOfStatuses(Map<String, StatusInfo> statusesMap) {
		List<StatusInfo> list = new ArrayList<>();
		for(Map.Entry<String, StatusInfo> entry: statusesMap.entrySet()) {
			list.add(entry.getValue());
		}
		return list;
	}
	
	private static Map<String, Object> getDTO(Map<String, StatusInfo> statusesMap, Mozart2Generation lastGeneration, Boolean isRunning) {
		Map<String, Object> dto = new HashMap<>();
		dto.put("statuses", getListOfStatuses(statusesMap));
		if(lastGeneration != null) {
			dto.put("lastGeneration", new Mozart2GenerationDTO(lastGeneration));
		} else {
			dto.put("lastGeneration", null);
		}
		dto.put("isRunning", isRunning);
		return dto;
	}
}
