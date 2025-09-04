package org.openmrs.module.eptsmozart2;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.openmrs.api.context.Context;
import org.openmrs.module.eptsmozart2.etl.AbstractNonScrollableResultSetGenerator;
import org.openmrs.util.OpenmrsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/13/22.
 */
public class Utils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNonScrollableResultSetGenerator.class);
	
	private static final String PATIENT_LIST_QUERY_FILE = "patient_list_query.sql";
	
	public static final String MOZART2_DIR_NAME = "mozart2";
	
	public static String readFileToString(String path) throws IOException {
		InputStream ip = Utils.class.getClassLoader().getResourceAsStream(path);
		if (ip == null) {
			throw new IllegalArgumentException("File not found");
		}
		return IOUtils.toString(ip, "utf8");
	}
	
	public static String inClause(Integer[] numbers) {
		StringBuilder sb = new StringBuilder("(");
		int i = 0;
		for (; i < numbers.length - 1; i++) {
			sb.append(numbers[i].toString()).append(",");
		}
		return sb.append(numbers[i].toString()).append(")").toString();
	}
	
	public static String getPatientListQuery() {
		try {
			return Utils
			        .readFileToString(PATIENT_LIST_QUERY_FILE)
			        .replace("sourceDatabase", Mozart2Properties.getInstance().getDatabaseName())
			        .replace(":locations", Mozart2Properties.getInstance().getLocationsIdsString())
			        .replace(":endDate",
			            String.format("'%s'", Mozart2Properties.getInstance().getFormattedEndDate("yyyy-MM-dd")));
		}
		catch (IOException e) {
			LOGGER.error("An error occured while reading ART patients' list query file", e);
			throw new RuntimeException(e);
		}
	}
	
	public static File createMozart2SqlDump() throws Exception {
		List<String> command = Arrays.asList(
				"mysqldump",
				"-u" + Mozart2Properties.getInstance().getDbUsername(),
				"-p" + Mozart2Properties.getInstance().getDbPassword(),
				"--host=" + Mozart2Properties.getInstance().getHost(),
				"--port=" + Mozart2Properties.getInstance().getPort(),
				"--protocol=tcp",
				"--compact",
				Mozart2Properties.getInstance().getNewDatabaseName()
		);
		File file = getDumpFilePath().toFile();

		LOGGER.info("Creating SQL dump file {}", file.getName());
		
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		processBuilder.redirectOutput(file);
		Process process = processBuilder.start();
        try {
            int exitCode = process.waitFor();
			if (exitCode == 0) {
				LOGGER.info("SQL dump file generated successfully");
				try(BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
					String line = "";
					while ((line = buf.readLine()) != null) {
						LOGGER.info(line);
					}
				}
				return file;
			} else {
				try(BufferedReader buf = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
					String line = "";
					LOGGER.error("Error while creating the dump file when running: {}", command);
					LOGGER.error("Command exited with exit code {}", exitCode);
					String errorMessage = "";
					while ((line = buf.readLine()) != null) {
						LOGGER.error(line);
						errorMessage += line;
					}
					if(StringUtils.isBlank(errorMessage)) {
						errorMessage = "Could not generate SQL dump file";
					}
					throw new IOException(errorMessage);
				}
			}
        } catch (Exception e) {
			if(e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
				throw new Exception(e);
			}
            throw e;
        }
	}
	
	public static Path getDumpFilePath() {
		String dumpFileName = getDumpFilename();
		String mozart2Directory = getMozart2Directory();
		return Paths.get(mozart2Directory, dumpFileName);
	}
	
	public static String getDumpFilename() {
		String filename = Mozart2Properties.getInstance().getMozart2DumpFilenameGPValue();
		if (StringUtils.isNotBlank(filename)) {
			String dateSuffix = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE).replace("-", "");
			filename = StringUtils.removeEnd(filename, ".sql");
			if (filename.endsWith("_")) {
				return filename.concat(dateSuffix + getQuarterForDate(LocalDate.now())).concat(".sql");
			} else {
				return filename.concat("_").concat(dateSuffix + getQuarterForDate(LocalDate.now())).concat(".sql");
			}
		}
		return new StringBuilder(Mozart2Properties.getInstance().getNewDatabaseName()).append(".")
		        .append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace(':', '_'))
		        .append(getQuarterForDate(LocalDate.now())).append(".sql").toString();
	}
	
	public static String getQuarterForDate(LocalDate date) {
		MonthDay md = MonthDay.from(date);
		
		if (!md.isBefore(MonthDay.of(Month.DECEMBER, 20)) || md.isBefore(MonthDay.of(Month.MARCH, 21))) {
			return "Q1";
		} else if (!md.isBefore(MonthDay.of(Month.MARCH, 20)) && md.isBefore(MonthDay.of(Month.JUNE, 21))) {
			return "Q2";
		} else if (!md.isBefore(MonthDay.of(Month.JUNE, 20)) && md.isBefore(MonthDay.of(Month.SEPTEMBER, 21))) {
			return "Q3";
		} else {
			return "Q4";
		}
	}
	
	public static String getMozart2Directory() {
		String dataDirectory = OpenmrsUtil.getApplicationDataDirectory();
		if (dataDirectory.endsWith(File.separator)) {
			return dataDirectory.concat(MOZART2_DIR_NAME);
		}
		return dataDirectory.concat(File.separator).concat(MOZART2_DIR_NAME);
	}
	
	public static void notifyObserversAboutException(Observable observable, Exception e) {
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put("name", "exception");
		parameters.put("status", e);
		try {
			Context.openSession();
			observable.notifyObservers(parameters);
		} finally {
			Context.closeSession();
		}
	}
}
