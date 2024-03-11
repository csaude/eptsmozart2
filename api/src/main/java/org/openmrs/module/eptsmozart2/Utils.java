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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Observable;

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
	
	public static File createMozart2SqlDump() throws IOException, InterruptedException {
		StringBuilder cmd = new StringBuilder("mysqldump -u").append(Mozart2Properties.getInstance().getDbUsername())
		        .append(" -p").append(Mozart2Properties.getInstance().getDbPassword()).append(" --host=")
		        .append(Mozart2Properties.getInstance().getHost()).append(" --port=")
		        .append(Mozart2Properties.getInstance().getPort()).append(" --protocol=").append("tcp")
		        .append(" --compact ").append(Mozart2Properties.getInstance().getNewDatabaseName());
		
		File file = getDumpFilePath().toFile();

		LOGGER.info("Creating SQL dump file {}", file.getName());
		
		ProcessBuilder processBuilder = new ProcessBuilder(cmd.toString().split(" "));
		processBuilder.redirectOutput(file);
		Process process = processBuilder.start();
		int exitCode = process.waitFor();
		
		if (exitCode != 0) {
			try(BufferedReader buf = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
				String line = "";
				LOGGER.error("Error while creating the dump file when running: {}", cmd);
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
		} else {
			LOGGER.info("SQL dump file generated successfully");
			try(BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line = "";
				while ((line = buf.readLine()) != null) {
					LOGGER.info(line);
				}
			}
			return file;
		}
	}
	
	public static Path getDumpFilePath() {
		String dumpFileName = getDumpFilename();
		String mozart2Directory = getMozart2Directory();
		return Paths.get(mozart2Directory, dumpFileName);
	}
	
	public static String getDumpFilename() {
		String filename = Context.getAdministrationService().getGlobalProperty(
		    EPTSMozART2Config.MOZART2_DUMP_FILENAME_GP_NAME);
		if (StringUtils.isNotBlank(filename)) {
			String dateSuffix = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE).replace("-", "");
			filename = StringUtils.removeEnd(filename, ".sql");
			if (filename.endsWith("_")) {
				return filename.concat(dateSuffix).concat(".sql");
			} else {
				return filename.concat("_").concat(dateSuffix).concat(".sql");
			}
		}
		return new StringBuilder(Mozart2Properties.getInstance().getNewDatabaseName()).append(".")
		        .append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace(':', '_')).append(".sql")
		        .toString();
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
