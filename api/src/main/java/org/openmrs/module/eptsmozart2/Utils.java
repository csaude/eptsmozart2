package org.openmrs.module.eptsmozart2;

import org.apache.commons.io.IOUtils;
import org.openmrs.module.eptsmozart2.etl.AbstractGenerator;
import org.openmrs.util.OpenmrsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/13/22.
 */
public class Utils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	private static final String ART_PATIENT_LIST_QUERY_FILE = "art_patient_list_query.sql";
	
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
	
	public static String getArtPatientListQuery() {
		try {
			return Utils
			        .readFileToString(ART_PATIENT_LIST_QUERY_FILE)
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
				while ((line = buf.readLine()) != null) {
					LOGGER.error(line);
				}
				throw new IOException("Could not generate SQL dump file");
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
		return new StringBuilder(Mozart2Properties.getInstance().getNewDatabaseName()).append(".")
		        .append(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME).replace(':', '_')).append(".sql")
		        .toString();
	}
	
	public static String getMozart2Directory() {
		String dataDirectory = OpenmrsUtil.getApplicationDataDirectory();
		if (dataDirectory.endsWith(File.separator)) {
			return dataDirectory.concat(MOZART2_DIR_NAME);
		}
		return dataDirectory.concat(File.separator).concat(MOZART2_DIR_NAME);
	}
}
