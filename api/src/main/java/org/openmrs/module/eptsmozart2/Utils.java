package org.openmrs.module.eptsmozart2;

import org.apache.commons.io.IOUtils;
import org.openmrs.module.eptsmozart2.etl.AbstractGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/13/22.
 */
public class Utils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	private static final String ART_PATIENT_LIST_QUERY_FILE = "art_patient_list_query.sql";
	
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
}
