package org.openmrs.module.eptsmozart2;

import org.apache.commons.lang3.StringUtils;
import org.openmrs.api.context.Context;
import org.openmrs.util.OpenmrsConstants;
import org.openmrs.util.OpenmrsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class Mozart2Properties {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Mozart2Properties.class);
	
	public final static String MOZART2_APPNAME_GP_NAME = "eptsmozart2.application.name";
	
	public final static String MOZART2_LOCATION_IDS_GP_NAME = "eptsmozart2.location.ids";
	
	public final static String MOZART2_BATCH_SIZE_GP_NAME = "eptsmozart2.batch.size";
	
	public final static String MOZART2_NEW_DB_NAME_GP_NAME = "eptsmozart2.mozart2.database.name";
	
	public final static String JDBC_URL_PROP = "jdbc.url";
	
	public final static String DB_PASSWORD_PROP = "db.password";
	
	public final static String DB_USERNAME_PROP = "db.username";
	
	public final static String END_DATE_PROP = "end.date";
	
	public final static String END_DATE_PATTERN_PROP = "end.date.pattern";
	
	public final static String BATCH_SIZE_PROP = "batch.size";
	
	public final static String MOZART2_PROPERTIES_FILENAME = "mozart2.properties";
	
	private static final String DEFAULT_END_DATE_PATTERN = "dd-MM-yyyy";
	
	private static final String DEFAULT_NEW_DB_NAME = "mozart2";
	
	private static final int DEFAULT_BATCH_SIZE = 20000;
	
	private static Mozart2Properties mozart2Properties = null;
	
	private static final Properties APP_PROPS = new Properties();
	
	private static Properties runtimeProperties = null;
	
	private LocalDate endDate;
	
	private DateTimeFormatter endDateFormatter;
	
	private String host;
	
	private String databaseName;
	
	private Integer port;
	
	private Map<Integer, String> locationUuidMap;
	
	private Set<Integer> locationIdsSet;
	
	private String locationIdsString;
	
	private Integer batchSize;
	
	private String newDatabaseName;
	
	private String sourceOpenmrsInstance;
	
	private Mozart2Properties() {
	}
	
	public static void initializeMozart2Properties() {
		mozart2Properties = new Mozart2Properties();
		try {
			String dataDirectory = OpenmrsUtil.getApplicationDataDirectory();
			Path path = Paths.get(dataDirectory, MOZART2_PROPERTIES_FILENAME);
			File mozart2PropFile = path.toFile();
			if(mozart2PropFile.exists() && mozart2PropFile.isFile()) {
				APP_PROPS.load(OpenmrsUtil.getResourceInputStream(mozart2PropFile.toURI().toURL()));
			}
			String applicationName = Context.getAdministrationService().getGlobalProperty(MOZART2_APPNAME_GP_NAME, "openmrs");
			runtimeProperties = OpenmrsUtil.getRuntimeProperties(applicationName);

			//Host and port
			mozart2Properties.determineMysqlHostAndPortFromJdbcUrl();

			if(StringUtils.isNotBlank(APP_PROPS.getProperty(END_DATE_PROP))) {
				try {
					String datePattern = APP_PROPS.getProperty(END_DATE_PATTERN_PROP, DEFAULT_END_DATE_PATTERN);
					mozart2Properties.endDateFormatter = DateTimeFormatter.ofPattern(datePattern);
					mozart2Properties.endDate = LocalDate.parse(APP_PROPS.getProperty(END_DATE_PROP), mozart2Properties.endDateFormatter);
				} catch (DateTimeParseException | NullPointerException e) {
					if (APP_PROPS.containsKey(END_DATE_PROP) && APP_PROPS.getProperty(END_DATE_PROP) != null) {
						LOGGER.warn("Invalid value set for property {}, defaulting to current date", END_DATE_PROP);
					} else {
						LOGGER.info("{} property not set, defaulting to current date", END_DATE_PROP);
					}
					mozart2Properties.endDate = LocalDate.now();
				}
			}
			try {
				mozart2Properties.batchSize = Integer.valueOf(Context.getAdministrationService().getGlobalProperty(MOZART2_BATCH_SIZE_GP_NAME));
			}
			catch (NumberFormatException e) {
				LOGGER.debug("Invalid value set for {}, ignoring and using default of {}", BATCH_SIZE_PROP, DEFAULT_BATCH_SIZE);
				mozart2Properties.batchSize = DEFAULT_BATCH_SIZE;
			}
			mozart2Properties.newDatabaseName = Context.getAdministrationService()
					.getGlobalProperty(MOZART2_NEW_DB_NAME_GP_NAME, DEFAULT_NEW_DB_NAME);

			mozart2Properties.sourceOpenmrsInstance = Context.getAdministrationService()
					.getGlobalProperty(OpenmrsConstants.GLOBAL_PROPERTY_DEFAULT_LOCATION_NAME);
			initializeLocationProperties();
		} catch (Exception e) {
			LOGGER.warn("An error occured during reading of app properties");
			LOGGER.warn("The passed properties are: {} ", APP_PROPS, e);
		}
	}
	
	private static void initializeLocationProperties () {
		mozart2Properties.locationIdsString = Context.getAdministrationService().getGlobalProperty(MOZART2_LOCATION_IDS_GP_NAME);
		if(StringUtils.isBlank(mozart2Properties.locationIdsString)) {
			throw new IllegalStateException("MozART2 database cannot be generated because there is no location specified in the global property value");
		}
		mozart2Properties.locationIdsString = Context.getAdministrationService().getGlobalProperty(MOZART2_LOCATION_IDS_GP_NAME);
		mozart2Properties.locationIdsString = StringUtils.trim(mozart2Properties.locationIdsString);
		mozart2Properties.locationIdsString = StringUtils.removeEnd(mozart2Properties.locationIdsString, ",");
		mozart2Properties.locationIdsSet = new HashSet<>();
		for(String locIdString: mozart2Properties.locationIdsString.split(",")) {
			try {
				int locId = Integer.parseInt(locIdString.trim());
				mozart2Properties.locationIdsSet.add(locId);
			} catch (NumberFormatException e) {
				String em = String.format("The provided location Id %s is invalid, please use numbers", locIdString);
				LOGGER.error(em);
				throw new IllegalStateException(em);
			}
		}
		Integer[] array = mozart2Properties.locationIdsSet.toArray(new Integer[0]);
		String locationQuery = new StringBuilder("SELECT * FROM ").append(mozart2Properties.databaseName)
				.append(".location WHERE location_id IN ").append(inClause(array)).toString();
		try(Connection conn = ConnectionPool.getConnection();
			ResultSet rs = conn.createStatement().executeQuery(locationQuery)) {
			mozart2Properties.locationUuidMap = new HashMap<>();
			while (rs.next()) {
				mozart2Properties.locationUuidMap.put(rs.getInt("location_id"), rs.getString("uuid"));
			}
		} catch (SQLException e) {
			LOGGER.warn("Error when fetching location UUIDs using query: {}", locationQuery);
            throw new RuntimeException(e);
        }
    }
	
	public static Mozart2Properties getInstance() {
		if (mozart2Properties == null) {
			initializeMozart2Properties();
		}
		return mozart2Properties;
	}
	
	public String getJdbcUrl() {
		if (StringUtils.isNotBlank(APP_PROPS.getProperty(JDBC_URL_PROP))) {
			return APP_PROPS.getProperty(JDBC_URL_PROP);
		}
		if (runtimeProperties != null && StringUtils.isNotBlank(runtimeProperties.getProperty("connection.url"))) {
			return runtimeProperties.getProperty("connection.url");
		}
		LOGGER.warn("JDBC connection URL is blank, EPTS Mozart2 needs it to run");
		return null;
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	
	public String getDbUsername() {
		if (StringUtils.isNotBlank(APP_PROPS.getProperty(DB_USERNAME_PROP))) {
			return APP_PROPS.getProperty(DB_USERNAME_PROP);
		}
		if (StringUtils.isNotBlank(runtimeProperties.getProperty("connection.username"))) {
			return runtimeProperties.getProperty("connection.username");
		}
		LOGGER.warn("JDBC connection username is blank, EPTS Mozart2 needs it to run");
		return null;
	}
	
	public String getDbPassword() {
		if (StringUtils.isNotBlank(APP_PROPS.getProperty(DB_PASSWORD_PROP))) {
			return APP_PROPS.getProperty(DB_PASSWORD_PROP);
		}
		if (StringUtils.isNotBlank(runtimeProperties.getProperty("connection.password"))) {
			return runtimeProperties.getProperty("connection.password");
		}
		LOGGER.warn("JDBC connection password is blank, EPTS Mozart2 needs it to run");
		return null;
	}
	
	public String getNewDatabaseName() {
		return newDatabaseName;
	}
	
	public String getSourceOpenmrsInstance() {
		return sourceOpenmrsInstance;
	}
	
	public String getHost() {
		return host;
	}
	
	public Integer getPort() {
		return port;
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}
	
	public LocalDate getEndDate() {
		return endDate;
	}
	
	public String getFormattedEndDate(String pattern) {
		if (pattern == null)
			return endDate.format(endDateFormatter);
		else {
			return endDate.format(DateTimeFormatter.ofPattern(pattern));
		}
	}
	
	public String getLocationsIdsString() {
		return locationIdsString;
	}
	
	public Set<Integer> getLocationIdsSet() {
		return locationIdsSet;
	}
	
	public String getLocationUuidById(Integer locationId) {
		return locationUuidMap.get(locationId);
	}
	
	@Override
	public String toString() {
		return APP_PROPS.toString();
	}
	
	private void determineMysqlHostAndPortFromJdbcUrl() {
		String jdbcUrl = this.getJdbcUrl();
		if (jdbcUrl != null) {
			int indexOfDoubleSlash = jdbcUrl.indexOf("//");
			String hostToEnd = jdbcUrl.substring(indexOfDoubleSlash + 2);
			int indexOfColonAfterHost = hostToEnd.indexOf(':');
			int indexOfSlashAfterHost = hostToEnd.indexOf('/');
			if (indexOfColonAfterHost > 0) {
				this.host = hostToEnd.substring(0, indexOfColonAfterHost);
				this.port = Integer.valueOf(hostToEnd.substring(indexOfColonAfterHost + 1, indexOfSlashAfterHost));
				
			} else {
				this.host = hostToEnd.substring(0, indexOfSlashAfterHost);
				this.port = 3306;
			}
			
			int indexOfQuestionMark = hostToEnd.indexOf('?');
			if (indexOfQuestionMark > 0) {
				this.databaseName = hostToEnd.substring(indexOfSlashAfterHost + 1, indexOfQuestionMark);
			} else {
				this.databaseName = hostToEnd.substring(indexOfSlashAfterHost);
			}
		}
	}
	
	public void setEndDate(LocalDate endDate) {
		this.endDate = endDate;
	}
	
	public List<String> validateProperties() {
		List<String> validationErrors = new ArrayList<>();
		validateNewDatabaseName(validationErrors);
		validateLocationIds(validationErrors);
		return validationErrors;
	}
	
	protected void validateNewDatabaseName(List<String> errors) {
		String newDatabaseName = Mozart2Properties.getInstance().getNewDatabaseName();
		if ("openmrs".equalsIgnoreCase(newDatabaseName)) {
			errors.add(OpenmrsUtil.getMessage("eptsmozart2.invalid.new.db.global.property"));
		}
	}
	
	protected void validateLocationIds(List<String> errors) {
		String locationsIdsString = Context.getAdministrationService().getGlobalProperty(MOZART2_LOCATION_IDS_GP_NAME);
		if (StringUtils.isBlank(locationsIdsString)) {
			errors.add(OpenmrsUtil.getMessage("eptsmozart2.empty.location.ids.global.property"));
		} else {
			String[] locIdStrings = locationsIdsString.split(",");
			for (String locIdString : locIdStrings) {
				try {
					Integer.parseInt(locIdString.trim());
				}
				catch (NumberFormatException e) {
					String em = OpenmrsUtil.getMessage("eptsmozart2.invalid.location.ids.global.property", locIdString);
					LOGGER.warn(em);
					errors.add(em);
				}
			}
		}
	}
}
