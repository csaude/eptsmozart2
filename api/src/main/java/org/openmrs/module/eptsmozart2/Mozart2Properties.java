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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

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
	
	private Mozart2Properties() {
	}
	
	public static Mozart2Properties getInstance() {
        if(mozart2Properties == null) {
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

                try {
                    String datePattern = APP_PROPS.getProperty(END_DATE_PATTERN_PROP, DEFAULT_END_DATE_PATTERN);
                    mozart2Properties.endDateFormatter = DateTimeFormatter.ofPattern(datePattern);
                    mozart2Properties.endDate = LocalDate.parse(APP_PROPS.getProperty(END_DATE_PROP), mozart2Properties.endDateFormatter);
                } catch (DateTimeParseException|NullPointerException e) {
                    if(APP_PROPS.containsKey(END_DATE_PROP) && APP_PROPS.getProperty(END_DATE_PROP) != null) {
                        LOGGER.error("Invalid value set for property {}", END_DATE_PROP);
                        throw e;
                    } else {
                        // Just use now.
                        LOGGER.info("{} property not set, defaulting to current date", END_DATE_PROP);
                        mozart2Properties.endDate = LocalDate.now();
                    }
                }
                //Host and port
                mozart2Properties.determineMysqlHostAndPortFromJdbcUrl();
            } catch (Exception e) {
                LOGGER.error("An error occured during reading of app properties (this most likely is a result of invalid application.properties file or lack thereof)");
                LOGGER.error("The passed properties are: {} ", APP_PROPS, e);
            }
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
		return Context.getAdministrationService().getGlobalProperty(MOZART2_NEW_DB_NAME_GP_NAME, DEFAULT_NEW_DB_NAME);
	}
	
	public String getSourceOpenmrsInstance() {
		return Context.getAdministrationService().getGlobalProperty(OpenmrsConstants.GLOBAL_PROPERTY_APPLICATION_NAME);
	}
	
	public String getHost() {
		return host;
	}
	
	public Integer getPort() {
		return port;
	}
	
	public Integer getBatchSize() {
		Integer batchSize;
		try {
			batchSize = Integer.valueOf(Context.getAdministrationService().getGlobalProperty(MOZART2_BATCH_SIZE_GP_NAME));
		}
		catch (NumberFormatException e) {
			LOGGER.debug("Invalid value set for {}, ignoring and using default of {}", BATCH_SIZE_PROP, DEFAULT_BATCH_SIZE);
			batchSize = DEFAULT_BATCH_SIZE;
		}
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
		String locationsIdsString = Context.getAdministrationService().getGlobalProperty(MOZART2_LOCATION_IDS_GP_NAME);
		
		if (StringUtils.isBlank(locationsIdsString)) {
			throw new IllegalStateException(
			        "MozART2 database cannot be generated because there is no location specified in the global property value");
		}
		return locationsIdsString;
	}
	
	public Set<Integer> getLocationsIds() {
		String locationsIdsString = Context.getAdministrationService().getGlobalProperty(MOZART2_LOCATION_IDS_GP_NAME);

		if(StringUtils.isBlank(locationsIdsString)) {
			throw new IllegalStateException("MozART2 database cannot be generated because there is no location specified in the global property value");
		}
		String[] locIdStrings = locationsIdsString.split(",");
		Set<Integer> locIds = new HashSet<>();
		for(String locIdString: locIdStrings) {
			try {
				int locId = Integer.parseInt(locIdString.trim());
				if(Context.getLocationService().getLocation(locId) == null) {
					throw new IllegalStateException(String.format("Provided location.location_id %s value does not exist", locIdString));
				}
				locIds.add(locId);
			} catch (NumberFormatException e) {
				String em = String.format("The provided location Id %s is invalid, please use numbers", locIdString);
				LOGGER.error(em);
				throw new IllegalStateException(em);
			}
		}
		return locIds;
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
}
