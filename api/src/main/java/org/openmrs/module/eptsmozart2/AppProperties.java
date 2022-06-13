package org.openmrs.module.eptsmozart2;

import org.openmrs.util.OpenmrsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class AppProperties {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppProperties.class);
	
	public final static String JDBC_URL_PROP = "jdbc.url";
	
	public final static String DB_PASSWORD_PROP = "db.password";
	
	public final static String DB_USERNAME_PROP = "db.username";
	
	public final static String END_DATE_PROP = "end.date";
	
	public final static String END_DATE_PATTERN_PROP = "end.date.pattern";
	
	public final static String NEW_DB_NAME_PROP = "newDb.name";
	
	public final static String DROP_NEW_DB_AFTER_PROP = "drop.newDb.after";
	
	public final static String BATCH_SIZE_PROP = "batch.size";
	
	public final static String MOZART2_PROPERTIES_FILENAME = "mozart2.properties";
	
	public static final String LOG_LEVEL_PROP = "log.level";
	
	private static final String DEFAULT_END_DATE_PATTERN = "dd-MM-yyyy";
	
	private static final int DEFAULT_BATCH_SIZE = 20000;
	
	private static AppProperties appProperties = null;
	
	private static final Properties APP_PROPS = new Properties();
	
	private LocalDate endDate;
	
	private DateTimeFormatter endDateFormatter;
	
	private String host;
	
	private String databaseName;
	
	private Integer port;
	
	private Boolean dropNewDbAfter;
	
	private Integer batchSize;
	
	private AppProperties() {
	}
	
	public static AppProperties getInstance() {
        if(appProperties == null) {
            appProperties = new AppProperties();
            try {
                String dataDirectory = OpenmrsUtil.getApplicationDataDirectory();
                Path path = Paths.get(dataDirectory, MOZART2_PROPERTIES_FILENAME);
                File mozart2PropFile = path.toFile();
                if(!mozart2PropFile.exists() || !mozart2PropFile.isFile()) {
                    // TODO: We have a problem
                }
                APP_PROPS.load(OpenmrsUtil.getResourceInputStream(mozart2PropFile.toURI().toURL()));

                try {
                    String datePattern = APP_PROPS.getProperty(END_DATE_PATTERN_PROP, DEFAULT_END_DATE_PATTERN);
                    appProperties.endDateFormatter = DateTimeFormatter.ofPattern(datePattern);
                    appProperties.endDate = LocalDate.parse(APP_PROPS.getProperty(END_DATE_PROP), appProperties.endDateFormatter);
                } catch (DateTimeParseException|NullPointerException e) {
                    if(APP_PROPS.containsKey(END_DATE_PROP) && APP_PROPS.getProperty(END_DATE_PROP) != null) {
                        LOGGER.error("Invalid value set for property {}", END_DATE_PROP);
                        throw e;
                    } else {
                        // Just use now.
                        LOGGER.info("{} property not set, defaulting to current date", END_DATE_PROP);
                        appProperties.endDate = LocalDate.now();
                    }
                }

                appProperties.dropNewDbAfter = Boolean.valueOf(APP_PROPS.getProperty(DROP_NEW_DB_AFTER_PROP, "FALSE"));


                try {
                    appProperties.batchSize = Integer.valueOf(APP_PROPS.getProperty(BATCH_SIZE_PROP));
                } catch (NumberFormatException e) {
                    LOGGER.debug("Invalid value set for {}, ignoring and using default of {}", BATCH_SIZE_PROP, DEFAULT_BATCH_SIZE);
                    appProperties.batchSize = DEFAULT_BATCH_SIZE;
                }
                //Host and port
                appProperties.determineMysqlHostAndPortFromJdbcUrl();
            } catch (Exception e) {
                LOGGER.error("An error occured during reading of app properties (this most likely is a result of invalid application.properties file or lack thereof)");
                LOGGER.error("The passed properties are: {} ", APP_PROPS, e);
            }
        }

        return appProperties;
    }
	
	public String getJdbcUrl() {
		return APP_PROPS.getProperty(JDBC_URL_PROP);
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	
	public String getDbUsername() {
		return APP_PROPS.getProperty(DB_USERNAME_PROP);
	}
	
	public String getDbPassword() {
		return APP_PROPS.getProperty(DB_PASSWORD_PROP);
	}
	
	public String getNewDatabaseName() {
		return APP_PROPS.getProperty(NEW_DB_NAME_PROP);
	}
	
	public String getHost() {
		return host;
	}
	
	public Integer getPort() {
		return port;
	}
	
	public Boolean getDropNewDbAfter() {
		return dropNewDbAfter;
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
	
	public String getLogLevel() {
		return APP_PROPS.getProperty(LOG_LEVEL_PROP, "TRACE").toUpperCase();
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
}
