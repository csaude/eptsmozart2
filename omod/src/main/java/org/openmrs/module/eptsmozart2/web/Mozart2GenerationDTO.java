package org.openmrs.module.eptsmozart2.web;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import org.openmrs.User;
import org.openmrs.module.eptsmozart2.Mozart2Generation;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.openmrs.module.eptsmozart2.EPTSMozART2Config.DATETIME_DISPLAY_PATTERN;
import static org.openmrs.module.eptsmozart2.EPTSMozART2Config.DATE_DISPLAY_PATTERN;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 9/19/22.
 */
public class Mozart2GenerationDTO {
    private Integer id;
    private String databaseName;
    private String dateStarted;
    private String dateEnded;
    private Integer batchSize;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_DISPLAY_PATTERN)
    @JsonSerialize(using = LocalDateSerializer.class)
    private String endDateUsed;

    private Map<String, String> executor = new HashMap<>();

    private Mozart2Generation.Status status;

    private String sqlDumpPath;

    private String errorMessage;

    private String stackTrace;

    private String duration;

    public Mozart2GenerationDTO(Mozart2Generation mozart2Generation) {
        if(mozart2Generation == null) return;
        this.id = mozart2Generation.getId();
        this.databaseName = mozart2Generation.getDatabaseName();
        this.batchSize = mozart2Generation.getBatchSize();
        this.status = mozart2Generation.getStatus();
        this.sqlDumpPath = mozart2Generation.getSqlDumpPath();
        this.errorMessage = mozart2Generation.getErrorMessage();
        this.stackTrace = mozart2Generation.getStackTrace();
        this.duration = mozart2Generation.getDuration();

        User user = mozart2Generation.getExecutor();
        if(user != null) {
            executor.put("uuid", user.getUuid());
            executor.put("fullname", user.getPersonName().getFullName());
            executor.put("username", user.getUsername());
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATETIME_DISPLAY_PATTERN);
        if(mozart2Generation.getDateStarted() != null) {
            this.dateStarted = formatter.format(mozart2Generation.getDateStarted());
        }
        if(mozart2Generation.getDateEnded() != null) {
            this.dateEnded = formatter.format(mozart2Generation.getDateEnded());
        }
        if(mozart2Generation.getEndDateUsed() != null) {
            endDateUsed = DateTimeFormatter.ofPattern(DATE_DISPLAY_PATTERN).format(mozart2Generation.getEndDateUsed());
        }
    }

    public Integer getId() {
        return id;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDateStarted() {
        return dateStarted;
    }

    public String getDateEnded() {
        return dateEnded;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public String getEndDateUsed() {
        return endDateUsed;
    }

    public Map<String, String> getExecutor() {
        return executor;
    }

    public Mozart2Generation.Status getStatus() {
        return status;
    }

    public String getSqlDumpPath() {
        return sqlDumpPath;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getDuration() {
        return duration;
    }
}
