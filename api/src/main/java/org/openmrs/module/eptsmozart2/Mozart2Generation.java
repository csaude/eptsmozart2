package org.openmrs.module.eptsmozart2;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Type;
import org.openmrs.User;
import org.openmrs.util.OpenmrsUtil;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.openmrs.module.eptsmozart2.EPTSMozART2Config.DATETIME_DISPLAY_PATTERN;
import static org.openmrs.module.eptsmozart2.EPTSMozART2Config.DATE_DISPLAY_PATTERN;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/17/22.
 */
@Entity
@Table(name = "eptsmozart2_generation")
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class Mozart2Generation {
	
	@Id
	@GeneratedValue
	private Integer id;
	
	@Column(name = "database_name")
	private String databaseName;
	
	@Column(name = "date_started", updatable = false)
	@Type(type = "org.hibernate.type.LocalDateTimeType")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATETIME_DISPLAY_PATTERN)
	@JsonSerialize(using = LocalDateTimeSerializer.class)
	private LocalDateTime dateStarted;
	
	@Column(name = "date_ended")
	@Type(type = "org.hibernate.type.LocalDateTimeType")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATETIME_DISPLAY_PATTERN)
	@JsonSerialize(using = LocalDateTimeSerializer.class)
	private LocalDateTime dateEnded;
	
	@Column(name = "batch_size")
	private Integer batchSize;
	
	@Column(name = "end_date_used")
	@Type(type = "org.hibernate.type.LocalDateType")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_DISPLAY_PATTERN)
	@JsonSerialize(using = LocalDateSerializer.class)
	private LocalDate endDateUsed;
	
	@ManyToOne
	@JoinColumn(name = "executor", nullable = false)
	private User executor;
	
	@Column
	@Enumerated(EnumType.STRING)
	private Status status;
	
	@Column(name = "sql_dump_path")
	@JsonIgnore
	private String sqlDumpPath;
	
	@Column(name = "error_message")
	private String errorMessage;
	
	@Column(name = "stack_trace")
	private String stackTrace;
	
	public Integer getId() {
		return id;
	}
	
	public void setId(Integer id) {
		this.id = id;
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public LocalDateTime getDateStarted() {
		return dateStarted;
	}
	
	public void setDateStarted(LocalDateTime dateStarted) {
		this.dateStarted = dateStarted;
	}
	
	public LocalDateTime getDateEnded() {
		return dateEnded;
	}
	
	public void setDateEnded(LocalDateTime dateCompleted) {
		this.dateEnded = dateCompleted;
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}
	
	public LocalDate getEndDateUsed() {
		return endDateUsed;
	}
	
	public void setEndDateUsed(LocalDate endDateUsed) {
		this.endDateUsed = endDateUsed;
	}
	
	public User getExecutor() {
		return executor;
	}
	
	public void setExecutor(User executor) {
		this.executor = executor;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public String getSqlDumpPath() {
		return sqlDumpPath;
	}
	
	public void setSqlDumpPath(String sqlDumpPath) {
		this.sqlDumpPath = sqlDumpPath;
	}
	
	public String getSqlDumpFilename() {
		return StringUtils.substringAfterLast(sqlDumpPath, "/");
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
	public String getStackTrace() {
		return stackTrace;
	}
	
	public void setStackTrace(String stackTrace) {
		this.stackTrace = stackTrace;
	}
	
	@JsonProperty("duration")
	public String getDuration() {
		if (dateStarted == null || dateEnded == null)
			return "";
		
		StringBuilder sb = new StringBuilder();
		long weeks = ChronoUnit.WEEKS.between(dateStarted, dateEnded);
		long days = ChronoUnit.DAYS.between(dateStarted, dateEnded);
		long hours = ChronoUnit.HOURS.between(dateStarted, dateEnded);
		long minutes = ChronoUnit.MINUTES.between(dateStarted, dateEnded);
		long seconds = ChronoUnit.SECONDS.between(dateStarted, dateEnded);
		long milliseconds = ChronoUnit.MILLIS.between(dateStarted, dateEnded);
		if (weeks > 0) {
			sb.append(weeks).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.weeks")).append(", ");
		}
		if (days > 0 && days % 7 > 0) {
			sb.append(days % 7).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.days")).append(", ");
		}
		if (hours > 0 && hours % 24 > 0) {
			sb.append(hours % 24).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.hours")).append(", ");
		}
		if (minutes > 0 && minutes % 60 > 0) {
			sb.append(minutes % 60).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.minutes")).append(", ");
		}
		if (seconds > 0 && seconds % 60 > 0) {
			sb.append(seconds % 60).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.seconds")).append(", ");
		}
		if (milliseconds > 0 && milliseconds % 1000 > 0) {
			sb.append(milliseconds % 1000).append(" ").append(OpenmrsUtil.getMessage("eptsmozart2.milliseconds"))
			        .append(", ");
		}
		
		String duration = sb.toString().trim();
		
		if (StringUtils.isBlank(duration)) {
			return "0 ms";
		}
		
		if (duration.endsWith(",")) {
			duration = StringUtils.chop(duration);
		}
		return duration;
	}
	
	public enum Status {
		@JsonProperty("ERROR")
		ERROR,
		
		@JsonProperty("COMPLETED")
		COMPLETED,
		
		@JsonProperty("CANCELLED")
		CANCELLED
	}
}
