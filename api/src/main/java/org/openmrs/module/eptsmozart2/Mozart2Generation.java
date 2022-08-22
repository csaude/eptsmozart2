package org.openmrs.module.eptsmozart2;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.openmrs.User;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

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
	@Temporal(TemporalType.TIMESTAMP)
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Africa/Maputo")
	private Date dateStarted;
	
	@Column(name = "date_ended")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Africa/Maputo")
	private Date dateEnded;
	
	@Column(name = "batch_size")
	private Integer batchSize;
	
	@ManyToOne
	@JoinColumn(name = "executor", nullable = false)
	private User executor;
	
	@Column
	@Enumerated(EnumType.STRING)
	private Status status;
	
	@Column(name = "sql_dump_path")
	@JsonIgnore
	private String sqlDumpPath;
	
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
	
	public Date getDateStarted() {
		return dateStarted;
	}
	
	public void setDateStarted(Date dateStarted) {
		this.dateStarted = dateStarted;
	}
	
	public Date getDateEnded() {
		return dateEnded;
	}
	
	public void setDateEnded(Date dateCompleted) {
		this.dateEnded = dateCompleted;
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
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
	
	public enum Status {
		@JsonProperty("ERROR")
		ERROR,
		
		@JsonProperty("COMPLETED")
		COMPLETED,
		
		@JsonProperty("CANCELLED")
		CANCELLED
	}
}
