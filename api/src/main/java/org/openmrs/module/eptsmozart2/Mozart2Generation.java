package org.openmrs.module.eptsmozart2;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.openmrs.User;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/17/22.
 */
@Entity
@Table(name = "eptsmozart2_generation")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Mozart2Generation {
	
	@Id
	@GeneratedValue
	private Integer id;
	
	@Column(name = "database_name")
	private String databaseName;
	
	@Column(name = "date_started", updatable = false)
	@Temporal(TemporalType.TIMESTAMP)
	private Date dateStarted;
	
	@Column(name = "date_completed")
	private Date dateCompleted;
	
	@Column(name = "batch_size")
	private Integer batchSize;
	
	@ManyToOne
	@JoinColumn(name = "executor", nullable = false)
	private User executor;
	
	@Column(name = "sql_dump_path")
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
	
	public Date getDateCompleted() {
		return dateCompleted;
	}
	
	public void setDateCompleted(Date dateCompleted) {
		this.dateCompleted = dateCompleted;
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
	
	public String getSqlDumpPath() {
		return sqlDumpPath;
	}
	
	public void setSqlDumpPath(String sqlDumpPath) {
		this.sqlDumpPath = sqlDumpPath;
	}
	
	public String getSqlDumpFilename() {
		return StringUtils.substringAfterLast(sqlDumpPath, "/");
	}
}
