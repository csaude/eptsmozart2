package org.openmrs.module.eptsmozart2;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/30/22.
 */
@XmlRootElement
public class StatusInfo {
	
	private String table;
	
	private Integer generated;
	
	private Integer toBeGenerated;
	
	public StatusInfo() {
	}
	
	public StatusInfo(String table, Integer generated, Integer toBeGenerated) {
		this.table = table;
		this.generated = generated;
		this.toBeGenerated = toBeGenerated;
	}
	
	public String getTable() {
		return table;
	}
	
	public void setTable(String table) {
		this.table = table;
	}
	
	public Integer getGenerated() {
		return generated;
	}
	
	public void setGenerated(Integer generated) {
		this.generated = generated;
	}
	
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	public void setToBeGenerated(Integer toBeGenerated) {
		this.toBeGenerated = toBeGenerated;
	}
}
