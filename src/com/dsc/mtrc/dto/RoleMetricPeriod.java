package com.dsc.mtrc.dto;

public class RoleMetricPeriod {

	private int metricPeriodId;
	private int metricId;
	private int tptId;
	private String metricPeriodName;
	private String token;
	
	public int getMetricPeriodId() {
		return metricPeriodId;
	}
	public void setMetricPeriodId(int metricPeriodId) {
		this.metricPeriodId = metricPeriodId;
	}
	public int getMetricId() {
		return metricId;
	}
	public void setMetricId(int metricId) {
		this.metricId = metricId;
	}
	public int getTptId() {
		return tptId;
	}
	public void setTptId(int tptId) {
		this.tptId = tptId;
	}
	public String getMetricPeriodName() {
		return metricPeriodName;
	}
	public void setMetricPeriodName(String metricPeriodName) {
		this.metricPeriodName = metricPeriodName;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
}
