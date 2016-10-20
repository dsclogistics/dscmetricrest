package com.dsc.mtrc.dto;

import java.util.List;

public class Role {
	
	private int roleId;
	private int prodId;
	private String roleName;
	private String description;
	private String prodName;
	private List<RoleMetricPeriod> roleMetricPeriods;
	public int getRoleId() {
		return roleId;
	}
	public void setRoleId(int roleId) {
		this.roleId = roleId;
	}
	public int getProdId() {
		return prodId;
	}
	public void setProdId(int prodId) {
		this.prodId = prodId;
	}
	public String getRoleName() {
		return roleName;
	}
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getProdName() {
		return prodName;
	}
	public void setProdName(String prodName) {
		this.prodName = prodName;
	}
	
	
	public List<RoleMetricPeriod> getRoleMetricPeriods() {
		return roleMetricPeriods;
	}
	public void setRoleMetricPeriods(List<RoleMetricPeriod> roleMetricPeriods) {
		this.roleMetricPeriods = roleMetricPeriods;
	}
	@Override
	public String toString() {
		return "Role [roleId=" + roleId + ", prodId=" + prodId + ", roleName=" + roleName + ", description="
				+ description + ", prodName=" + prodName + "]";
	}
	
	

}
