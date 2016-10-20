package com.dsc.mtrc.dto;

import java.util.List;

public class User {

	private String ssoId;
	private int appUserId;
	private String fullName;
	private String email;
	private List<Building> buildings;
	private List<Role> roles;
	
	
	
	public String getSsoId() {
		return ssoId;
	}
	public void setSsoId(String ssoId) {
		this.ssoId = ssoId;
	}
	public String getEmail() {
		return this.email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public List<Building> getBuilginds(){
		return this.buildings;
	}
	public void setBuildings(List<Building> buildings)
	{
		this.buildings = buildings;
	}
	public List<Role> getRoles(){
		return this.roles;
	}
	public void setRoles(List<Role> roles)
	{
		this.roles = roles;
	}
	public int getAppUserId() {
		return this.appUserId;
	}
	public void setAppUserId(int appUserId) {
		this.appUserId = appUserId;
	}
	public String getFullName() {
		return this.fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	@Override
	public String toString() {
		return "User [ssoId=" + ssoId + ", appUserId=" + appUserId + ", fullName=" + fullName + ", email=" + email
				+ ", buildings=" + buildings + "]";
	}
	
	
	
}
