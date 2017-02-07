package com.dsc.mtrc.internal;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.ws.rs.core.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;
import com.dsc.mtrc.dto.Building;
import com.dsc.mtrc.dto.Role;
import com.dsc.mtrc.dto.RoleMetricPeriod;
import com.dsc.mtrc.dto.User;

public class RZAuthenticationManager {
	
	public Response loginUser(String ssoId) throws JSONException
	{
		Response rb = null;
		User user = getUserInfo(ssoId,"DSC AD");		
		JSONObject retJson = new JSONObject();
		JSONArray jroles = new JSONArray();
		
		JSONArray jbuildings = new JSONArray();
		
		try
		{
			if(user!=null && user.getAppUserId()>0)
			{
				retJson.put("result", "Success");
				retJson.put("username", user.getFullName());
				retJson.put("user_id", user.getAppUserId());
				retJson.put("email", user.getEmail());
				retJson.put("sso_id", user.getSsoId());
				if(user.getRoles()!=null)
				{
					for(Role role: user.getRoles())
					{
						JSONObject jrole = new JSONObject();
						if(role.getProdName().equals("Red Zone")||role.getProdName().equals("ALL"))
						{
							jrole.put("prod_name",role.getProdName());
							jrole.put("role_name",role.getRoleName());
							jrole.put("role_id", role.getRoleId());
							jrole.put("role_desc", role.getDescription());
							if(role.getRoleMetricPeriods()!=null)
							{
								JSONArray jmperiods = new JSONArray();
								for(RoleMetricPeriod rmp :role.getRoleMetricPeriods())
								{
									JSONObject jmperiod = new JSONObject();
									jmperiod.put("metric_period_name", rmp.getMetricPeriodName());
									jmperiod.put("metric_period_id", rmp.getMetricPeriodId());
									jmperiod.put("mtrc_name", rmp.getMetricName());
									jmperiod.put("mtrc_id", rmp.getMetricId());
									jmperiods.put(jmperiod);							
								}
								jrole.put("metrics", jmperiods);
							}										
							jroles.put(jrole);	
						}
										
					}//end of for(Role role: user.getRoles())
				}			
				retJson.put("roles", jroles);
				if(user.getBuilginds()!=null)
				{
					for(Building bldg:user.getBuilginds())
					{
						JSONObject jbuilding = new JSONObject();
						jbuilding.put("dsc_mtrc_lc_bldg_id", bldg.getBuildingId());
						jbuilding.put("dsc_mtrc_lc_bldg_name", bldg.getBuildingName());
						jbuilding.put("dsc_mtrc_lc_bldg_code", bldg.getBuildingCode());
						jbuildings.put(jbuilding);
					}
					
				}
				retJson.put("buildings", jbuildings);
				rb = Response.ok(retJson.toString()).build();
			}
			else
			{
				
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error:Cannot find user information");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			JSONObject errJson = new JSONObject();
			errJson.put("result", "FAILED");
			errJson.put("resultCode", "200");
			errJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(errJson.toString()).build();			
			return rb;
		}
		
				
		return rb;
	}
	public Response mockLoginDMUser(String ssoId) throws JSONException
	{
		Response rb = null;
		User user = getUserInfo(ssoId,"DSC AD");		
		JSONObject retJson = new JSONObject();
		JSONArray jroles = new JSONArray();
		
		JSONArray jbuildings = new JSONArray();
		
		try
		{
			if(user!=null && user.getAppUserId()>0)
			{
				retJson.put("result", "Success");
				retJson.put("username", user.getFullName());
				retJson.put("user_id", user.getAppUserId());
				retJson.put("email", user.getEmail());
				retJson.put("sso_id", user.getSsoId());
				if(user.getRoles()!=null)
				{
					for(Role role: user.getRoles())
					{
						JSONObject jrole = new JSONObject();
						if(role.getProdName().equals("Metric Data Management"))
						{
							jrole.put("prod_name",role.getProdName());
							jrole.put("role_name",role.getRoleName());
							jrole.put("role_id", role.getRoleId());
							jrole.put("role_desc", role.getDescription());
							if(role.getRoleMetricPeriods()!=null)
							{
								JSONArray jmperiods = new JSONArray();
								for(RoleMetricPeriod rmp :role.getRoleMetricPeriods())
								{
									JSONObject jmperiod = new JSONObject();
									jmperiod.put("metric_period_name", rmp.getMetricPeriodName());
									jmperiod.put("metric_period_id", rmp.getMetricPeriodId());
									jmperiod.put("mtrc_name", rmp.getMetricName());
									jmperiod.put("mtrc_id", rmp.getMetricId());
									jmperiods.put(jmperiod);							
								}
								jrole.put("metrics", jmperiods);
							}										
							jroles.put(jrole);	
						}
										
					}//end of for(Role role: user.getRoles())
				}			
				retJson.put("roles", jroles);
				if(user.getBuilginds()!=null)
				{
					for(Building bldg:user.getBuilginds())
					{
						JSONObject jbuilding = new JSONObject();
						jbuilding.put("dsc_mtrc_lc_bldg_id", bldg.getBuildingId());
						jbuilding.put("dsc_mtrc_lc_bldg_name", bldg.getBuildingName());
						jbuilding.put("dsc_mtrc_lc_bldg_code", bldg.getBuildingCode());
						jbuildings.put(jbuilding);
					}
					
				}
				retJson.put("buildings", jbuildings);
				rb = Response.ok(retJson.toString()).build();
			}
			else
			{
				
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error:Cannot find user information");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			JSONObject errJson = new JSONObject();
			errJson.put("result", "FAILED");
			errJson.put("resultCode", "200");
			errJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(errJson.toString()).build();			
			return rb;
		}
		
				
		return rb;
	}

	public Response loginRZUser(String username, String password, String domain) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		String fullName = null;
		String email = null;
		
		 
		//JSONObject adResult = authenticateDSCADUser(getADUrl(),username,password);
		JSONObject adResult = authenticateDSCADUserForRZ(username,password,domain);
		try
		{
			if(adResult !=null && adResult.has("result"))
			{
				if(adResult.getString("result").equals("SUCCESS"))
				{
					fullName = adResult.getJSONObject("DSCAuthenticationSrv").getString("first_name")+" "+adResult.getJSONObject("DSCAuthenticationSrv").getString("last_name");
				    email =adResult.getJSONObject("DSCAuthenticationSrv").getString("email"); 			    
				    String validator = authorizeRZUser(username,"DSC AD",fullName,email);
				    System.out.println("authorize user returned: "+validator);
				    if(validator.equals("Success"))
				    {				    	
				    	return loginUser(username);
				    }
				    else
				    {
				    	retJson.put("result", "FAILED");
						retJson.put("resultCode", "200");
						retJson.put("message", validator);
						rb = Response.ok(retJson.toString()).build();
				    }
				}
				else
				{				
					rb = Response.ok(adResult.toString()).build();
				}
			}
			else
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: Cannot authenticate user");
				//retJson.put("message", "Error:"+adResult.toString());
				rb = Response.ok(retJson.toString()).build();
			}
		}
		catch(Exception e)
		{
			
		}
		
		
		return rb;
		
	}
	public Response loginRZDMUser(String username, String password) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		String fullName = null;
		String email = null;
		
		 
		JSONObject adResult = authenticateDSCADUser(getADUrl(),username,password);
		
		try
		{
			if(adResult !=null && adResult.has("result"))
			{
				if(adResult.getString("result").equals("SUCCESS"))
				{
					fullName = adResult.getJSONObject("DSCAuthenticationSrv").getString("first_name")+" "+adResult.getJSONObject("DSCAuthenticationSrv").getString("last_name");
				    email =adResult.getJSONObject("DSCAuthenticationSrv").getString("email"); 
				    String validator = authorizeRZDMUser(username,"DSC AD",fullName,email);
				    System.out.println("authorize user returned: "+validator);
				    if(validator.equals("Success"))
				    {
				    	return mockLoginDMUser(username);
				    }
				    else
				    {
				    	retJson.put("result", "FAILED");
						retJson.put("resultCode", "200");
						retJson.put("message", validator);
						rb = Response.ok(retJson.toString()).build();
				    }
				}
				else
				{				
					rb = Response.ok(adResult.toString()).build();
				}
			}
			else
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: Cannot authenticate user");				
				rb = Response.ok(retJson.toString()).build();
			}
		}
		catch(Exception e)
		{
			
		}
		
		
		return rb;
		
	}
	
	public static String getADUrl()
	{
		String url = null;
		try {
		    Context ctx = new InitialContext();
		    ctx = (Context) ctx.lookup("java:comp/env");
		    url = (String) ctx.lookup("adurl");
		}
		catch (Exception e) {
	        e.printStackTrace();
			url = "http://dscapidev.dsccorp.net:8080/dscrest/api/v1/getobsemp/DSCAuthenticationSrv";			
		}
		return url;
	}

	public JSONObject authenticateDSCADUser(String adUrl,String username, String password) throws JSONException
	{
		JSONObject adOutput = null;
		StringBuilder sb = new StringBuilder();
		try {

	        //URL url = new URL("http://dscapi.dsccorp.net/dscrest/api/v1/getobsemp/DSCAuthenticationSrv");
			//http://dscapidev.dsccorp.net/dscrest/api/v1/getobsemp/DSCAuthenticationSrv			
			//URL url = new URL("http://dscapidev.dsccorp.net:8080/dscrest/api/v1/getobsemp/DSCAuthenticationSrv");
			URL url = new URL(adUrl);
	        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	        conn.setDoOutput(true);
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Content-Type", "application/json");
	        String input = "{ \"username\":\""+username+"\",\"password\":\""+password+"\" }";
	        //System.out.println("payload is:"+input);
	        OutputStream os = conn.getOutputStream();
	        os.write(input.getBytes());
	        os.flush();
	       
	        BufferedReader br = new BufferedReader(new InputStreamReader(
	                (conn.getInputStream())));

	        String output=null;
	        System.out.println("Output from Server .... \n");
	        while (( output =br.readLine()) != null) {	        
	        	
	        	sb.append(output+"\n");
	            System.out.println(output);
	        }
	        String json = sb.toString();
	        System.out.println(json);
			
	       try 
	        {
	        	adOutput = new JSONObject(json);
			}
	        catch (JSONException e)
	        {
				e.printStackTrace();
			}
	        
	        conn.disconnect();

	      } catch (MalformedURLException e) {

	        e.printStackTrace();

	      } catch (IOException e) {

	        e.printStackTrace();

	     }
	
		
		
		return adOutput;
	}

	/*This method retrieves user information from the database
	 * it accepts sso id and authentication system as input parameters
	 * and returns User object which includes basic user info(name, email etc)
	 * and all user roles and role attributes */
	public User getUserInfo(String ssoId, String ssoSystem)

	{
		User user = new User();
		Connection conn = null;		
		PreparedStatement ps = null;
		PreparedStatement mpPrepStmt = null;
		PreparedStatement bldgPrepStmt = null;
		ResultSet rs = null;
		ResultSet res = null;
		
		List<Role> roles = new ArrayList<Role>(); 
		
		
		String rolesSQL = "select u.app_user_id as userid,"
				+ " u.app_user_email_addr as email,"
				+ " u.app_user_full_name as name,"
				+ " u.app_user_sso_id as sso_id,"
				+ " u.app_user_sso_system sso_system,"
				+ " u.app_user_disabled_yn,"
				+ " ap.mar_id,"
				+ " ap.muar_id,"
				+ " r.mar_name as role,"
				+ " r.mar_desc as role_desc,"
				+ " r.mar_req_bldg_auth,"
				+ " r.mar_req_mtrc_mgmt_auth, "
				+ " prod.prod_id,"
				+ " prod.prod_name"
				+ " from dsc_app_user u join"
				+ " MTRC_USER_APP_ROLES ap"
				+ " on u.app_user_id = ap.app_user_id  join"
				+ " MTRC_APP_ROLE r on ap.mar_id = r.mar_id join"
				+ " MTRC_PRODUCT prod on r.prod_id = prod.prod_id"
				+ " where u.app_user_sso_id = ?"
				+ " and u.app_user_sso_system  = ?"
				+ " and getdate() between ap.muar_eff_start_dt and ap.muar_eff_end_dt"
				+ " and getdate() between r.mar_eff_start_dt and r.mar_eff_end_dt";
				
		
		String mpSQL = "select a.mtrc_period_id, p.mtrc_period_name,m.mtrc_id,m.mtrc_name"
				+ " from MTRC_MGMT_AUTH_NEW a join "
				+ " MTRC_METRIC_PERIOD p on a.mtrc_period_id = p.mtrc_period_id"
				+ " join  MTRC_METRIC m"
				+ " on p.mtrc_id = m.mtrc_id"
				+ " where a.muar_id = ?"
				+ " and getdate() between a.mma_eff_start_date and a.mma_eff_end_date";
		
		String bldgSQL = "select b.dsc_mtrc_lc_bldg_name,"
				+ " b.dsc_mtrc_lc_bldg_id,"
				+ " b.dsc_mtrc_lc_bldg_code"
				+ " from RZ_BLDG_AUTHORIZATION a,"
				+ " DSC_MTRC_LC_BLDG b"
				+ " where a.app_user_id = ? "
				+ " and a.dsc_mtrc_lc_bldg_id = b.dsc_mtrc_lc_bldg_id";

		try
		{
			conn = ConnectionManager.mtrcConn().getConnection();
		} 
		catch (Exception e)
		{
			e.printStackTrace();			
		}
		
		try 
		{
			
			ps = conn.prepareStatement(rolesSQL);				
			ps.setString(1, ssoId);
			ps.setString(2, ssoSystem);
			rs = ps.executeQuery();
			int num = 0;
			while(rs.next())
			{
				if(num == 0)
				{
					user.setSsoId(ssoId);
					user.setFullName(rs.getString("name"));
					user.setAppUserId(rs.getInt("userid"));
					user.setEmail(rs.getString("email"));
					num++;
				}
				Role role = new Role();
				role.setRoleId(rs.getInt("mar_id"));
				role.setRoleName(rs.getString("role"));
				role.setDescription(rs.getString("role_desc"));
				role.setProdId(rs.getInt("prod_id"));
				role.setProdName(rs.getString("prod_name"));	
				role.setRoleMetricPeriods(new ArrayList<RoleMetricPeriod>());
				mpPrepStmt = conn.prepareStatement(mpSQL);
				mpPrepStmt.setInt(1, rs.getInt("muar_id"));
				res = mpPrepStmt.executeQuery();	
				while(res.next())
				{
					RoleMetricPeriod mperiod = new RoleMetricPeriod();
					mperiod.setMetricPeriodId(res.getInt("mtrc_period_id"));
					mperiod.setMetricPeriodName(res.getString("mtrc_period_name"));
					mperiod.setMetricName(res.getString("mtrc_name"));
					mperiod.setMetricId(res.getInt("mtrc_id"));

					role.getRoleMetricPeriods().add(mperiod);				
				}//end of while
				res.close();
				//role.setMetricPeriods(mperiods);				
				roles.add(role);
				
			}//end of while
			rs.close();
			user.setRoles(roles);
			
			//at this point we're done with roles. now need to work on buildings
			bldgPrepStmt = conn.prepareStatement(bldgSQL);
			bldgPrepStmt.setInt(1, user.getAppUserId());
			rs = bldgPrepStmt.executeQuery();
			user.setBuildings(new ArrayList<Building>());
			while(rs.next())
			{				
				Building building = new Building();
				building.setBuildingId(rs.getInt("dsc_mtrc_lc_bldg_id"));
				building.setBuildingName(rs.getString("dsc_mtrc_lc_bldg_name"));
				building.setBuildingCode(rs.getString("dsc_mtrc_lc_bldg_code"));
				user.getBuilginds().add(building);
			}//end of while
			rs.close();
		} //end of try
		catch (Exception e)
		{
			
			e.printStackTrace();
		}//end  of catch
		finally
		{
			if(ps!=null)
			{
				try
				{
					ps.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			if(mpPrepStmt!=null)
			{
				try
				{
					mpPrepStmt.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			if(bldgPrepStmt!=null)
			{
				try
				{
					bldgPrepStmt.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			if(conn!=null)
			{
				try
				{
					conn.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			
			
		}//end of finally
		     	
		return user;
	}

	public String authorizeRZUser(String ssoId,String ssoSystem, String fullName, String email)
	{
		
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement updateFullNamePS = null;
		PreparedStatement validateRolePS = null;
		PreparedStatement getUserRolePS = null;
		PreparedStatement createUserRolePS = null;
		PreparedStatement createUserPS = null;
		ResultSet rs = null;
		String result = null;//this string will be returned to the called
		//ResultSetMetaData rsmd = res.getMetaData();
		
		
		String userValidationSQL = "select * from DSC_APP_USER where app_user_sso_id = ? and app_user_sso_system = ?";
		try
		{
			conn = ConnectionManager.mtrcConn().getConnection();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			return "Error:"+e.getMessage();
		}
		
		try
		{
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(userValidationSQL);
			ps.setString(1, ssoId);
			ps.setString(2, ssoSystem);
			rs = ps.executeQuery();
			if(rs.next())
			{
				
				if(rs.getString("app_user_disabled_yn").equals("Y"))
				{
					result = "Error: Red Zone User is Disabled";
				}
				else
				{
					int appUsrId = rs.getInt("app_user_id");
					if(rs.getString("app_user_full_name")==null||rs.getString("app_user_full_name").equals(""))
					{
						
						String updateUserNameSQL = "update DSC_APP_USER "
								+ " set app_user_full_name = ? "
								+ " where app_user_sso_id = ? "
								+ " and app_user_sso_system = ?";
					    updateFullNamePS = conn.prepareStatement(updateUserNameSQL);
						updateFullNamePS.setString(1, fullName);
						updateFullNamePS.setString(2, ssoId);
						updateFullNamePS.setString(3, ssoSystem);
						updateFullNamePS.executeUpdate();
						//need to update full name field
					}
					if(rs.getString("app_user_email_addr")==null||rs.getString("app_user_email_addr").equals(""))
					{
						String updateEmailSQL = "update DSC_APP_USER "
								+ " set app_user_email_addr = ? "
								+ " where app_user_sso_id = ? "
								+ " and app_user_sso_system = ?";
						PreparedStatement updateEmailPS = conn.prepareStatement(updateEmailSQL);
						updateEmailPS.setString(1, email);
						updateEmailPS.setString(2, ssoId);
						updateEmailPS.setString(3, ssoSystem);
						updateEmailPS.executeUpdate();
					}
					
					//at this point we're done with user validation.
					//now we need to validate user role
					
					String validateRoleSQL = "select active.cnt active_cnt, inactive.cnt inactive_cnt"
							+ " from ( select count(*) cnt from dsc_app_user u join"
							+ " MTRC_USER_APP_ROLES ap"
							+ " on u.app_user_id = ap.app_user_id  join"
							+ " MTRC_APP_ROLE r on ap.mar_id = r.mar_id join"
							+ " MTRC_PRODUCT prod on r.prod_id = prod.prod_id"
							+ " where u.app_user_sso_id = ?"
							+ " and u.app_user_sso_system = ?"
							+ " and prod.prod_name = ?"
							+ " and getdate() between ap.muar_eff_start_dt and ap.muar_eff_end_dt"
							+ " and getdate() between r.mar_eff_start_dt and r.mar_eff_end_dt)active,"
							+ " ( select count(*) cnt from dsc_app_user u join"
							+ " MTRC_USER_APP_ROLES ap"
							+ " on u.app_user_id = ap.app_user_id  join"
							+ " MTRC_APP_ROLE r on ap.mar_id = r.mar_id join"
							+ " MTRC_PRODUCT prod on r.prod_id = prod.prod_id"
							+ " where u.app_user_sso_id = ?"
							+ " and u.app_user_sso_system = ?"
							+ " and prod.prod_name = ?)inactive";

					validateRolePS = conn.prepareStatement(validateRoleSQL);
					validateRolePS.setString(1, ssoId);
					validateRolePS.setString(2, ssoSystem);
					validateRolePS.setString(3, "Red Zone");
					validateRolePS.setString(4, ssoId);
					validateRolePS.setString(5, ssoSystem);
					validateRolePS.setString(6, "Red Zone");
					ResultSet res = validateRolePS.executeQuery();
					int activeRoleCount = 0;
					int inactiveRoleCount = 0;
					while(res.next())
					{
						activeRoleCount = res.getInt("active_cnt");
						inactiveRoleCount = res.getInt("inactive_cnt");
					}
					
					//Here we need to check if user has active roles.
					//The approach is:
					//if user has active roles then simple return success
					//if user doesn't have active roles but has deactivated roles
					// then we assume that all of his RZ roles were disables,
					// and we return "No Active RZ roles error"
					// if user has no roles at all we need to assign RZ_USER role to him/her
					if(activeRoleCount>0)//user has role(s) assigned to him/her
					{
						result = "Success";
					}
					else if(inactiveRoleCount>0)
					{
						result = "User doesn't have active Red Zone role(s)";
					}
					else//need to create RZ_USER role for this user
					{
						String getRZ_UserIdSQL = "select mar_id from MTRC_APP_ROLE where mar_name = ?";
						getUserRolePS = conn.prepareStatement(getRZ_UserIdSQL);
						getUserRolePS.setString(1, "RZ_USER");
						res = getUserRolePS.executeQuery();
						int marId = 0;
						if(res.next())
						{
							marId = res.getInt("mar_id");
							String createUserRoleSQL = "insert into MTRC_USER_APP_ROLES (app_user_id,mar_id,muar_eff_start_dt,muar_eff_end_dt)"
									+ "values(?,?,getdate(),'12/31/2060')";
							
							createUserRolePS = conn.prepareStatement(createUserRoleSQL);
							createUserRolePS.setInt(1, appUsrId);
							createUserRolePS.setInt(2, marId);
							createUserRolePS.executeUpdate();			
							result = "Success";
							
						}//end if
						else
						{
						  result = "Error: RZ_USER role doesn't exist";
						}
						
						
					}//end  else
									
				}//end  else
				
				
				
				
			}//end of if(rs.next())
			else
			{
				//TO DO: logic to create user needs to go here
				String createUserSQL = "insert into DSC_APP_USER (app_user_sso_id,app_user_sso_system,app_user_email_addr,app_user_full_name,app_user_disabled_yn)"
						+ "values(?,?,?,?,'N')";
				
				createUserPS = conn.prepareStatement(createUserSQL, PreparedStatement.RETURN_GENERATED_KEYS);
				createUserPS.setString(1, ssoId);
				createUserPS.setString(2, ssoSystem);
				createUserPS.setString(3, email);
				createUserPS.setString(4, fullName);
				createUserPS.executeUpdate();
				rs = createUserPS.getGeneratedKeys();
				int appUserID =-1;
				if(rs != null && rs.next())
				{
				   System.out.println("Generated Id: "+rs.getInt(1));
				   appUserID = rs.getInt(1);
				}
				String getRZ_UserIdSQL = "select mar_id from MTRC_APP_ROLE where mar_name = ?";
				getUserRolePS = conn.prepareStatement(getRZ_UserIdSQL);
				getUserRolePS.setString(1, "RZ_USER");
				rs = getUserRolePS.executeQuery();
				int marId = 0;
				if(rs.next())
				{
					marId = rs.getInt("mar_id");
					String createUserRoleSQL = "insert into MTRC_USER_APP_ROLES (app_user_id,mar_id,muar_eff_start_dt,muar_eff_end_dt)"
							+ "values(?,?,getdate(),'12/31/2060')";
					
					createUserRolePS = conn.prepareStatement(createUserRoleSQL);
					createUserRolePS.setInt(1, appUserID);
					createUserRolePS.setInt(2, marId);
					createUserRolePS.executeUpdate();			
					result = "Success";
					
				}//end if
				else
				{
				  result = "Error: RZ_USER role doesn't exist";
				}
				rs.close();
				result = "Success";			
			}
			
			
		conn.commit();
		
		}//end of try
		catch(Exception e)
		{
			try
			{
				conn.rollback();
			}
			catch(SQLException e1)
			{
				e1.printStackTrace();
			}
			e.printStackTrace();
			result = "Error: "+e.getMessage();
			
		}
		finally
		{
			if(ps !=null)
			{
				try
				{
					ps.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(validateRolePS!=null)
			{
				try
				{
					validateRolePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(updateFullNamePS!=null)
			{
				try
				{
					updateFullNamePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(getUserRolePS!=null)
			{
				try
				{
					getUserRolePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(createUserRolePS!=null)
			{
				try
				{
					createUserRolePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(conn!=null)
			{
				try
				{
					conn.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
		}//end of finally
		return result;
	}

	public String authorizeRZDMUser(String ssoId,String ssoSystem, String fullName, String email)
	{
		
		Connection conn = null;
		PreparedStatement ps = null;
		
		PreparedStatement validateRolePS = null;
		PreparedStatement getUserRolePS = null;
		
		
		ResultSet rs = null;
		String result = null;//this string will be returned to the called
		//ResultSetMetaData rsmd = res.getMetaData();
		
		
		String userValidationSQL = "select * from DSC_APP_USER where app_user_sso_id = ? and app_user_sso_system = ?";
		try
		{
			conn = ConnectionManager.mtrcConn().getConnection();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			return "Error:"+e.getMessage();
		}
		
		try
		{
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(userValidationSQL);
			ps.setString(1, ssoId);
			ps.setString(2, ssoSystem);
			rs = ps.executeQuery();
			if(rs.next())
			{
				
				if(rs.getString("app_user_disabled_yn").equals("Y"))
				{
					result = "Error: Red Zone User is Disabled";
				}
				else
				{
					int appUsrId = rs.getInt("app_user_id");					
					
					//at this point we're done with user validation.
					//now we need to validate user role
					
					String validateRoleSQL = "select count(*) cnt"
							+ " from dsc_app_user u join"
							+ " MTRC_USER_APP_ROLES ap"
							+ " on u.app_user_id = ap.app_user_id  join"
							+ " MTRC_APP_ROLE r on ap.mar_id = r.mar_id join"
							+ " MTRC_PRODUCT prod on r.prod_id = prod.prod_id"
							+ " where u.app_user_sso_id = ?"
							+ " and u.app_user_sso_system = ?"
							+ " and prod.prod_token = ?"
							+ " and getdate() between ap.muar_eff_start_dt and ap.muar_eff_end_dt"
							+ " and getdate() between r.mar_eff_start_dt and r.mar_eff_end_dt";

					validateRolePS = conn.prepareStatement(validateRoleSQL);
					validateRolePS.setString(1, ssoId);
					validateRolePS.setString(2, ssoSystem);
					validateRolePS.setString(3, "MTRC_DM_TOOL");
					ResultSet res = validateRolePS.executeQuery();
					int roleCount = 0;
					while(res.next())
					{
						roleCount = res.getInt("cnt");
					}
					
					
					if(roleCount>0)//user has role(s) assigned to him/her
					{
						result = "Success";
					}
					else
					{
						  result = "User is not authorized to access this application";
				
					}//end  else
									
				}//end  else
				
				
				
				
			}//end of if(rs.next())
			
		
		}//end of try
		catch(Exception e)
		{
			try
			{
				conn.rollback();
			}
			catch(SQLException e1)
			{
				e1.printStackTrace();
			}
			e.printStackTrace();
			result = "Error: "+e.getMessage();
			
		}
		finally
		{
			if(ps !=null)
			{
				try
				{
					ps.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(validateRolePS!=null)
			{
				try
				{
					validateRolePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
			if(getUserRolePS!=null)
			{
				try
				{
					getUserRolePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}			
			if(conn!=null)
			{
				try
				{
					conn.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
		}//end of finally
		return result;
	}

    /*
     * This method call the local ldap authentication method instead of calling 
     * the one hosted on observation api server.
     * 
     * */
	public JSONObject authenticateDSCADUserForRZ(String username, String password, String domain) throws JSONException
	{
		Ldap ldap = new Ldap();
		return(ldap.authenticateLDAPUser(username, password,domain));
	}
}

