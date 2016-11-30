package com.dsc.mtrc.internal;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import com.dsc.mtrc.dao.ConnectionManager;


public class TaskManager {

	
	
	public Response getTasksSummary(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject(); 
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int userId = -1;
		
		String SQL = "select a.sub_count,b.rev_count,c.mtrc_count "
				+ " from"
				+ " (select count(RZ_BAP_METRICS.rz_bapm_id)  sub_count"
				+ " from RZ_BLDG_ACTION_PLAN,RZ_BAP_METRICS"
				+ " where RZ_BLDG_ACTION_PLAN.rz_bap_id = RZ_BAP_METRICS.rz_bap_id"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Ready For Review'"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Approved'"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Expired'"
				+ " and RZ_BLDG_ACTION_PLAN.dsc_mtrc_lc_bldg_id in"
				+ " (select RZ_BLDG_AUTHORIZATION.dsc_mtrc_lc_bldg_id from DSC_APP_USER "
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt "
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join RZ_BLDG_AUTHORIZATION on DSC_APP_USER.app_user_id = RZ_BLDG_AUTHORIZATION.app_user_id"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='RZ_AP_SUBMITTER'))a,"
				+ " (select count(RZ_BAP_METRICS.rz_bapm_id) rev_count  from MTRC_METRIC_PERIOD_VALUE,RZ_BAP_METRICS"
				+ " where MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id = RZ_BAP_METRICS.mtrc_period_val_id"
				+ " and RZ_BAP_METRICS.rz_bapm_status ='Ready For Review'"
				+ " and MTRC_METRIC_PERIOD_VALUE.mtrc_period_id in"
				+ " (select MTRC_MGMT_AUTH_NEW.mtrc_period_id from DSC_APP_USER "
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join MTRC_MGMT_AUTH_NEW on MTRC_USER_APP_ROLES.muar_id = MTRC_MGMT_AUTH_NEW.muar_id"
				+ " and getdate() between MTRC_MGMT_AUTH_NEW.mma_eff_start_date and MTRC_MGMT_AUTH_NEW.mma_eff_end_date"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='RZ_AP_REVIEWER'))b,"
				+ " (select count(RZ_MTRC_PERIOD_STATUS.rz_mps_id) mtrc_count  from"
				+ " RZ_MTRC_PERIOD_STATUS"
				+ " where RZ_MTRC_PERIOD_STATUS.rz_mps_status ='Open'"
				+ " and RZ_MTRC_PERIOD_STATUS.mtrc_period_id in"
				+ " (select MTRC_MGMT_AUTH_NEW.mtrc_period_id from DSC_APP_USER "
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join MTRC_MGMT_AUTH_NEW on MTRC_USER_APP_ROLES.muar_id = MTRC_MGMT_AUTH_NEW.muar_id"
				+ " and getdate() between MTRC_MGMT_AUTH_NEW.mma_eff_start_date and MTRC_MGMT_AUTH_NEW.mma_eff_end_date"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='MTRC_COLLECTOR'))c";

		if(!inputJsonObj.has("app_user_id")||inputJsonObj.getString("app_user_id")==null||inputJsonObj.getString("app_user_id").equals(""))
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "app_user_id parameter is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			userId = inputJsonObj.getInt("app_user_id");
		}
		try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		try
		{
		
			ps = conn.prepareStatement(SQL);
			ps.setInt(1, userId);
			ps.setInt(2, userId);
			ps.setInt(3, userId);
			rs = ps.executeQuery();
			int ap_count = 0;
			int mtrc_count = 0;
			while(rs.next())
			{
				ap_count = rs.getInt("sub_count")+rs.getInt("rev_count");
				mtrc_count = rs.getInt("mtrc_count");
			}
			retJson.put("result", "Success");
			retJson.put("act_plan_count",ap_count );
			retJson.put("mtrc_count", mtrc_count);
			rb = Response.ok(retJson.toString()).build();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			
		}
		finally
		{
			if(ps!=null)
			{
				try
				{
					ps.close();
				} 
				catch (SQLException e1)
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
				catch (SQLException e1)
				{					
					e1.printStackTrace();
				}
			}
		}//end of finally
		
		
		
		
		return rb;
		
	}

	public Response getTaskDetails(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject(); 
		JSONArray submitTasks = new JSONArray();
		JSONArray reviewTasks = new JSONArray();
		JSONArray dataTasks = new JSONArray();
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement ps1 = null;
		ResultSet rs = null;
		ResultSet res = null;
		int userId = -1;
		
		if(!inputJsonObj.has("app_user_id")||inputJsonObj.getString("app_user_id")==null||inputJsonObj.getString("app_user_id").equals(""))
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "app_user_id parameter is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			userId = inputJsonObj.getInt("app_user_id");
		}
		try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		String submitterSQL = "select Month(MTRC_TM_PERIODS.tm_per_end_dtm) ap_month,"
				+ " Year(MTRC_TM_PERIODS.tm_per_end_dtm) ap_year,"
				+ " MTRC_TM_PERIODS.tm_period_id,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_name,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_id,"
				+ " DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_name,"
				+ " DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id,"
				+ " MTRC_METRIC_PRODUCTS.mtrc_prod_display_text,"
				+ " MTRC_METRIC_PERIOD.mtrc_id,"
				+ " RZ_BAP_METRICS.rz_bapm_status,"
				+ " RZ_BAP_METRICS.rz_bapm_id,"
				+ " RZ_BAP_METRICS.mtrc_period_val_id"
				+ " from RZ_BLDG_ACTION_PLAN,RZ_BAP_METRICS,MTRC_METRIC_PERIOD_VALUE,MTRC_METRIC_PERIOD,MTRC_TM_PERIODS,DSC_MTRC_LC_BLDG,MTRC_METRIC_PRODUCTS,MTRC_PRODUCT "
				+ " where RZ_BLDG_ACTION_PLAN.rz_bap_id = RZ_BAP_METRICS.rz_bap_id"
				+ " and RZ_BAP_METRICS.mtrc_period_val_id = MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and MTRC_METRIC_PRODUCTS.prod_id = MTRC_PRODUCT.prod_id"
				+ " and MTRC_PRODUCT.prod_token ='RED_ZONE_WHS'"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Ready For Review'"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Approved'"
				+ " and RZ_BAP_METRICS.rz_bapm_status !='Expired'"
				+ " and RZ_BLDG_ACTION_PLAN.dsc_mtrc_lc_bldg_id in"
				+ " (select RZ_BLDG_AUTHORIZATION.dsc_mtrc_lc_bldg_id from DSC_APP_USER"
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join RZ_BLDG_AUTHORIZATION on DSC_APP_USER.app_user_id = RZ_BLDG_AUTHORIZATION.app_user_id"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='RZ_AP_SUBMITTER')";
		
		
		String reviewerSQL = "select Month(MTRC_TM_PERIODS.tm_per_end_dtm) ap_month,"
				+ " Year(MTRC_TM_PERIODS.tm_per_end_dtm) ap_year,"
				+ " MTRC_TM_PERIODS.tm_period_id,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_name,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_id,"
				+ " DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_name,"
				+ " DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id,"
				+ " MTRC_METRIC_PRODUCTS.mtrc_prod_display_text,"
				+ " MTRC_METRIC_PERIOD.mtrc_id,"
				+ " RZ_BAP_METRICS.rz_bapm_status,"
				+ " RZ_BAP_METRICS.rz_bapm_id"
				+ " from RZ_BLDG_ACTION_PLAN,RZ_BAP_METRICS,MTRC_METRIC_PERIOD_VALUE,MTRC_METRIC_PERIOD,MTRC_TM_PERIODS,DSC_MTRC_LC_BLDG,MTRC_METRIC_PRODUCTS,MTRC_PRODUCT"
				+ " where RZ_BLDG_ACTION_PLAN.rz_bap_id = RZ_BAP_METRICS.rz_bap_id "
				+ " and RZ_BAP_METRICS.mtrc_period_val_id = MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
				+ " and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and MTRC_METRIC_PRODUCTS.prod_id = MTRC_PRODUCT.prod_id"
				+ " and MTRC_PRODUCT.prod_token ='RED_ZONE_WHS'"
				+ " and RZ_BAP_METRICS.rz_bapm_status ='Ready For Review'"
				+ " and MTRC_METRIC_PERIOD_VALUE.mtrc_period_id in"
				+ " (select MTRC_MGMT_AUTH_NEW.mtrc_period_id from DSC_APP_USER"
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join MTRC_MGMT_AUTH_NEW on MTRC_USER_APP_ROLES.muar_id = MTRC_MGMT_AUTH_NEW.muar_id"
				+ " and getdate() between MTRC_MGMT_AUTH_NEW.mma_eff_start_date and MTRC_MGMT_AUTH_NEW.mma_eff_end_date"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='RZ_AP_REVIEWER')";

		String metricSQL = "select Month(MTRC_TM_PERIODS.tm_per_end_dtm) ap_month,"
				+ " Year(MTRC_TM_PERIODS.tm_per_end_dtm) ap_year,"
				+ " MTRC_TM_PERIODS.tm_period_id,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_name,"
				+ " MTRC_METRIC_PERIOD.mtrc_period_id,"
				+ " MTRC_METRIC_PRODUCTS.mtrc_prod_display_text,"
				+ " MTRC_METRIC_PERIOD.mtrc_id"
				+ " from RZ_MTRC_PERIOD_STATUS,MTRC_METRIC_PERIOD,MTRC_TM_PERIODS,MTRC_METRIC_PRODUCTS,MTRC_PRODUCT"
				+ " where RZ_MTRC_PERIOD_STATUS.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and RZ_MTRC_PERIOD_STATUS.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
				+ " and MTRC_METRIC_PRODUCTS.prod_id = MTRC_PRODUCT.prod_id"
				+ " and MTRC_PRODUCT.prod_token ='RED_ZONE_WHS'"
				+ " and RZ_MTRC_PERIOD_STATUS.rz_mps_status ='Open'"
				+ " and RZ_MTRC_PERIOD_STATUS.mtrc_period_id in"
				+ " (select MTRC_MGMT_AUTH_NEW.mtrc_period_id from DSC_APP_USER"
				+ " join MTRC_USER_APP_ROLES on DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and getdate() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " join MTRC_APP_ROLE on MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and getdate() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " join MTRC_MGMT_AUTH_NEW on MTRC_USER_APP_ROLES.muar_id = MTRC_MGMT_AUTH_NEW.muar_id"
				+ " and getdate() between MTRC_MGMT_AUTH_NEW.mma_eff_start_date and MTRC_MGMT_AUTH_NEW.mma_eff_end_date"
				+ " where DSC_APP_USER.app_user_id = ?"
				+ " and MTRC_APP_ROLE.mar_name ='MTRC_COLLECTOR')"
				+ " order by Year(MTRC_TM_PERIODS.tm_per_end_dtm), Month(MTRC_TM_PERIODS.tm_per_end_dtm)";
		
		String periodStatus = "select count(*) cnt from MTRC_METRIC_PERIOD_VALUE where tm_period_id = ? and mtrc_period_id = ?";


		try
		{
			ps = conn.prepareStatement(submitterSQL);
			ps.setInt(1, userId);			
			rs = ps.executeQuery();
			while(rs.next())
			{
				JSONObject sTask = new JSONObject();
				sTask.put("month", rs.getInt("ap_month"));
				sTask.put("year", rs.getInt("ap_year"));
				sTask.put("tm_period_id", rs.getInt("tm_period_id"));
				sTask.put("period", rs.getString("mtrc_period_name"));
				sTask.put("mtrc_period_id", rs.getInt("mtrc_period_id"));
				sTask.put("building", rs.getString("dsc_mtrc_lc_bldg_name"));
				sTask.put("dsc_mtrc_lc_bldg_id", rs.getInt("dsc_mtrc_lc_bldg_id"));			
				sTask.put("mtrc_id", rs.getInt("mtrc_id"));
				sTask.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				sTask.put("status", rs.getString("rz_bapm_status"));
				sTask.put("rz_bapm_id", rs.getInt("rz_bapm_id"));
				sTask.put("mtrc_period_val_id", rs.getInt("mtrc_period_val_id"));
				submitTasks.put(sTask);
			}
			ps.close();
			ps = conn.prepareStatement(reviewerSQL);
			ps.setInt(1, userId);			
			rs = ps.executeQuery();
			while(rs.next())
			{
				JSONObject rTask = new JSONObject();
				rTask.put("month", rs.getInt("ap_month"));
				rTask.put("year", rs.getInt("ap_year"));
				rTask.put("tm_period_id", rs.getInt("tm_period_id"));
				rTask.put("period", rs.getString("mtrc_period_name"));
				rTask.put("mtrc_period_id", rs.getInt("mtrc_period_id"));
				rTask.put("building", rs.getString("dsc_mtrc_lc_bldg_name"));
				rTask.put("dsc_mtrc_lc_bldg_id", rs.getInt("dsc_mtrc_lc_bldg_id"));				
				rTask.put("mtrc_id", rs.getInt("mtrc_id"));
				rTask.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				rTask.put("status", rs.getString("rz_bapm_status"));
				rTask.put("rz_bapm_id", rs.getInt("rz_bapm_id"));

				reviewTasks.put(rTask);
			}
			ps.close();
			ps = conn.prepareStatement(metricSQL);
			ps.setInt(1, userId);			
			rs = ps.executeQuery();
			while(rs.next())
			{
				JSONObject mTask = new JSONObject();
				mTask.put("month", rs.getInt("ap_month"));
				mTask.put("year", rs.getInt("ap_year"));
				mTask.put("tm_period_id", rs.getInt("tm_period_id"));
				mTask.put("period", rs.getString("mtrc_period_name"));		
				mTask.put("mtrc_period_id", rs.getInt("mtrc_period_id"));
				mTask.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				mTask.put("mtrc_id", rs.getInt("mtrc_id"));
				ps1 = conn.prepareStatement(periodStatus);
				ps1.setInt(1, rs.getInt("tm_period_id"));
				ps1.setInt(2, rs.getInt("mtrc_period_id"));
				res = ps1.executeQuery();
				while(res.next())
				{
					if(res.getInt("cnt")==0)
					{
						mTask.put("status", "Not Started");
					}
					if(res.getInt("cnt")>0)
					{
						mTask.put("status", "Not Published");
					}
				}
				res.close();
				
				dataTasks.put(mTask);
			}//end of while
			rs.close();
			retJson.put("result", "Success");
			retJson.put("submittertasks", submitTasks);
			retJson.put("reviewertasks", reviewTasks);
			retJson.put("datacollectortasks", dataTasks);
			rb = Response.ok(retJson.toString()).build();
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
		}
		finally
		{
			if(ps!=null)
			{
				try
				{
					ps.close();
				} 
				catch (SQLException e1)
				{					
					e1.printStackTrace();
				}
			}
			if(ps1!=null)
			{
				try
				{
					ps1.close();
				} 
				catch (SQLException e1)
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
				catch (SQLException e1)
				{					
					e1.printStackTrace();
				}
			}
		}//end of finally






		
		return rb;
		
	}

	public Response getMyTeamActivities(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		String prodName = null;
		int userId = -1;
		
		
		if(!inputJsonObj.has("productname"))
		{				
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "productname is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			prodName = inputJsonObj.getString("productname");
		}
		if(inputJsonObj.has("app_user_id") && inputJsonObj.getString("app_user_id")!=null && !inputJsonObj.getString("app_user_id").equals(""))
		{	

			userId = inputJsonObj.getInt("app_user_id");
					
			
		}
		else
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "app_user_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
	    if(inputJsonObj.has("begmonth") && inputJsonObj.getString("begmonth")!=null && !inputJsonObj.getString("begmonth").equals("")
				&& inputJsonObj.has("begyear") && inputJsonObj.getString("begyear")!=null && !inputJsonObj.getString("begyear").equals("")
				&& inputJsonObj.has("endmonth") && inputJsonObj.getString("endmonth")!=null && !inputJsonObj.getString("endmonth").equals("")
				&& inputJsonObj.has("endyear") && inputJsonObj.getString("endyear")!=null && !inputJsonObj.getString("endyear").equals(""))
		{
			
			int begMonth = inputJsonObj.getInt("begmonth");
			int begYear = inputJsonObj.getInt("begyear");
			int endMonth = inputJsonObj.getInt("endmonth");
			int endYear = inputJsonObj.getInt("endyear");
			
			if(endYear-begYear>1)
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Date Range has to be less than 1 year");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
			if(endYear-begYear==1&&begMonth<endMonth)
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Date Range has to be less than 1 year");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
			
			rb = Response.ok(lookupMyTeamActivities(prodName, begMonth,begYear,endMonth,endYear,userId).toString()).build();
		}
		else
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Start and End Dates required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		return rb;
	}
	public JSONObject lookupMyTeamActivities(String prodName,int begMonth,int begYear,int endMonth,int endYear, int userId) throws JSONException
	{
		JSONObject retJson = new JSONObject();
		JSONArray actionPlans = new JSONArray();
	
		
		Connection conn = null;
		PreparedStatement apPS = null;// stmt for action plans headers query		
		PreparedStatement apdPS = null;
		ResultSet rs = null;
		DecimalFormat df2 = new DecimalFormat("0.00");
        df2.setRoundingMode(RoundingMode.UP);
        
        String from = begMonth+"/01/"+begYear;
        String to = endMonth+"/01/"+endYear;
     
        String apSQL = "select  m.rz_bapm_id,m.rz_bap_id, m.mtrc_period_val_id,mpv.mtrc_period_val_value,mpv.dsc_mtrc_lc_bldg_id,mpv.mtrc_period_id,"
				+ " (coalesce(MTRC_MPBG.mpbg_display_text, MTRC_MPG.mpg_display_text)) as goal_txt,MTRC_DATA_TYPE.data_type_token,"
				+ " m.rz_bapm_status,m.rz_bapm_status_updt_dtm,m.rz_bapm_created_on_dtm,m.rz_bapm_ntfy_dtm,"
				+ " m.rz_bapm_approved_on_dtm,"				
				+ " bldg.dsc_mtrc_lc_bldg_name, mprod.mtrc_prod_display_text,"
				+ " month(tp.tm_per_start_dtm) as month, year(tp.tm_per_start_dtm) as year"
				+ " from rz_bap_metrics m"
				+ " inner join MTRC_METRIC_PERIOD_VALUE mpv"
				+ " on m.mtrc_period_val_id = mpv.mtrc_period_val_id"
				+ " inner join DSC_MTRC_LC_BLDG bldg"
				+ " on mpv.dsc_mtrc_lc_bldg_id = bldg.dsc_mtrc_lc_bldg_id"
				+ " inner join MTRC_METRIC_PERIOD mp"
				+ " on mpv.mtrc_period_id = mp.mtrc_period_id"
				+ " inner join MTRC_METRIC"				
				+ " on mp.mtrc_id = MTRC_METRIC.mtrc_id"
				+ " inner join MTRC_DATA_TYPE"
				+ " on MTRC_METRIC.data_type_id =MTRC_DATA_TYPE.data_type_id"
				+ " inner join MTRC_METRIC_PRODUCTS mprod"
				+ " on mp.mtrc_period_id = mprod.mtrc_period_id"
				+ " inner join MTRC_PRODUCT prod"
				+ " on mprod.prod_id = prod.prod_id"
				+ " and prod.prod_name = ?"
				+ " inner join MTRC_TM_PERIODS tp"
				+ " on mpv.tm_period_id = tp.tm_period_id"				
				+ " left outer join MTRC_MPBG"
				+ " on mp.mtrc_period_id =  MTRC_MPBG.mtrc_period_id"
				+ " and mpv.dsc_mtrc_lc_bldg_id = MTRC_MPBG.dsc_mtrc_lc_bldg_id	"
				+ " and prod.prod_id = MTRC_MPBG.prod_id"
				+ " and tp.tm_per_start_dtm between  MTRC_MPBG.mpbg_start_eff_dtm and MTRC_MPBG.mpbg_end_eff_dtm"
				+ " and tp.tm_per_end_dtm between  MTRC_MPBG.mpbg_start_eff_dtm and MTRC_MPBG.mpbg_end_eff_dtm"
				+ " left outer join MTRC_MPG"
				+ " on mp.mtrc_period_id = MTRC_MPG.mtrc_period_id"
				+ " and prod.prod_id = MTRC_MPG.prod_id"
				+ " and tp.tm_per_start_dtm between MTRC_MPG.mpg_start_eff_dtm and MTRC_MPG.mpg_end_eff_dtm"
				+ " and tp.tm_per_end_dtm between MTRC_MPG.mpg_start_eff_dtm and MTRC_MPG.mpg_end_eff_dtm"
				+ " where tp.tm_per_start_dtm between ? and ?"
				+ " and mpv.dsc_mtrc_lc_bldg_id in"
				+ " (select distinct RZ_BLDG_AUTHORIZATION.dsc_mtrc_lc_bldg_id"
                + " from RZ_BLDG_AUTHORIZATION,MTRC_APP_ROLE,MTRC_USER_APP_ROLES,MTRC_PRODUCT,DSC_APP_USER"
                + " where MTRC_USER_APP_ROLES.app_user_id = RZ_BLDG_AUTHORIZATION.app_user_id"
                + " and MTRC_USER_APP_ROLES.app_user_id = DSC_APP_USER.app_user_id"
                + " and MTRC_APP_ROLE.mar_id = MTRC_USER_APP_ROLES.mar_id"
                + " and MTRC_APP_ROLE.prod_id = MTRC_PRODUCT.prod_id"
                + " and MTRC_PRODUCT.prod_name =  ?"
                + " and MTRC_APP_ROLE.mar_name = 'RZ_BLDG_USER'"
                + " and DSC_APP_USER.app_user_disabled_yn = 'N'"
                + " and GETDATE() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
                + " and GETDATE() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
                + " and MTRC_USER_APP_ROLES.app_user_id = ?) ";
				        
        String apDetailsSQL = "select d.rz_apd_id,"
        		+ " d.rz_apd_subm_app_user_id,"
        		+ " d.rz_apd_revw_app_user_id,"
        		+ " d.rz_apd_ap_ver,d.rz_apd_ap_created_on_dtm,d.rz_apd_ap_last_saved_on_dtm,d.rz_apd_ap_submitted_on_dtm,	"
        		+ " d.rz_apd_ap_status,d.rz_apd_ap_stat_upd_on_dtm,d.rz_apd_ap_text,d.rz_apd_ap_review_text,"
        		+ " su.app_user_sso_id as submittedby,ru.app_user_sso_id as reviewedby"
        		+ " from rz_bap_metrics m "
        		+ " left outer join rz_action_plan_dtl d"
        		+ " on m.rz_bapm_id = d.rz_bapm_id"
        		+ " left outer join dsc_app_user su"
        		+ " on d.rz_apd_subm_app_user_id = su.app_user_id"
        		+ " left outer join dsc_app_user ru"
        		+ " on d.rz_apd_revw_app_user_id = ru.app_user_id"
        		+ " where m.rz_bapm_id= ?"
        		+ " and d.rz_apd_ap_ver = "
        		+ "(select max(RZ_ACTION_PLAN_DTL.rz_apd_ap_ver) max_ver "
        		+ "from RZ_ACTION_PLAN_DTL where RZ_ACTION_PLAN_DTL.rz_bapm_id = ?)";
					

        
        try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");					
			return retJson;
		}
        
        try
        {
        	
        	apPS = conn.prepareStatement(apSQL);
        	apPS.setString(1, prodName);
        	apPS.setString(2, from);
        	apPS.setString(3, to);
        	apPS.setString(4, prodName);
        	apPS.setInt(5, userId);       	
        	rs = apPS.executeQuery();
			int value_id = 0;

        	while(rs.next())
        	{
        		JSONObject actPlan = new JSONObject();       			
    			value_id = rs.getInt("mtrc_period_val_id");
    			int bapmId = rs.getInt("rz_bapm_id");
				actPlan.put("month", rs.getInt("month"));
				actPlan.put("year", rs.getInt("year"));
				actPlan.put("dsc_mtrc_lc_bldg_name", rs.getString("dsc_mtrc_lc_bldg_name"));
				actPlan.put("dsc_mtrc_lc_bldg_id", rs.getInt("dsc_mtrc_lc_bldg_id"));
				actPlan.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				actPlan.put("mtrc_period_id", rs.getInt("mtrc_period_id"));
				String value = rs.getString("mtrc_period_val_value");
				
				if(rs.getString("data_type_token").equals("pct"))
				{
					double valueNum = Double.parseDouble(value)*100;
					value = df2.format(valueNum);
					actPlan.put("mtrc_period_val_value", value);
				}
				else
				{
					actPlan.put("mtrc_period_val_value", value);
				}
				actPlan.put("goal_txt", rs.getString("goal_txt"));
				actPlan.put("mtrc_period_val_id", value_id);
				actPlan.put("rz_bapm_status", rs.getString("rz_bapm_status"));
				actPlan.put("rz_bapm_id",rs.getInt("rz_bapm_id"));				
				actPlan.put("rz_bapm_created_on_dtm", rs.getTimestamp("rz_bapm_created_on_dtm"));
				actPlan.put("rz_bapm_status_updt_dtm", rs.getTimestamp("rz_bapm_status_updt_dtm"));
				actPlan.put("rz_bapm_ntfy_dtm", rs.getTimestamp("rz_bapm_ntfy_dtm"));
				actPlan.put("rz_bapm_approved_on_dtm", rs.getTimestamp("rz_bapm_approved_on_dtm"));
				SortedSet<Timestamp> set = new TreeSet<Timestamp>();
				if(rs.getTimestamp("rz_bapm_created_on_dtm")!=null)set.add(rs.getTimestamp("rz_bapm_created_on_dtm"));
				if(rs.getTimestamp("rz_bapm_status_updt_dtm")!=null)set.add(rs.getTimestamp("rz_bapm_status_updt_dtm"));
				if(rs.getTimestamp("rz_bapm_approved_on_dtm")!=null)set.add(rs.getTimestamp("rz_bapm_approved_on_dtm"));
				//System.out.println(apDetailsSQL);
				apdPS = conn.prepareStatement(apDetailsSQL);
				apdPS.setInt(1, bapmId);
				apdPS.setInt(2, bapmId);
				ResultSet res = apdPS.executeQuery();
				
				while(res.next())
				{
					if(res.getString("rz_apd_id")!=null)
					{
						
						if(res.getTimestamp("rz_apd_ap_created_on_dtm")!=null)set.add(res.getTimestamp("rz_apd_ap_created_on_dtm"));
						if(res.getTimestamp("rz_apd_ap_last_saved_on_dtm")!=null)set.add(res.getTimestamp("rz_apd_ap_last_saved_on_dtm"));
						if(res.getTimestamp("rz_apd_ap_stat_upd_on_dtm")!=null)set.add(res.getTimestamp("rz_apd_ap_stat_upd_on_dtm"));
						if(res.getTimestamp("rz_apd_ap_submitted_on_dtm")!=null)set.add(res.getTimestamp("rz_apd_ap_submitted_on_dtm"));						
						actPlan.put("rz_apd_id", res.getInt("rz_apd_id"));
						actPlan.put("rz_apd_ap_ver", res.getInt("rz_apd_ap_ver"));
						actPlan.put("rz_apd_subm_app_user_id", res.getInt("rz_apd_subm_app_user_id"));
						actPlan.put("rz_apd_revw_app_user_id", res.getInt("rz_apd_revw_app_user_id"));
						actPlan.put("rz_apd_ap_created_on_dtm", res.getTimestamp("rz_apd_ap_created_on_dtm"));
						actPlan.put("rz_apd_ap_last_saved_on_dtm", res.getTimestamp("rz_apd_ap_last_saved_on_dtm"));
						actPlan.put("rz_apd_ap_submitted_on_dtm", res.getTimestamp("rz_apd_ap_submitted_on_dtm"));
						actPlan.put("rz_apd_ap_stat_upd_on_dtm", res.getTimestamp("rz_apd_ap_stat_upd_on_dtm"));										
						actPlan.put("submittedby", res.getString("submittedby"));
						actPlan.put("reviewedby", res.getString("reviewedby"));
												
					}						
				}
				actPlan.put("lastactivity_on_dtm", set.last());
				actionPlans.put(actPlan);
				res.close();
        	}// end of while(rs.next())
        	rs.close();
        	
        	retJson.put("result", "Success");
			retJson.put("actionplans", actionPlans);        	        	
        }//end of try
        catch(Exception e)
        {
        	e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
        }//end of catch
        finally
        {
        	if(apPS!=null)
			{
				try{apPS.close();} catch(Exception e){e.printStackTrace();}
			}			
			if(conn!=null)
			{
				try{conn.close();} catch(Exception e){e.printStackTrace();}
			}       	
        }//end of finally
        		
		return retJson;
	}
	
	
	public Response getUsersForAp(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		JSONArray users = new JSONArray();
			
		Connection conn = null;
		PreparedStatement ps = null;	
		ResultSet rs = null;
		
		int bapmId = -1; //action plan id
		int bldgId = -1;//building id
		int mpId = -1;//metric period id
		String status = null;// action plan status
		
		//validating input JSON to make sure Action Plan id is submitted
		if(!inputJsonObj.has("rz_bapm_id")||inputJsonObj.getString("rz_bapm_id")==null||inputJsonObj.equals(""))
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "rz_bapm_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			bapmId = inputJsonObj.getInt("rz_bapm_id");
		}
		try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		// action plan status check
		String apStatusSQL ="select RZ_BAP_METRICS.rz_bapm_status, MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id,"
				+ " MTRC_METRIC_PERIOD_VALUE.mtrc_period_id"
				+ " from RZ_BAP_METRICS,MTRC_METRIC_PERIOD_VALUE"
				+ " where RZ_BAP_METRICS.mtrc_period_val_id = MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id"
				+ " and RZ_BAP_METRICS.rz_bapm_id = ? ";
		
		//sql for submitter
		String submittersSQL = "select coalesce(DSC_APP_USER.app_user_full_name,DSC_APP_USER.app_user_sso_id) username,"
				+ " DSC_APP_USER.app_user_id,DSC_APP_USER.app_user_email_addr"
				+ " from RZ_BLDG_AUTHORIZATION,DSC_APP_USER, MTRC_USER_APP_ROLES,MTRC_APP_ROLE"
				+ " where RZ_BLDG_AUTHORIZATION.app_user_id = DSC_APP_USER.app_user_id"
				+ " and DSC_APP_USER.app_user_id = MTRC_USER_APP_ROLES.app_user_id"
				+ " and MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and MTRC_APP_ROLE.mar_name = 'RZ_AP_SUBMITTER'"
				+ " and RZ_BLDG_AUTHORIZATION.dsc_mtrc_lc_bldg_id= ? "
				+ " and DSC_APP_USER.app_user_disabled_yn = 'N' "
				+ " and GETDATE() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt";
 


		//sql for reviewer 
		String revirewerSQL = "select coalesce(DSC_APP_USER.app_user_full_name,DSC_APP_USER.app_user_sso_id) username,"
				+ " DSC_APP_USER.app_user_id,DSC_APP_USER.app_user_email_addr"
				+ " from MTRC_MGMT_AUTH_NEW,DSC_APP_USER, MTRC_USER_APP_ROLES,MTRC_APP_ROLE"
				+ " where MTRC_MGMT_AUTH_NEW.muar_id = MTRC_USER_APP_ROLES.muar_id"
				+ " and MTRC_USER_APP_ROLES.app_user_id = DSC_APP_USER.app_user_id"
				+ " and MTRC_USER_APP_ROLES.mar_id = MTRC_APP_ROLE.mar_id"
				+ " and MTRC_APP_ROLE.mar_name = 'RZ_AP_REVIEWER'"
				+ " and MTRC_MGMT_AUTH_NEW.mtrc_period_id= ? "
				+ " and DSC_APP_USER.app_user_disabled_yn = 'N' "
				+ " and GETDATE() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " and GETDATE() between MTRC_MGMT_AUTH_NEW.mma_eff_start_date and MTRC_MGMT_AUTH_NEW.mma_eff_end_date";
		
		
		try
		{
			//first we need to figure out what is the status of the action plan
			
			ps = conn.prepareStatement(apStatusSQL);
			ps.setInt(1, bapmId);
			rs = ps.executeQuery();
			while(rs.next())
			{
				status = rs.getString("rz_bapm_status");
				bldgId = rs.getInt("dsc_mtrc_lc_bldg_id");
				mpId = rs.getInt("mtrc_period_id");				
			}//end of while
			ps.close();
			rs.close();
			
			//If status is expired of approved we just simply return the message to the user
			// since in these cases we can't determine who's responsible for this action plan
			if(status.equals("Approved"))
			{
				retJson.put("result", "Success");				
				retJson.put("message", "Action Plan is Approved. No Action Required");
				rb = Response.ok(retJson.toString()).build();
			}
			if(status.equals("Expired"))
			{
				retJson.put("result", "Success");				
				retJson.put("message", "Action Plan is Expired. No Action Available");
				rb = Response.ok(retJson.toString()).build();
			}
			// if it hasn't been submitted or it was rejected, we need to return the list of users
			// whose role is rz_submitter(users responsible for the building )
			if(status.equals("Not Started")||status.equals("WIP")||status.equals("Rejected"))
			{				
				ps = conn.prepareStatement(submittersSQL);
				ps.setInt(1, bldgId);
				rs = ps.executeQuery();
				while(rs.next())
				{					
					JSONObject user = new JSONObject();
					user.put("username", rs.getString("username"));
					user.put("email", rs.getString("app_user_email_addr"));
					user.put("app_user_id", rs.getInt("app_user_id"));
					users.put(user);					
				}//end of while
				rs.close();
				retJson.put("result", "Success");
				retJson.put("users", users);
				
				rb = Response.ok(retJson.toString()).build();								
			}
			//if action plan is submitted and waiting for the review, we need to return the users who
			// can perform the review (rz_reviewer )
			if(status.equals("Ready For Review"))
			{
				ps = conn.prepareStatement(revirewerSQL);
				ps.setInt(1, mpId);
				rs = ps.executeQuery();
				while(rs.next())
				{
					JSONObject user = new JSONObject();
					user.put("username", rs.getString("username"));
					user.put("email", rs.getString("app_user_email_addr"));
					user.put("app_user_id", rs.getInt("app_user_id"));
					users.put(user);											
				}//end of while
				rs.close();
				System.out.println("Size ="+users.length());				
				retJson.put("result", "Success");
				retJson.put("users", users);
				rb = Response.ok(retJson.toString()).build();				
			}						
		}// end of try
		catch(Exception e)
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error:"+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
			e.printStackTrace();
		}
		finally
		{//need to close connection and all prepared statements
			if(ps!=null)
			{
				try{
					ps.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(conn!=null)
			{
				try{
					conn.close();
				}
				catch(Exception e1){
					e1.printStackTrace();
				}
			}
		}
	
		return rb;
	}
	
	public Response getMyTeamActivitySummary(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		String prodName = null;
		int userId = -1;
		
		
		if(!inputJsonObj.has("productname"))
		{				
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "productname is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			prodName = inputJsonObj.getString("productname");
		}
		if(inputJsonObj.has("app_user_id") && inputJsonObj.getString("app_user_id")!=null && !inputJsonObj.getString("app_user_id").equals(""))
		{	

			userId = inputJsonObj.getInt("app_user_id");
					
			
		}
		else
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "app_user_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
	    if(inputJsonObj.has("begmonth") && inputJsonObj.getString("begmonth")!=null && !inputJsonObj.getString("begmonth").equals("")
				&& inputJsonObj.has("begyear") && inputJsonObj.getString("begyear")!=null && !inputJsonObj.getString("begyear").equals("")
				&& inputJsonObj.has("endmonth") && inputJsonObj.getString("endmonth")!=null && !inputJsonObj.getString("endmonth").equals("")
				&& inputJsonObj.has("endyear") && inputJsonObj.getString("endyear")!=null && !inputJsonObj.getString("endyear").equals(""))
		{
			
			int begMonth = inputJsonObj.getInt("begmonth");
			int begYear = inputJsonObj.getInt("begyear");
			int endMonth = inputJsonObj.getInt("endmonth");
			int endYear = inputJsonObj.getInt("endyear");
			
			if(endYear-begYear>1)
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Date Range has to be less than 1 year");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
			if(endYear-begYear==1&&begMonth<endMonth)
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Date Range has to be less than 1 year");
				rb = Response.ok(retJson.toString()).build();			
				return rb;
			}
			
			rb = Response.ok(lookupTeamActivitySummary(prodName, begMonth,begYear,endMonth,endYear,userId).toString()).build();
		}
		else
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Start and End Dates required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		return rb;
	}
	
	public JSONObject lookupTeamActivitySummary(String prodName,int begMonth,int begYear,int endMonth,int endYear, int userId) throws JSONException
	{
		JSONObject retJson = new JSONObject();
		JSONArray activities = new JSONArray();
	
		
		Connection conn = null;
		PreparedStatement validatePS = null;// stmt for action plans headers query		
		PreparedStatement bldgPS = null;
		PreparedStatement countPS = null;
		ResultSet rs = null;
		int bldgId = -1;//building id
		String bldgName = null;
		String from = begMonth+"/01/"+begYear;
        String to = endMonth+"/01/"+endYear;
		
		try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
						
			return retJson;
		}
		
		String validateUserSQL = "select count(*)is_bldg_user"
				+ " from MTRC_APP_ROLE,MTRC_USER_APP_ROLES,MTRC_PRODUCT,DSC_APP_USER"
				+ " where  MTRC_APP_ROLE.mar_id = MTRC_USER_APP_ROLES.mar_id"
				+ " and MTRC_USER_APP_ROLES.app_user_id = DSC_APP_USER.app_user_id"
				+ " and MTRC_APP_ROLE.prod_id = MTRC_PRODUCT.prod_id"
				+ " and MTRC_PRODUCT.prod_name =  ?"
				+ " and MTRC_APP_ROLE.mar_name = 'RZ_BLDG_USER'"
				+ " and MTRC_USER_APP_ROLES.app_user_id = ?"
				+ " and GETDATE() between MTRC_USER_APP_ROLES.muar_eff_start_dt and MTRC_USER_APP_ROLES.muar_eff_end_dt"
				+ " and GETDATE() between MTRC_APP_ROLE.mar_eff_start_dt and MTRC_APP_ROLE.mar_eff_end_dt"
				+ " and DSC_APP_USER.app_user_disabled_yn = 'N'";
	
	  String bldgSQL = "select DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_name,DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id"
	  		+ " from RZ_BLDG_AUTHORIZATION,DSC_MTRC_LC_BLDG"
	  		+ " where RZ_BLDG_AUTHORIZATION.dsc_mtrc_lc_bldg_id = DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id and RZ_BLDG_AUTHORIZATION.app_user_id = ?";
		
		
	  String summarySQL = "select a.sub_count,b.rev_count "
	  		+ " from"
	  		+ " (select count(RZ_BAP_METRICS.rz_bapm_id)  sub_count"
	  		+ " from RZ_BAP_METRICS,MTRC_METRIC_PERIOD_VALUE,MTRC_TM_PERIODS"
	  		+ " where  RZ_BAP_METRICS.mtrc_period_val_id = MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id"
	  		+ " and MTRC_METRIC_PERIOD_VALUE.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
	  		+ " and RZ_BAP_METRICS.rz_bapm_status !='Ready For Review'"
	  		+ " and RZ_BAP_METRICS.rz_bapm_status !='Approved'"
	  		+ " and RZ_BAP_METRICS.rz_bapm_status !='Expired'"
	  		+ " and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = ?"
	  		+ " and MTRC_TM_PERIODS.tm_per_start_dtm between ? and ?)a,"
	  		+ " (select count(RZ_BAP_METRICS.rz_bapm_id)  rev_count"
	  		+ " from RZ_BAP_METRICS,MTRC_METRIC_PERIOD_VALUE,MTRC_TM_PERIODS"
	  		+ " where  RZ_BAP_METRICS.mtrc_period_val_id = MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id"
	  		+ " and MTRC_METRIC_PERIOD_VALUE.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
	  		+ " and RZ_BAP_METRICS.rz_bapm_status ='Ready For Review'"
	  		+ " and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = ?"
	  		+ " and MTRC_TM_PERIODS.tm_per_start_dtm between ? and ?)b";
  				  

	  try
		{
			validatePS = conn.prepareStatement(validateUserSQL);			
			validatePS.setString(1, prodName);
			validatePS.setInt(2,userId);
			rs = validatePS.executeQuery();
			boolean isValidUser = false;
			while(rs.next())
			{
				isValidUser = rs.getInt("is_bldg_user")==0?false:true;
			}
			rs.close();
			if(isValidUser)
			{
				bldgPS = conn.prepareStatement(bldgSQL);
				bldgPS.setInt(1, userId);
				rs = bldgPS.executeQuery();
				while(rs.next())
				{
					JSONObject activity = new JSONObject();
					bldgId = rs.getInt("dsc_mtrc_lc_bldg_id");
					bldgName = rs.getString("dsc_mtrc_lc_bldg_name");
					activity.put("dsc_mtrc_lc_bldg_id", bldgId);
					activity.put("dsc_mtrc_lc_bldg_name", bldgName);
					countPS = conn.prepareStatement(summarySQL);
					countPS.setInt(1, bldgId);
					countPS.setString(2, from);
					countPS.setString(3, to);
					countPS.setInt(4, bldgId);
					countPS.setString(5, from);
					countPS.setString(6, to);
					ResultSet res = countPS.executeQuery();
					while(res.next())
					{
						activity.put("submit_count", res.getInt("sub_count"));
						activity.put("review_count", res.getInt("rev_count"));
					}//end of res while
					activities.put(activity);

				}//end of rs while
				retJson.put("result", "Success");
				retJson.put("tasks", activities);												
			}//end if(isValidUser)
			else
			{				
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Invalid User Role");											
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error:"+e.getMessage());	
		}
	  finally
		{//need to close connection and all prepared statements
			if(countPS!=null)
			{
				try{
					countPS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(bldgPS!=null)
			{
				try{
					bldgPS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(validatePS!=null)
			{
				try{
					validatePS.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(conn!=null)
			{
				try{
					conn.close();
				}
				catch(Exception e1){
					e1.printStackTrace();
				}
			}
		}

		return retJson;
	}
	
}

