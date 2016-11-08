package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;

public class TaskManager {

	//need a method to return json structure for tasks header with count of metric collection and action plan tasks
	
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
				+ " RZ_BAP_METRICS.rz_bapm_id"
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

}

