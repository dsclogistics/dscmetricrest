package com.dsc.mtrc.internal;

import javax.ws.rs.core.Response;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;

public class ActionPlanManager {
	
	public Response getAPforBampId(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		
		JSONArray details = new JSONArray();
		Connection conn = null;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String SQL = null;
		int bapmId = 0;
		String prodName = null;
		
		try {
			conn = ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		if(!inputJsonObj.has("productname"))
		{	
			try
			{
			  conn.close();		
			} 
			catch (SQLException e) 
			{			
			  e.printStackTrace();
		    }		
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
		if(!inputJsonObj.has("rz_bapm_id"))
		{	
			try
			{
			  conn.close();		
			} 
			catch (SQLException e) 
			{			
			  e.printStackTrace();
		    }		
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
		//need to check if user is requesting a specific or max version of ap detail
		if(inputJsonObj.has("rz_apd_ap_ver")&& (inputJsonObj.getString("rz_apd_ap_ver")!=null && inputJsonObj.getString("rz_apd_ap_ver").trim()!=""))
		{
			if(inputJsonObj.getString("rz_apd_ap_ver").trim().toLowerCase().equals("max"))
			{
				SQL = "";
			}
			else
			{
				SQL = "";
			}
		}
		else
		{
			SQL ="select  m.rz_bapm_id,m.rz_bap_id,m.mtrc_period_val_id,m.rz_bapm_status,m.rz_bapm_status_updt_dtm,m.rz_bapm_created_on_dtm,m.rz_bapm_ntfy_dtm,m.rz_bapm_approved_on_dtm, "        
					+" d.rz_apd_id,d.rz_apd_subm_app_user_id,d.rz_apd_revw_app_user_id,d.rz_apd_ap_ver,d.rz_apd_ap_created_on_dtm,d.rz_apd_ap_last_saved_on_dtm,d.rz_apd_ap_submitted_on_dtm,"		
					+" d.rz_apd_ap_status,d.rz_apd_ap_stat_upd_on_dtm,d.rz_apd_ap_text,d.rz_apd_ap_review_text,su.app_user_sso_id as submittedby,ru.app_user_sso_id as reviewedby,"
					+" bldg.dsc_mtrc_lc_bldg_name, mprod.mtrc_prod_display_text, month(tp.tm_per_start_dtm) as month, year(tp.tm_per_start_dtm) as year"
                    +" from rz_bap_metrics m"
                    +" inner join MTRC_METRIC_PERIOD_VALUE mpv"
                    +" on m.mtrc_period_val_id = mpv.mtrc_period_val_id"
                    +" inner join DSC_MTRC_LC_BLDG bldg"
                    +" on mpv.dsc_mtrc_lc_bldg_id = bldg.dsc_mtrc_lc_bldg_id"
                    +" inner join MTRC_METRIC_PERIOD mp"
                    +" on mpv.mtrc_period_id = mp.mtrc_period_id"
                    +" inner join MTRC_METRIC_PRODUCTS mprod"
                    +" on mp.mtrc_period_id = mprod.mtrc_period_id"
                    +" inner join MTRC_PRODUCT prod"
                    +" on mprod.prod_id = prod.prod_id"
                    +" and prod.prod_name = ? "
                    +" inner join MTRC_TM_PERIODS tp"
                    +" on mpv.tm_period_id = tp.tm_period_id"
                    +" left outer join rz_action_plan_dtl d"
                    +" on m.rz_bapm_id = d.rz_bapm_id"
                    +" left outer join dsc_app_user su"
				    +" on d.rz_apd_subm_app_user_id = su.app_user_id"
					+" left outer join dsc_app_user ru"
					+" on d.rz_apd_revw_app_user_id = ru.app_user_id"
                    +" where m.rz_bapm_id = ?";
		}
		
		try
		{
			
			prepStmt = conn.prepareStatement(SQL);			
			prepStmt.setString(1, prodName);
			prepStmt.setInt(2, bapmId);
			rs = prepStmt.executeQuery();
			int num = 0;
			while(rs.next())
			{
				if(num == 0)
				{//json header
				   retJson.put("result", "Success");
				   retJson.put("dsc_mtrc_lc_bldg_name", rs.getString("dsc_mtrc_lc_bldg_name"));
				   retJson.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				   retJson.put("month", rs.getInt("month"));
				   retJson.put("year", rs.getInt("year"));
				   retJson.put("rz_bapm_id", rs.getInt("rz_bapm_id"));
				   retJson.put("rz_bap_id", rs.getInt("rz_bap_id"));
				   retJson.put("mtrc_period_val_id", rs.getInt("mtrc_period_val_id"));
				   retJson.put("rz_bapm_status", rs.getString("rz_bapm_status"));
				   retJson.put("rz_bapm_status_updt_dtm", rs.getTimestamp("rz_bapm_status_updt_dtm"));
				   retJson.put("rz_bapm_created_on_dtm", rs.getTimestamp("rz_bapm_created_on_dtm"));
				   retJson.put("rz_bapm_ntfy_dtm", rs.getTimestamp("rz_bapm_ntfy_dtm"));
				   retJson.put("rz_bapm_approved_on_dtm", rs.getTimestamp("rz_bapm_approved_on_dtm"));			   
				}
				if(rs.getString("rz_apd_id")!=null)
				{
					JSONObject version = new JSONObject();
					version.put("rz_apd_id", rs.getInt("rz_apd_id"));
					version.put("rz_apd_subm_app_user_id", rs.getInt("rz_apd_subm_app_user_id"));
					version.put("rz_apd_revw_app_user_id", rs.getInt("rz_apd_revw_app_user_id"));
					version.put("rz_apd_ap_ver", rs.getInt("rz_apd_ap_ver"));
					version.put("rz_apd_ap_created_on_dtm", rs.getTimestamp("rz_apd_ap_created_on_dtm"));
					version.put("rz_apd_ap_last_saved_on_dtm", rs.getTimestamp("rz_apd_ap_last_saved_on_dtm"));
					version.put("rz_apd_ap_submitted_on_dtm", rs.getTimestamp("rz_apd_ap_submitted_on_dtm"));
					version.put("rz_apd_ap_status", rs.getString("rz_apd_ap_status"));
					version.put("rz_apd_ap_stat_upd_on_dtm", rs.getTimestamp("rz_apd_ap_stat_upd_on_dtm"));
					version.put("rz_apd_ap_text", rs.getString("rz_apd_ap_text"));
					version.put("rz_apd_ap_review_text", rs.getString("rz_apd_ap_review_text"));
					version.put("submittedby", rs.getString("submittedby"));
					version.put("reviewedby", rs.getString("reviewedby"));
					details.put(version);		
					
				}
						
			}//end of while
			rs.close();
			retJson.put("details", details);
			
		}//end of try
		catch (SQLException e)
		{
			e.printStackTrace();
			if(prepStmt!=null)
			{
				try
				{
					prepStmt.close();
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
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
		}
		finally
		{
			if(prepStmt!=null)
			{
				try
				{
					prepStmt.close();
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
		
		rb = Response.ok(retJson.toString()).build();			
		return rb;		
	}

	public Response submitActionPlan(JSONObject inputJsonObj) throws JSONException
	{

		Response rb = null;
		JSONObject retJson = new JSONObject();
		Connection conn = null;
		PreparedStatement updatePrepStmt = null;
		PreparedStatement insertPrepStmt = null;
		PreparedStatement validatePrepStmt = null;
		ResultSet rs = null;
		String updateSQL = null;
		String insertSQL = null;
		String validateVersionSQL = null;
		String validateStatusSQL = null;
		int bapmId;	
		int apdId = -1;
		int submitterId;
		String curStatus = null;
		int version;
		int maxCurVersion = 0;
		String apText = null;
		

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
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}		
		
		validateVersionSQL = "select COALESCE(max(rz_apd_ap_ver),0) max_ver from RZ_ACTION_PLAN_DTL where rz_bapm_id = ?";
		validateStatusSQL = "select rz_apd_ap_status from RZ_ACTION_PLAN_DTL where rz_bapm_id = ?";
		updateSQL = "update rz_bap_metrics set rz_bapm_status = ?, rz_bapm_status_updt_dtm = ? where rz_bapm_id = ?";
		insertSQL = "insert into RZ_ACTION_PLAN_DTL(rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_text,rz_apd_ap_submitted_on_dtm)"
				+ " values(?,?,?,?,?,?,?,?)";
		
		String updateAPDetailSQL = " update RZ_ACTION_PLAN_DTL set rz_apd_ap_status = ?,rz_apd_ap_text = ?,rz_apd_ap_stat_upd_on_dtm =?, rz_apd_ap_submitted_on_dtm = ? where rz_apd_id = ?";
		String updateAPDetailWOTextSQL = "update RZ_ACTION_PLAN_DTL set rz_apd_ap_status = ?,rz_apd_ap_stat_upd_on_dtm =?, rz_apd_ap_submitted_on_dtm = ? where rz_apd_id = ?"; 
		try
		{
			/***********Input Data Validation***********/
			
			if(!inputJsonObj.has("rz_bapm_id")||(inputJsonObj.get("rz_bapm_id")==null)||(inputJsonObj.get("rz_bapm_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_bapm_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				bapmId = inputJsonObj.getInt("rz_bapm_id");
			}
			try
			{
				if(inputJsonObj.has("rz_apd_id")||(inputJsonObj.get("rz_apd_id")!=null)||(!inputJsonObj.get("rz_apd_id").equals("")))
				{
					apdId = inputJsonObj.getInt("rz_apd_id");
				}
				System.out.println("apdId = "+apdId);
			}
			catch(Exception e)
			{
				
			}
			
			if(!inputJsonObj.has("rz_apd_ap_ver")||(inputJsonObj.get("rz_apd_ap_ver")==null)||(inputJsonObj.get("rz_apd_ap_ver").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_ver value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				version = inputJsonObj.getInt("rz_apd_ap_ver");
			}
			
			if(!inputJsonObj.has("rz_apd_subm_app_user_id")||(inputJsonObj.get("rz_apd_subm_app_user_id")==null)||(inputJsonObj.get("rz_apd_subm_app_user_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_subm_app_user_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				submitterId = inputJsonObj.getInt("rz_apd_subm_app_user_id");
			}
			
			
			
			if((apdId ==-1) && (!inputJsonObj.has("rz_apd_ap_text")||(inputJsonObj.get("rz_apd_ap_text")==null)||(inputJsonObj.get("rz_apd_ap_text").equals(""))))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_text value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				try{
					apText = inputJsonObj.getString("rz_apd_ap_text");
				}
				catch(Exception e3){}
				
			}
			
			/*********** End of Input Data Validation***********/		
			System.out.println("Done with validation");
			System.out.println("Id = "+bapmId+" version = "+version+" user = "+submitterId+" text = "+apText);
			
			conn.setAutoCommit(false);
			insertPrepStmt = conn.prepareStatement(insertSQL, PreparedStatement.RETURN_GENERATED_KEYS);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			validatePrepStmt = conn.prepareStatement(validateVersionSQL);
			validatePrepStmt.setInt(1, bapmId);
			rs = validatePrepStmt.executeQuery();			
			while(rs.next())
			{
				maxCurVersion =rs.getInt("max_ver");
			}
			if(apdId ==-1)// no action plan detail id provided by the input json
			{
				if(maxCurVersion > 0)//this means there's another detail record for this action plan
				{
					//need to check to make sure previous detailed record is in rejected status
					validatePrepStmt = conn.prepareStatement(validateStatusSQL);
					validatePrepStmt.setInt(1, bapmId);
					rs = validatePrepStmt.executeQuery();
					while(rs.next())
					{
						curStatus = rs.getString("rz_apd_ap_status");					
					}
					if(!curStatus.equals("Rejected"))//if it's not in rejected status, we need to return an error
					{
						retJson.put("result", "FAILED");
						retJson.put("resultCode", "200");
						retJson.put("message", "Error: Cannot Submit a new action plan. Current action plan exists and has not been rejected yet ");
						rb = Response.ok(retJson.toString()).build();
					}
					else if(maxCurVersion >= version)//need to make user is passing an updated version 
					{
						retJson.put("result", "FAILED");
						retJson.put("resultCode", "200");
						retJson.put("message", "Error: Version must be greater than current max version ");
						rb = Response.ok(retJson.toString()).build();
					}
					else//at this point we're done with all the validation and can perform an update and insert
					{
						Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
						int newApDetailId = 0;
						updatePrepStmt.setString(1, "Ready For Review");
						updatePrepStmt.setTimestamp(2, addDte);
						updatePrepStmt.setInt(3, bapmId);
						updatePrepStmt.executeUpdate();
						//rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_text
						
						insertPrepStmt.setInt(1, bapmId);
						insertPrepStmt.setInt(2, submitterId);
						insertPrepStmt.setInt(3,version);
						insertPrepStmt.setTimestamp(4, addDte);
						insertPrepStmt.setString(5,"Ready For Review");
						insertPrepStmt.setTimestamp(6, addDte);
						insertPrepStmt.setString(7, apText);
						insertPrepStmt.setTimestamp(8, addDte);
						insertPrepStmt.executeUpdate();
						rs = insertPrepStmt.getGeneratedKeys();
						while(rs.next())
						{
							newApDetailId = rs.getInt(1);
						}
						conn.commit();
						rs.close();
						retJson.put("result", "Success");
					    retJson.put("resultCode", "100");
					    retJson.put("message", "Changes have been saved");
					    retJson.put("rz_apd_id", newApDetailId);
					    retJson.put("rz_apd_ap_status", "Ready For Review");
					    rb = Response.ok(retJson.toString()).build();											
					}
					 					
				}// end of if(maxCurVersion > 0)
				else//if we're here, that means it's the first time we're adding a detail record to the ap details table
				{
					Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
					int newApDetailId = 0;
					ResultSet res = null;
					updatePrepStmt.setString(1, "Ready For Review");
					updatePrepStmt.setTimestamp(2, addDte);
					updatePrepStmt.setInt(3, bapmId);
					updatePrepStmt.executeUpdate();
					//rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_text
					
					insertPrepStmt.setInt(1, bapmId);
					insertPrepStmt.setInt(2, submitterId);
					insertPrepStmt.setInt(3,version);
					insertPrepStmt.setTimestamp(4, addDte);
					insertPrepStmt.setString(5,"Ready For Review");
					insertPrepStmt.setTimestamp(6, addDte);
					insertPrepStmt.setString(7, apText);
					insertPrepStmt.setTimestamp(8, addDte);
					insertPrepStmt.executeUpdate();
					res = insertPrepStmt.getGeneratedKeys();
					while(res.next())
					{
						newApDetailId = res.getInt(1);
					}
					conn.commit();
					res.close();
					retJson.put("result", "Success");
				    retJson.put("resultCode", "100");
				    retJson.put("message", "Changes have been saved");
				    retJson.put("rz_apd_id", newApDetailId);
				    retJson.put("rz_apd_ap_status", "Ready For Review");
				    rb = Response.ok(retJson.toString()).build();											
				}
			}// end of if(apdId ==-1)	
			
			if(apdId > 0)//this means we're submitting existing record
			{
				validatePrepStmt = conn.prepareStatement(validateStatusSQL);
				validatePrepStmt.setInt(1, bapmId);
				rs = validatePrepStmt.executeQuery();
				while(rs.next())
				{
					curStatus = rs.getString("rz_apd_ap_status");					
				}
				if(maxCurVersion != version)// if they submitting existing record we need to check if it's the most current version of the AP
				{
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Error: Passed version ("+version+") doesn't match the current max version ("+maxCurVersion+")");
					rb = Response.ok(retJson.toString()).build();
				}
				else if(!curStatus.equals("WIP"))// since we're trying to submit existing ap, we need to make sure it's in WIP status.
				{
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Error: Incorrect Action Plan status");
					rb = Response.ok(retJson.toString()).build();
				}
				else
				{
					Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
					
					int newApDetailId = apdId;
					updatePrepStmt.setString(1, "Ready For Review");
					updatePrepStmt.setTimestamp(2, addDte);
					updatePrepStmt.setInt(3, bapmId);
					updatePrepStmt.executeUpdate();
					if(apText==null)
					{
						updatePrepStmt = conn.prepareStatement(updateAPDetailWOTextSQL);
						//update RZ_ACTION_PLAN_DTL set rz_apd_ap_status = ?
						//,rz_apd_ap_stat_upd_on_dtm = ?,rz_apd_ap_stat_upd_on_dtm =?, rz_apd_ap_submitted_on_dtm = ? where rz_apd_id = ?
						updatePrepStmt.setString(1, "Ready For Review");
						updatePrepStmt.setTimestamp(2, addDte);					
						updatePrepStmt.setTimestamp(3, addDte);
						updatePrepStmt.setInt(4, apdId);
						updatePrepStmt.executeUpdate();
						retJson.put("result", "Success");
					    retJson.put("resultCode", "100");
					    retJson.put("message", "Changes have been saved");
					    retJson.put("rz_apd_id", newApDetailId);
					    retJson.put("rz_apd_ap_status", "Ready For Review");
					    rb = Response.ok(retJson.toString()).build();			
						conn.commit();
					}
					else
					{
						updatePrepStmt = conn.prepareStatement(updateAPDetailSQL); 
						//update RZ_ACTION_PLAN_DTL set 
						//rz_apd_ap_status = ?,
						//rz_apd_ap_text = ?,
						//rz_apd_ap_stat_upd_on_dtm =?,
						//rz_apd_ap_submitted_on_dtm = ?
						//where rz_apd_id = ?"
						updatePrepStmt.setString(1, "Ready For Review");						
						updatePrepStmt.setString(2, apText);
						updatePrepStmt.setTimestamp(3, addDte);
						updatePrepStmt.setTimestamp(4, addDte);
						updatePrepStmt.setInt(5, apdId);
						updatePrepStmt.executeUpdate();
						retJson.put("result", "Success");
					    retJson.put("resultCode", "100");
					    retJson.put("message", "Changes have been saved");
					    retJson.put("rz_apd_id", newApDetailId);
					    retJson.put("rz_apd_ap_status", "Ready For Review");
					    rb = Response.ok(retJson.toString()).build();			
						conn.commit();
					}
					
				}


			}
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+ e.getMessage());
			rb = Response.ok(retJson.toString()).build();	
			if(conn != null)
			{
				try
				{
					conn.rollback();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
				
		}
		finally
		{
			if(validatePrepStmt != null)
			{
				try
				{
					validatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(updatePrepStmt != null)
			{
				try
				{
					updatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(insertPrepStmt != null)
			{
				try
				{
					insertPrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
			if(conn != null)
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
		}
		
		return rb;
	}

	public Response saveActionPlan(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		try
		{
			if(inputJsonObj.has("rz_apd_id")&&(inputJsonObj.get("rz_apd_id")!=null)&&(!inputJsonObj.get("rz_apd_id").equals("")))
			{				
				rb = updateActionPlan(inputJsonObj);
			}
			else
			{
				rb = addActionPlan(inputJsonObj);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			return rb;
			
		}
		
		return rb;
	}
	
	public Response addActionPlan(JSONObject inputJsonObj)throws JSONException
	{
		Response rb = null;
		ResultSet rs = null;
		JSONObject retJson = new JSONObject();
		Connection conn = null;
		PreparedStatement updatePrepStmt = null;
		PreparedStatement insertPrepStmt = null;
		PreparedStatement validatePrepStmt = null;
		int bapmId = -1;
		int version = -1;
		int submitterId = -1;
		String apText = null;

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
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}	
		
		String validateVersionSQL = "select COALESCE(max(rz_apd_ap_ver),0) max_ver from RZ_ACTION_PLAN_DTL where rz_bapm_id = ?";
		String validateStatusSQL = "select rz_apd_ap_status from RZ_ACTION_PLAN_DTL where rz_bapm_id = ?";
		String updateSQL = "update rz_bap_metrics set rz_bapm_status = ?, rz_bapm_status_updt_dtm = ? where rz_bapm_id = ?";
		String insertSQL = "insert into RZ_ACTION_PLAN_DTL(rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_last_saved_on_dtm,rz_apd_ap_text)"
				+ " values(?,?,?,?,?,?,?,?)";
		
		
		try
		{
			/***********Input Data Validation***********/
			
			if(!inputJsonObj.has("rz_bapm_id")||(inputJsonObj.get("rz_bapm_id")==null)||(inputJsonObj.get("rz_bapm_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_bapm_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				bapmId = inputJsonObj.getInt("rz_bapm_id");
			}			
			
			if(!inputJsonObj.has("rz_apd_ap_ver")||(inputJsonObj.get("rz_apd_ap_ver")==null)||(inputJsonObj.get("rz_apd_ap_ver").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_ver value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				version = inputJsonObj.getInt("rz_apd_ap_ver");
			}
			
			if(!inputJsonObj.has("rz_apd_subm_app_user_id")||(inputJsonObj.get("rz_apd_subm_app_user_id")==null)||(inputJsonObj.get("rz_apd_subm_app_user_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_subm_app_user_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				submitterId = inputJsonObj.getInt("rz_apd_subm_app_user_id");
			}
			if((!inputJsonObj.has("rz_apd_ap_text")||(inputJsonObj.get("rz_apd_ap_text")==null)||(inputJsonObj.get("rz_apd_ap_text").equals(""))))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_text value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{									
				apText = inputJsonObj.getString("rz_apd_ap_text");
			
			}
									
			
			/*********** End of Input Data Validation***********/
			
		}
		catch(Exception e)
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			return rb;	
		}
		
		try
		{
			conn.setAutoCommit(false);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			insertPrepStmt = conn.prepareStatement(insertSQL,PreparedStatement.RETURN_GENERATED_KEYS);
			validatePrepStmt = conn.prepareStatement(validateVersionSQL);
			validatePrepStmt.setInt(1, bapmId);
			rs = validatePrepStmt.executeQuery();			
			int maxCurVersion=-1;
			while(rs.next())
			{
				maxCurVersion =rs.getInt("max_ver");
			}
			if(maxCurVersion > 0)//this means there's another detail record for this action plan
			{
				//need to check to make sure previous detailed record is in rejected status
				String curStatus = null;
				validatePrepStmt = conn.prepareStatement(validateStatusSQL);
				validatePrepStmt.setInt(1, bapmId);
				rs = validatePrepStmt.executeQuery();
				while(rs.next())
				{
					curStatus = rs.getString("rz_apd_ap_status");					
				}
				if(!curStatus.equals("Rejected"))//if it's not in rejected status, we need to return an error
				{
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Error: Cannot Submit a new action plan. Current action plan exists and has not been rejected yet ");
					rb = Response.ok(retJson.toString()).build();
				}
				else if(maxCurVersion >= version)//need to make user is passing an updated version 
				{
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Error: Version must be greater than current max version ");
					rb = Response.ok(retJson.toString()).build();
				}
				else//at this point we're done with all the validation and can perform an update and insert
				{
					Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
					int newApDetailId = 0;
					updatePrepStmt.setString(1, "WIP");
					updatePrepStmt.setTimestamp(2, addDte);
					updatePrepStmt.setInt(3, bapmId);
					updatePrepStmt.executeUpdate();
					
					//rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_last_saved_on_dtm,rz_apd_ap_text
					insertPrepStmt.setInt(1, bapmId);
					insertPrepStmt.setInt(2, submitterId);
					insertPrepStmt.setInt(3,version);
					insertPrepStmt.setTimestamp(4, addDte);
					insertPrepStmt.setString(5,"WIP");
					insertPrepStmt.setTimestamp(6, addDte);					
					insertPrepStmt.setTimestamp(7, addDte);
					insertPrepStmt.setString(8, apText);
					insertPrepStmt.executeUpdate();
					rs = insertPrepStmt.getGeneratedKeys();
					while(rs.next())
					{
						newApDetailId = rs.getInt(1);
					}
					conn.commit();
					rs.close();
					retJson.put("result", "Success");
				    retJson.put("resultCode", "100");
				    retJson.put("message", "Changes have been saved");
				    retJson.put("rz_apd_id", newApDetailId);
				    retJson.put("rz_apd_ap_status", "WIP");
				    rb = Response.ok(retJson.toString()).build();											
				}
				 					
			}// end of if(maxCurVersion > 0)
			else// this is the first time we're adding the data for this action plan
			{
				Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
				int newApDetailId = 0;
				updatePrepStmt.setString(1, "WIP");
				updatePrepStmt.setTimestamp(2, addDte);
				updatePrepStmt.setInt(3, bapmId);
				updatePrepStmt.executeUpdate();
				
				//rz_bapm_id,rz_apd_subm_app_user_id,rz_apd_ap_ver,rz_apd_ap_created_on_dtm,rz_apd_ap_status,rz_apd_ap_stat_upd_on_dtm,rz_apd_ap_last_saved_on_dtm,rz_apd_ap_text
				insertPrepStmt.setInt(1, bapmId);
				insertPrepStmt.setInt(2, submitterId);
				insertPrepStmt.setInt(3,version);
				insertPrepStmt.setTimestamp(4, addDte);
				insertPrepStmt.setString(5,"WIP");
				insertPrepStmt.setTimestamp(6, addDte);					
				insertPrepStmt.setTimestamp(7, addDte);
				insertPrepStmt.setString(8, apText);
				insertPrepStmt.executeUpdate();
				ResultSet res = insertPrepStmt.getGeneratedKeys();
				while(res.next())
				{
					newApDetailId = res.getInt(1);
				}
				conn.commit();
				res.close();
				retJson.put("result", "Success");
			    retJson.put("resultCode", "100");
			    retJson.put("message", "Changes have been saved");
			    retJson.put("rz_apd_id", newApDetailId);
			    retJson.put("rz_apd_ap_status", "WIP");
			    rb = Response.ok(retJson.toString()).build();											
			}
			
			
			
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+ e.getMessage());
			rb = Response.ok(retJson.toString()).build();	
			if(conn != null)
			{
				try
				{
					conn.rollback();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
		}//end of catch
		finally
		{
			if(validatePrepStmt != null)
			{
				try
				{
					validatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(updatePrepStmt != null)
			{
				try
				{
					updatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(insertPrepStmt != null)
			{
				try
				{
					insertPrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
			if(conn != null)
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
		}
		
		return rb;
	}
	
	public Response updateActionPlan(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		ResultSet rs = null;
		Connection conn = null;
		JSONObject retJson = new JSONObject();
		int apdId = -1;
		int submitterId = -1;
		int bapmId = -1;
		String apText = null;
		String curStatus = null;
		
			
		try
		{
			/***********Input Data Validation***********/
			
			if(!inputJsonObj.has("rz_bapm_id")||(inputJsonObj.get("rz_bapm_id")==null)||(inputJsonObj.get("rz_bapm_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_bapm_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				bapmId = inputJsonObj.getInt("rz_apd_id");
			}	
			if(!inputJsonObj.has("rz_apd_id")||(inputJsonObj.get("rz_apd_id")==null)||(inputJsonObj.get("rz_apd_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				apdId = inputJsonObj.getInt("rz_apd_id");
			}	
			
			if(!inputJsonObj.has("rz_apd_subm_app_user_id")||(inputJsonObj.get("rz_apd_subm_app_user_id")==null)||(inputJsonObj.get("rz_apd_subm_app_user_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_subm_app_user_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				submitterId = inputJsonObj.getInt("rz_apd_subm_app_user_id");
			}
			if((!inputJsonObj.has("rz_apd_ap_text")||(inputJsonObj.get("rz_apd_ap_text")==null)||(inputJsonObj.get("rz_apd_ap_text").equals(""))))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_text value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{									
				apText = inputJsonObj.getString("rz_apd_ap_text");
			
			}
									
			
			/*********** End of Input Data Validation***********/
			
		}
		catch(Exception e)
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			return rb;	
		}
		
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
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}	
		
		String validateSQL = "select rz_apd_ap_ver, rz_apd_ap_status  from RZ_ACTION_PLAN_DTL where rz_apd_id = ?";
		String updateSQL = "update RZ_ACTION_PLAN_DTL set rz_apd_ap_text = ?,rz_apd_ap_last_saved_on_dtm = ?  where rz_apd_id = ? ";
		PreparedStatement validatePrepStmt = null;
		PreparedStatement updatePrepStmt = null;
		try
		{
			conn.setAutoCommit(false);
			validatePrepStmt = conn.prepareStatement(validateSQL);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			
			validatePrepStmt.setInt(1, apdId);
			rs = validatePrepStmt.executeQuery();
			while(rs.next())
			{
				curStatus = rs.getString("rz_apd_ap_status");
			}
			System.out.println(curStatus);
			rs.close();
			if(!curStatus.equals("WIP"))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: Current status doesn't allow any changes");
				rb = Response.ok(retJson.toString()).build();
			}
			else
			{
				updatePrepStmt.setString(1,apText);
				updatePrepStmt.setTimestamp(2,java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
				updatePrepStmt.setInt(3,apdId);
				updatePrepStmt.executeUpdate();
				retJson.put("result", "Success");
			    retJson.put("resultCode", "100");
			    retJson.put("message", "Changes have been saved");
			    retJson.put("rz_apd_id", apdId);
			    retJson.put("rz_apd_ap_status", "WIP");
			    rb = Response.ok(retJson.toString()).build();
				conn.commit();
			}			
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+ e.getMessage());
			rb = Response.ok(retJson.toString()).build();	
			if(conn != null)
			{
				try
				{
					conn.rollback();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
		}//end of catch
		finally
		{
			if(validatePrepStmt != null)
			{
				try
				{
					validatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(updatePrepStmt != null)
			{
				try
				{
					updatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
			
			if(conn != null)
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
		}
		
		
		
		return rb;
	}

	public Response submitAPReview(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();		
		Connection conn = null;
		PreparedStatement validatePrepStmt = null;
		PreparedStatement updatePrepStmt = null;				
		ResultSet rs = null;
		int reviewerId = -1;
		int apdId = -1;		
		int bapmId = -1;
		String status = null;
		String curStatus = null;
		String reviewText = null;
		
		try
		{
			/***********Input Data Validation***********/
			
			if((!inputJsonObj.has("rz_apd_ap_status")||(inputJsonObj.get("rz_apd_ap_status")==null)||(inputJsonObj.get("rz_apd_ap_status").equals(""))))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_status value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{													
				status = inputJsonObj.getString("rz_apd_ap_status");
				System.out.println("status= "+status);
				if(!status.equals("Rejected")&&!status.equals("Approved"))
				{
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Error: Invalid Status");
					rb = Response.ok(retJson.toString()).build();			
					return rb;
				}
			
			}
			if(!inputJsonObj.has("rz_apd_id")||(inputJsonObj.get("rz_apd_id")==null)||(inputJsonObj.get("rz_apd_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				apdId = inputJsonObj.getInt("rz_apd_id");
			}	
			
			if(!inputJsonObj.has("rz_apd_revw_app_user_id")||(inputJsonObj.get("rz_apd_revw_app_user_id")==null)||(inputJsonObj.get("rz_apd_revw_app_user_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_revw_app_user_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				reviewerId = inputJsonObj.getInt("rz_apd_revw_app_user_id");
			}
			if(status.equals("Rejected")&&((!inputJsonObj.has("rz_apd_ap_review_text")||(inputJsonObj.get("rz_apd_ap_review_text")==null)||(inputJsonObj.get("rz_apd_ap_review_text").equals("")))))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: rz_apd_ap_review_text value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{									
				try
				{
					reviewText = inputJsonObj.getString("rz_apd_ap_review_text");
				}
				catch(Exception e){}
										
			}
									
			
			/*********** End of Input Data Validation***********/
			
		}
		catch(Exception e)
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			return rb;	
		}
		
		
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
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		String validateSQL = "select rz_apd_ap_status, rz_bapm_id from RZ_ACTION_PLAN_DTL where rz_apd_id = ?";
		String updateHeaderSQL = "update RZ_BAP_METRICS set rz_bapm_status = ?, rz_bapm_approved_on_dtm = ?, rz_bapm_status_updt_dtm = ? where rz_bapm_id = ?";
		String updateDetailSQL = "update RZ_ACTION_PLAN_DTL set rz_apd_revw_app_user_id = ?,rz_apd_ap_status = ?,rz_apd_ap_stat_upd_on_dtm = ?,rz_apd_ap_review_text =? where rz_apd_id = ?";
		
		try
		{
			conn.setAutoCommit(false);
			validatePrepStmt = conn.prepareStatement(validateSQL);
			validatePrepStmt.setInt(1, apdId);
			rs = validatePrepStmt.executeQuery();
			while(rs.next())
			{
				bapmId = rs.getInt("rz_bapm_id");
				curStatus = rs.getString("rz_apd_ap_status");				
			}
			rs.close();
			if(!curStatus.equals("Ready For Review"))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error:Action Plan has not been submitted yet");
				rb = Response.ok(retJson.toString()).build();		
			}
			else
			{    
				Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
				//update RZ_BAP_METRICS set rz_apd_ap_status = ?, rz_bapm_approved_on_dtm = ?, rz_bapm_status_updt_dtm where rz_bapm_id
				updatePrepStmt = conn.prepareStatement(updateHeaderSQL);
				updatePrepStmt.setString(1,status);
				updatePrepStmt.setTimestamp(2, addDte);
				updatePrepStmt.setTimestamp(3, addDte);
				updatePrepStmt.setInt(4, bapmId);
				updatePrepStmt.executeUpdate();
				
				//rz_apd_revw_app_user_id = ?,rz_apd_ap_status = ?,rz_apd_ap_stat_upd_on_dtm = ?,rz_apd_ap_review_text =? where rz_apd_id = ?
				updatePrepStmt = conn.prepareStatement(updateDetailSQL);
				updatePrepStmt.setInt(1, reviewerId);
				updatePrepStmt.setString(2, status);
				updatePrepStmt.setTimestamp(3, addDte);
				updatePrepStmt.setString(4, reviewText);
				updatePrepStmt.setInt(5, apdId);
				updatePrepStmt.executeUpdate();
				
				retJson.put("result", "Success");
			    retJson.put("resultCode", "100");
			    retJson.put("message", "Changes have been saved");
			    retJson.put("rz_apd_id", apdId);
			    retJson.put("rz_apd_ap_status", status);
			    rb = Response.ok(retJson.toString()).build();
				conn.commit();
				
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+ e.getMessage());
			rb = Response.ok(retJson.toString()).build();	
			if(conn != null)
			{
				try
				{
					conn.rollback();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
		}//end of catch
		{
			if(validatePrepStmt != null)
			{
				try
				{
					validatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			if(updatePrepStmt != null)
			{
				try
				{
					updatePrepStmt.close();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}
			}
			
			
			if(conn != null)
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
		}
		return rb;
	}

	public Response getPriorAP(JSONObject inputJsonObj)throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		JSONArray actionPlans = new JSONArray();
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement reasonsPrepStmt = null;
		//PreparedStatement updatePrepStmt = null;				
		ResultSet rs = null;
		int begMonth = 0;
		int begYear = 0;
		int endMonth = 0;
		int endYear = 0;
		int buildingId = 0;
		int metricPeriodId = 0;
		String productName = null;
		DecimalFormat df2 = new DecimalFormat("0.00");
        df2.setRoundingMode(RoundingMode.UP);
		
		/*******************input json validation********************/
		try
		{
			if(!inputJsonObj.has("productname")||(inputJsonObj.get("productname")==null)||(inputJsonObj.get("productname").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: productname value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				productName = inputJsonObj.getString("productname");
			}
			
			if(!inputJsonObj.has("mtrc_period_id")||(inputJsonObj.get("mtrc_period_id")==null)||(inputJsonObj.get("mtrc_period_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: mtrc_period_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				metricPeriodId = inputJsonObj.getInt("mtrc_period_id");
			}
			if(!inputJsonObj.has("dsc_mtrc_lc_bldg_id")||(inputJsonObj.get("dsc_mtrc_lc_bldg_id")==null)||(inputJsonObj.get("dsc_mtrc_lc_bldg_id").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: dsc_mtrc_lc_bldg_id value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				buildingId = inputJsonObj.getInt("dsc_mtrc_lc_bldg_id");
			}
			
			if(!inputJsonObj.has("begmonth")||(inputJsonObj.get("begmonth")==null)||(inputJsonObj.get("begmonth").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: begmonth value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				begMonth = inputJsonObj.getInt("begmonth");
			}
			if(!inputJsonObj.has("begyear")||(inputJsonObj.get("begyear")==null)||(inputJsonObj.get("begyear").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: begyear value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				begYear = inputJsonObj.getInt("begyear");
			}
			if(!inputJsonObj.has("endmonth")||(inputJsonObj.get("endmonth")==null)||(inputJsonObj.get("endmonth").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: endmonth value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				endMonth = inputJsonObj.getInt("endmonth");
			}
			if(!inputJsonObj.has("endyear")||(inputJsonObj.get("endyear")==null)||(inputJsonObj.get("endyear").equals("")))
			{
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Error: endyear value is required ");
				rb = Response.ok(retJson.toString()).build();			
				return rb;		
			}
			else
			{
				endYear = inputJsonObj.getInt("endyear");
			}	
			
		}//end of try
		catch(Exception e)
		{
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();			
			return rb;	
			
		}//end of catch
		
		/*****************end of json input validation******************/
		
		/***************** get db connection ******************/
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
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		/***************** end of get db connection ******************/
		
		
		String SQL = "select month(MTRC_TM_PERIODS.tm_per_start_dtm) as ap_month,"
        +" year(MTRC_TM_PERIODS.tm_per_start_dtm) as ap_year,"
		+" MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id as building_id,"
		+" MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id as value_id,"
		+" MTRC_METRIC_PERIOD_VALUE.mtrc_period_id as metric_period_id,"
		+" MTRC_METRIC_PERIOD_VALUE.tm_period_id as tm_period_id,"
		+" MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_value as value,"
	    +" MTRC_DATA_TYPE.data_type_token as data_type,"
	    + "(coalesce(MTRC_MPBG.mpbg_display_text, MTRC_MPG.mpg_display_text)) as goal_txt, "
	    +" (coalesce(MTRC_METRIC_PERIOD.mtrc_period_max_dec_places, MTRC_METRIC.mtrc_max_dec_places)) as max_decimal,"
	    +" RZ_ACTION_PLAN_DTL.rz_bapm_id as bapm_id,"
	    +" RZ_ACTION_PLAN_DTL.rz_apd_id as apd_id,"
	    +" RZ_ACTION_PLAN_DTL.rz_apd_ap_status as ap_status,"
	    +" RZ_ACTION_PLAN_DTL.rz_apd_ap_text as ap_submit_text,"
	    +" RZ_ACTION_PLAN_DTL.rz_apd_ap_review_text as ap_review_text,"
	    +" user1.app_user_sso_id submitted_by,"
	    +" user2.app_user_sso_id approved_by,"
	    +" RZ_ACTION_PLAN_DTL.rz_apd_ap_submitted_on_dtm as submitted_on,"
	    +" RZ_BAP_METRICS.rz_bapm_approved_on_dtm as approved_on"	   	
        +" from MTRC_METRIC_PERIOD_VALUE "
        +" join MTRC_TM_PERIODS"
        +" on MTRC_METRIC_PERIOD_VALUE.tm_period_id = MTRC_TM_PERIODS.tm_period_id"
        +" join RZ_BAP_METRICS "
        +" on  MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id = RZ_BAP_METRICS.mtrc_period_val_id"
        +" join RZ_ACTION_PLAN_DTL on RZ_BAP_METRICS.rz_bapm_id = RZ_ACTION_PLAN_DTL.rz_bapm_id"
        +" join DSC_APP_USER user1 on RZ_ACTION_PLAN_DTL.rz_apd_subm_app_user_id = user1.app_user_id"
        +" join DSC_APP_USER user2 on RZ_ACTION_PLAN_DTL.rz_apd_revw_app_user_id = user2.app_user_id"
        +" join MTRC_METRIC_PERIOD on MTRC_METRIC_PERIOD_VALUE.mtrc_period_id = MTRC_METRIC_PERIOD.mtrc_period_id"
        +" join MTRC_METRIC on MTRC_METRIC_PERIOD.mtrc_id = MTRC_METRIC.mtrc_id "
        +" join MTRC_DATA_TYPE on MTRC_METRIC.data_type_id = MTRC_DATA_TYPE.data_type_id"
        +" join MTRC_MPG on MTRC_METRIC_PERIOD.mtrc_period_id = MTRC_MPG.mtrc_period_id"
        +" join MTRC_PRODUCT on MTRC_MPG.prod_id = MTRC_PRODUCT.prod_id"
        +" left outer join MTRC_MPBG on MTRC_MPG.prod_id = MTRC_MPBG.prod_id" 
        +" and MTRC_MPG.mtrc_period_id = MTRC_MPBG.mtrc_period_id"
        +" and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = MTRC_MPBG.dsc_mtrc_lc_bldg_id "
        +" and MTRC_MPBG.mpbg_start_eff_dtm <= getdate()" 
        +" and MTRC_MPBG.mpbg_end_eff_dtm >= getdate()"
        +" where "
        +" MTRC_METRIC_PERIOD_VALUE.mtrc_period_id = ?"
        +" and MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id = ?"
        +" and MTRC_TM_PERIODS.tm_per_start_dtm between ? and ?" 
       // +" and (month(MTRC_TM_PERIODS.tm_per_end_dtm)<= ? and year(MTRC_TM_PERIODS.tm_per_end_dtm)<= ?)"
        +" and RZ_BAP_METRICS.rz_bapm_status = 'Approved'"
        +" and RZ_ACTION_PLAN_DTL.rz_apd_ap_status ='Approved'"
        +" and MTRC_PRODUCT.prod_name = ?"
        +" and MTRC_MPG.mpg_start_eff_dtm <=getdate()"
        +" and MTRC_MPG.mpg_end_eff_dtm >=getdate()";
		
		String reasonSQL = "select ar.mpvr_id,"
				           + " ar.mpr_id,"
				           + " r.mpr_display_text,"
				           + " ar.mpvr_comment,"
				           + " r.mpr_desc "
				           + " from MTRC_MPV_REASONS ar" + " join MTRC_MP_REASON r" + " on ar.mpr_id = r.mpr_id"
				           + " where ar.mtrc_period_val_id= ?";
		try
		{			
			ps = conn.prepareStatement(SQL);
			ps.setInt(1,metricPeriodId);
			ps.setInt(2,buildingId);
			ps.setString(3, begMonth+"/01/"+begYear);
			//ps.setInt(4, begYear);
			ps.setString(4, endMonth+"/01/"+endYear);
			//ps.setInt(6, endYear);
			ps.setString(5,productName);
			
			rs = ps.executeQuery();
			 
			while(rs.next())
			{
				JSONObject temp = new JSONObject();
				temp.put("month", rs.getInt("ap_month"));
				temp.put("year", rs.getInt("ap_year"));
				temp.put("dsc_mtrc_lc_bldg_id",rs.getInt("building_id"));
				temp.put("mtrc_period_val_id", rs.getInt("value_id"));
				temp.put("mtrc_period_id", rs.getInt("metric_period_id"));
				String value = rs.getString("value");
				temp.put("data_type_token", rs.getString("data_type"));
				if(rs.getString("data_type").equals("pct"))
				{
					double valueNum = Double.parseDouble(value)*100;
					value = df2.format(valueNum);
					temp.put("mtrc_period_val_value", value);
				}
				else
				{
					temp.put("mtrc_period_val_value", value);
				}
				temp.put("goal_txt", rs.getString("goal_txt"));
				temp.put("rz_bapm_id", rs.getInt("bapm_id"));
				temp.put("rz_apd_id", rs.getInt("apd_id"));
				temp.put("rz_apd_ap_status", rs.getString("ap_status"));
				temp.put("rz_apd_ap_text", rs.getString("ap_submit_text"));
				temp.put("rz_apd_ap_review_text", rs.getString("ap_review_text"));
				temp.put("rz_apd_ap_review_text", rs.getString("ap_review_text")==null?"":rs.getString("ap_review_text"));
				temp.put("submitted_by", rs.getString("submitted_by"));
				temp.put("approved_by", rs.getString("approved_by"));
				temp.put("rz_apd_ap_submitted_on_dtm", rs.getTimestamp("submitted_on"));
				temp.put("rz_bapm_approved_on_dtm", rs.getTimestamp("approved_on"));
				
				JSONArray reasons = new JSONArray();
				reasonsPrepStmt = conn.prepareStatement(reasonSQL);
				reasonsPrepStmt.setInt(1, rs.getInt("value_id"));
				ResultSet res = reasonsPrepStmt.executeQuery();
				ResultSetMetaData rsmd = res.getMetaData();
				int numColomns = rsmd.getColumnCount();				
				while(res.next())
				{
					JSONObject reason = new JSONObject();
					for(int i = 1;i<numColomns;i++)
					{
						String column_name = rsmd.getColumnName(i);

						reason.put(column_name, res.getString(i)==null?"":res.getString(i));
					}
					reasons.put(reason);
				}
				res.close();
				temp.put("assignedreasons", reasons);
				actionPlans.put(temp);							
			}//end of while
			retJson.put("result", "Success");
			retJson.put("actionplans", actionPlans);
			rs.close();
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			//retJson = null;//clearing return json
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "Error: "+ e.getMessage());
			//rb = Response.ok(retJson.toString()).build();	
			
		}//end of catch
		finally
		{
			
			if(ps!=null)
			{
				try
				{
					ps.close();
				}
				catch(Exception e1)
				{}
			}
			if(reasonsPrepStmt!=null)
			{
				try
				{
					reasonsPrepStmt.close();
				}
				catch(Exception e1)
				{}
			}
			if(conn!=null)
			{
				try
				{
					conn.close();
				}
				catch(Exception e1)
				{}
			}
		}//end of finally
		
		rb = Response.ok(retJson.toString()).build();	
		return rb;
	}
	

}
