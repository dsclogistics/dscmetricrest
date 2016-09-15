package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;

public class ReasonManager {

	public Response getReason(JSONObject inputJsonObj) throws JSONException {
		Response rb = null;
		JSONArray json = new JSONArray();
		JSONObject obj1 = new JSONObject();
		String metricPeriodID = "";
		if (inputJsonObj.has("mtrc_period_id")) 
		{
			metricPeriodID = inputJsonObj.get("mtrc_period_id").toString();
		} 
		else 
		{
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "mtrc_period_id parameter is required");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}

		Connection conn = null;
		try 
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "DB Connection Failed");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}

		try 
		{
			String SQL = "select distinct MTRC_MP_REASON.mpr_id,"
					+ "MTRC_MP_REASON.mtrc_period_id,"
					+ "MTRC_MP_REASON.mpr_display_text,"
					+ "MTRC_MP_REASON.mpr_display_order,"
					+ "MTRC_MP_REASON.mpr_desc,"
					+ "MTRC_MP_REASON.mpr_std_yn,"
					+ "MTRC_MP_REASON.mpr_added_on_dtm,"
					+ "MTRC_MP_REASON.mpr_added_by_uid,"
					+ "MTRC_MP_REASON.mpr_stdized_on_dtm,"
					+ "MTRC_MP_REASON.mpr_stdized_by_uid,"
					+ "COALESCE(counter.userby,0) as usedby"
					+ " from MTRC_MP_REASON"
					+ " left outer join "
					+ "(select count(MTRC_MPV_REASONS.mpvr_id) as userby,"
					+ "  MTRC_MPV_REASONS.Mpr_id "
					+ "from MTRC_MPV_REASONS group by Mpr_id) counter"
					+ " on MTRC_MP_REASON.mpr_id = counter.mpr_id"
					+ " where mtrc_period_id ="+metricPeriodID
					+ " group by counter.userby,"
					+ "MTRC_MP_REASON.mpr_id,"
					+ "MTRC_MP_REASON.mtrc_period_id,"
					+ "MTRC_MP_REASON.mpr_display_text,"
					+ "MTRC_MP_REASON.mpr_display_order,"
					+ "MTRC_MP_REASON.mpr_desc,"
					+ "MTRC_MP_REASON.mpr_std_yn,"
					+ "MTRC_MP_REASON.mpr_added_on_dtm,"
					+ "MTRC_MP_REASON.mpr_added_by_uid,"
					+ "MTRC_MP_REASON.mpr_stdized_on_dtm,"
					+ "MTRC_MP_REASON.mpr_stdized_by_uid ";

			Statement stmt = conn.createStatement();
			// System.out.println("statement connect done" );
			// do starts here
			ResultSet rs = stmt.executeQuery(SQL);
			ResultSetMetaData rsmd = rs.getMetaData();
			// System.out.println("result set created" );

			int numColumns = rsmd.getColumnCount();
			while (rs.next()) 
			{

				JSONObject obj = new JSONObject();
				for (int i = 1; i < numColumns + 1; i++)
				{
					String column_name = rsmd.getColumnName(i);		
					obj.put(column_name, rs.getString(i)==null?"":rs.getString(i));
				} // for numcolumns
				json.put(obj);
			} // while loop

			rs.close();
			stmt.close();
			if (conn != null) 
			{
				conn.close();
			}
			obj1.put("reasons", (Object) json);
		} 
		catch (Exception e) 
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e1)
			{
				e1.printStackTrace();
			}
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "Failed to retrieve data: "+e.getMessage());
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		rb = Response.ok(obj1.toString()).build();
		if (conn != null) 
		{
			try 
			{
				conn.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return rb;
	}

	public Response getAssignedReason(JSONObject inputJsonObj) throws JSONException {
		Response rb = null;
		JSONArray json = new JSONArray();
		JSONObject obj1 = new JSONObject();
		String metricPeriodValID = "";
		if (inputJsonObj.has("mtrc_period_val_id"))
		{
			metricPeriodValID = inputJsonObj.get("mtrc_period_val_id").toString();
		} 
		else 
		{
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "mtrc_period_val_id parameter is required");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}

		Connection conn = null;
		try
		{
			conn = ConnectionManager.mtrcConn().getConnection();
			conn.setReadOnly(true);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "DB Connection Failed");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}

		try
		{

			String SQL = "select ar.mpvr_id," + " ar.mtrc_period_val_id," + " ar.mpr_id, " + " ar.mpvr_created_on_dtm,"
					+ " ar.mpvr_created_by_uid," + " ar.mpvr_last_updt_on_dtm," + " ar.mpvr_last_updt_by_uid,"
					+ " ar.mpvr_comment," + " r.mtrc_period_id," + " r.mpr_display_text," + " r.mpr_desc "
					+ " from MTRC_MPV_REASONS ar" + " join MTRC_MP_REASON r" + " on ar.mpr_id = r.mpr_id"
					+ " where ar.mtrc_period_val_id=" + metricPeriodValID;

			Statement stmt = conn.createStatement();
			// System.out.println("statement connect done" );
			// do starts here
			ResultSet rs = stmt.executeQuery(SQL);
			ResultSetMetaData rsmd = rs.getMetaData();
			// System.out.println("result set created" );

			int numColumns = rsmd.getColumnCount();
			while (rs.next())
			{

				JSONObject obj = new JSONObject();

				for (int i = 1; i < numColumns + 1; i++)
				{
					String column_name = rsmd.getColumnName(i);

					obj.put(column_name, rs.getString(i)==null?"":rs.getString(i));

				} // for numcolumns
				json.put(obj);
			} // while loop

			rs.close();
			stmt.close();
			if (conn != null) {
				conn.close();
			}
			obj1.put("assignedreasons", (Object) json);
		} 
		catch (Exception e) 
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e1) 
			{
				e1.printStackTrace();
			}
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "Failed to retrieve data: "+e.getMessage());
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}

		rb = Response.ok(obj1.toString()).build();
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return rb;
	}
	
	public Response addUpdateAssignedReason(JSONObject inputJsonObj) throws JSONException {

		Response rb = null;
		JSONObject obj1 = new JSONObject();
		JSONArray rawInputData = inputJsonObj.getJSONArray("assignedreasons");
		Connection conn = null;
		PreparedStatement  updatePrepStmt = null;
		PreparedStatement  insertPrepStmt = null;
		try {
			conn = ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {

			e.printStackTrace();			
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "DB Connection Failed");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		String updateSQL = " update MTRC_MPV_REASONS set mpvr_comment = ?, mpvr_last_updt_by_uid = ?, mpvr_last_updt_on_dtm =?"
				         + " where mpvr_id = ?  ";
		
		String insertSQL = " insert into MTRC_MPV_REASONS (mtrc_period_val_id,mpr_id,mpvr_created_on_dtm,mpvr_created_by_uid,mpvr_comment)"
				         + " values(?,?,?,?,?)";
		try			 
		  {			
			conn.setAutoCommit(false);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			insertPrepStmt = conn.prepareStatement(insertSQL);
			int updateCounter = 0;
			int insertCounter = 0;
			for(int i=0;i<rawInputData.length();i++)
			{
				String mprId = rawInputData.getJSONObject(i).getString("mpr_id").toString();
				String metricPeriodValueId = rawInputData.getJSONObject(i).getString("mtrc_period_val_id").toString();
				String comment = rawInputData.getJSONObject(i).getString("mpvr_comment").toString().trim();
				String userId = rawInputData.getJSONObject(i).getString("user_id").toString();
				
				//if mpvr_id is part of the payload, that means we need to update the existing record,
				//if not we have to insert new record
				if (rawInputData.getJSONObject(i).has("mpvr_id")) 
				{
					updateCounter++;
					String mpvrId = rawInputData.getJSONObject(i).getString("mpvr_id").toString();
					updatePrepStmt.setString(1, comment);
					updatePrepStmt.setString(2, userId);
					updatePrepStmt.setTimestamp(3, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
					updatePrepStmt.setString(4, mpvrId);
					updatePrepStmt.addBatch();
				} //end if
				else 
				{
					insertCounter++;
					insertPrepStmt.setString(1,metricPeriodValueId );
					insertPrepStmt.setString(2,mprId );
					insertPrepStmt.setTimestamp(3, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
					insertPrepStmt.setString(4,userId);
					insertPrepStmt.setString(5,comment);
					insertPrepStmt.addBatch();					
				}//end else
									
			}//end of for loop
			
			   if(updateCounter>0){
				   
				   updatePrepStmt.executeBatch();
			   }
			   if(insertCounter>0){
				   
				   insertPrepStmt.executeBatch();
			   }
	      	   updatePrepStmt.close();
	      	   insertPrepStmt.close();
			   conn.commit();		   
	      	   conn.close();
		}//end of try
		catch(Exception e){
			try {
				conn.rollback();
				if (updatePrepStmt != null) {
			        try {
			        	updatePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }
				if (insertPrepStmt != null) {
				        try {
				        	insertPrepStmt.close();
				        } catch (SQLException e1) {  }
				    }				
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();			
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "Failed to save record: "+e.getMessage());
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		finally 
		{
		    if (updatePrepStmt != null) {
		        try {
		        	updatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (insertPrepStmt != null) {
		        try {
		        	insertPrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}
		obj1.put("result", "Success");
		obj1.put("resultCode", "100");
		obj1.put("message", "Changes have been saved");
	    rb=Response.ok(obj1.toString()).build();
		return rb;

	}

	public Response removeAssignedReason(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject obj1 = new JSONObject();
		JSONArray rawInputData = inputJsonObj.getJSONArray("reasonstodelete");
		Connection conn = null;
		PreparedStatement  deletePrepStmt = null;

		try {
			conn = ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "DB Connection Failed");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		String deleteSQL = " delete from  MTRC_MPV_REASONS "
				         + " where mpvr_id = ?  ";
		
		try			 
		 {			
			conn.setAutoCommit(false);
			deletePrepStmt = conn.prepareStatement(deleteSQL);
			for(int i=0;i<rawInputData.length();i++)
			{				
			   int mpvrId = rawInputData.getJSONObject(i).getInt("mpvr_id");
			   deletePrepStmt.setInt(1, mpvrId);
			   deletePrepStmt.addBatch();
			}//end of for loop
			
			   deletePrepStmt.executeBatch();
			   deletePrepStmt.close();
			   conn.commit();		   
	      	   conn.close();
		 }//end of try
		catch(Exception e){
			try {
				conn.rollback();
				if (deletePrepStmt != null) {
			        try {
			        	deletePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }				
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "Failed to delete record:"+e.getMessage());
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		finally 
		{
		    if (deletePrepStmt != null) {
		        try {
		        	deletePrepStmt.close();
		        } catch (SQLException e) {  }
		    }	
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}
       
        obj1.put("result", "Success");
        obj1.put("resultCode", "100");
        obj1.put("message", "Reason(s) have been deleted");
	    rb=Response.ok(obj1.toString()).build();
		return rb;
		
	
	}
	
	public Response addReason(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONArray savedData = new JSONArray();
		JSONObject obj1 = new JSONObject();
		JSONObject retJson = new JSONObject();
		JSONArray rawInputData = inputJsonObj.getJSONArray("reasons");
		Connection conn = null;
		PreparedStatement  insertPrepStmt = null;
		PreparedStatement  validatePrepStmt = null;
		ResultSet rs = null;
		int ordCount = 0;
		try {
			conn = ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			obj1.put("result", "FAILED");
			obj1.put("resultCode", "200");
			obj1.put("message", "DB Connection Failed");
			rb = Response.ok(obj1.toString()).build();			
			return rb;
		}
		
		String insertSQL = " insert into MTRC_MP_REASON (mtrc_period_id,mpr_display_text,mpr_display_order,mpr_desc,mpr_std_yn,mpr_added_on_dtm,mpr_added_by_uid,mpr_stdized_on_dtm,mpr_stdized_by_uid)"
				         + " values(?,?,?,?,?,?,?,?,?)";
		
		String SQL = "select count(*) ord_count from MTRC_MP_REASON where mtrc_period_id = ? and mpr_display_order = ?";
		try			 
		  {			
			conn.setAutoCommit(false);
			insertPrepStmt = conn.prepareStatement(insertSQL, PreparedStatement.RETURN_GENERATED_KEYS);
			validatePrepStmt = conn.prepareStatement(SQL);
			

			for(int i=0;i<rawInputData.length();i++)
			{
				String metricPeriodID = null;
				String displayText = null;
				String isStandard = null;
				String userId = null;
				String displayOrder =null;
				Timestamp addDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
				Timestamp stdDte = null;
				String stdUsr = null;
				
				if(!rawInputData.getJSONObject(i).has("mpr_std_yn")||rawInputData.getJSONObject(i).getString("mpr_std_yn").trim().equals(""))
				{												
					insertPrepStmt.close();
					validatePrepStmt.close();
					conn.rollback();						
					conn.close();

					retJson.put("result", "FAILED");
				    retJson.put("resultCode", "200");
				    retJson.put("message", "Failed to save record: Standard Y or N is Required");
					rb = Response.ok(retJson.toString()).build();
					return rb;												
				}
				else
				{
					isStandard = rawInputData.getJSONObject(i).getString("mpr_std_yn").toString().trim();
				}
				if(!rawInputData.getJSONObject(i).has("mpr_display_text")||rawInputData.getJSONObject(i).getString("mpr_display_text").trim().equals(""))
				{												
					insertPrepStmt.close();
					validatePrepStmt.close();
					conn.rollback();						
					conn.close();

					retJson.put("result", "FAILED");
				    retJson.put("resultCode", "200");
				    retJson.put("message", "Failed to save record: Display Text is Required");
					rb = Response.ok(retJson.toString()).build();
					return rb;												
				}
				else
				{
					displayText = rawInputData.getJSONObject(i).getString("mpr_display_text").toString().trim();
				}
				
				if(!rawInputData.getJSONObject(i).has("mtrc_period_id")||rawInputData.getJSONObject(i).getString("mtrc_period_id").equals(""))
				{												
					insertPrepStmt.close();
					validatePrepStmt.close();
					conn.rollback();						
					conn.close();
					retJson.put("result", "FAILED");
				    retJson.put("resultCode", "200");
				    retJson.put("message", "Failed to save record: Metric Period Id is Required ");
					rb = Response.ok(retJson.toString()).build();
					return rb;												
				}
				else
				{
					metricPeriodID = rawInputData.getJSONObject(i).getString("mtrc_period_id").toString();
				}				
				if(!rawInputData.getJSONObject(i).has("user_id")||rawInputData.getJSONObject(i).getString("user_id").equals(""))
				{												
					insertPrepStmt.close();
					validatePrepStmt.close();
					conn.rollback();						
					conn.close();
					
					retJson.put("result", "FAILED");
				    retJson.put("resultCode", "200");
				    retJson.put("message", "Failed to save record: User Id is Required");
					rb = Response.ok(retJson.toString()).build();
					return rb;												
				}
				else
				{
					userId = rawInputData.getJSONObject(i).getString("user_id").toString();
				}
				
				if(isStandard.equals("Y"))
				{
					if(!rawInputData.getJSONObject(i).has("mpr_display_order")||rawInputData.getJSONObject(i).getString("mpr_display_order").equals(""))
					{												
						insertPrepStmt.close();
						validatePrepStmt.close();
						conn.rollback();						
						conn.close();
						retJson.put("result", "FAILED");
					    retJson.put("resultCode", "200");
					    retJson.put("message", "Failed to save record:Display Order is required for standard reasons ");
						rb = Response.ok(retJson.toString()).build();
						return rb;												
					}
					else
					{
						displayOrder = rawInputData.getJSONObject(i).getString("mpr_display_order").toString();
						validatePrepStmt.setString(1, metricPeriodID);
						validatePrepStmt.setString(2,displayOrder);
						rs = validatePrepStmt.executeQuery();
						while(rs.next())
						{
							 ordCount = rs.getInt("ord_count");
						}
						if(ordCount >0)
						{
							rs.close();
							insertPrepStmt.close();
							validatePrepStmt.close();
							conn.rollback();						
							conn.close();
							retJson.put("result", "FAILED");
						    retJson.put("resultCode", "200");
						    retJson.put("message", "Failed to save record:Order "+displayOrder+" already exists for metric period id "+metricPeriodID);
							rb = Response.ok(retJson.toString()).build();
							return rb;
						}
						rs.close();
						
					}											
				}//if(isStandard.equals("Y"))
				else
				{
					isStandard ="N";
				}
			    insertPrepStmt.setString(1,metricPeriodID );
				insertPrepStmt.setString(2,displayText);
				insertPrepStmt.setString(3,displayOrder);
				if(rawInputData.getJSONObject(i).has("mpr_desc"))
				{
					insertPrepStmt.setString(4,rawInputData.getJSONObject(i).getString("mpr_desc").toString());
				}
				else
				{
					insertPrepStmt.setString(4,null);
				}
				insertPrepStmt.setString(5,isStandard);				
				insertPrepStmt.setTimestamp(6, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
				insertPrepStmt.setString(7,userId);
				if(isStandard.equals("Y"))
				{
					stdDte = addDte;
					stdUsr = userId;
					insertPrepStmt.setTimestamp(8,java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
					insertPrepStmt.setString(9,userId);
				}
				else
				{
					insertPrepStmt.setTimestamp(8,null);
					insertPrepStmt.setString(9,null);
				}
				insertPrepStmt.executeUpdate();
				rs = insertPrepStmt.getGeneratedKeys();
				if(rs != null && rs.next())
				{
				   System.out.println("Generated Id: "+rs.getInt(1));
				}
				JSONObject obj = new JSONObject();
				obj.put("mpr_id", rs.getInt(1));
				obj.put("mtrc_period_id", metricPeriodID);
				obj.put("mpr_display_text", displayText);
				obj.put("mpr_stdized_on", stdDte==null?"":stdDte);
				obj.put("mpr_stded_by_uid", stdUsr==null?"":stdUsr);
				savedData.put(obj);
				
				
				
			}//end of for loop							
			rs = insertPrepStmt.getGeneratedKeys();
			if(rs != null && rs.next())
			{
			   System.out.println("Generated Id: "+rs.getInt(1));
			}
			insertPrepStmt.close();
			conn.commit();		   
	      	conn.close();
		}//end of try
		catch(Exception e){
			try {
				
				conn.rollback();				
				if (insertPrepStmt != null) {
				        try {
				        	insertPrepStmt.close();
				        } catch (SQLException e1) {  }
				    }	
				if (validatePrepStmt != null) {
			        try {
			        	validatePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			
		
			retJson.put("result", "FAILED");
		    retJson.put("resultCode", "200");
		    retJson.put("message", "Failed to save record: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
			return rb;
		}
		
		finally 
		{		    
		    if (insertPrepStmt != null) {
		        try {
		        	insertPrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (validatePrepStmt != null) {
		        try {
		        	validatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}
        
        retJson.put("result", "Success");
        retJson.put("resultCode", "100");
        retJson.put("message", "Changes have been saved");
        retJson.put("savedData",savedData);
	    rb=Response.ok(retJson.toString()).build();
		return rb;

		
	}

	public Response editReason(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONArray savedData = new JSONArray();
		JSONObject retJson = new JSONObject();
		JSONObject rawInputData = inputJsonObj;
		Connection conn = null;
		PreparedStatement updatePrepStmt = null;
		PreparedStatement validatePrepStmt = null;
		PreparedStatement checkFlagPrepStmt = null;
		ResultSet rs = null;
		int ordCount = 0;
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
		
		String updateSQL = " update MTRC_MP_REASON"
				         + " set mpr_display_text = ?,"
				         + " mpr_std_yn = ?,"
				         + " mpr_desc = ?,"
				         + " mpr_display_order = ?,"
				         + " mpr_stdized_on_dtm = ?,"
				         + " mpr_stdized_by_uid = ? "
				         + " where mpr_id = ? "
				         + " and mtrc_period_id = ? ";
	
		
		String validateOrdSQL = "select count(*) ord_count from MTRC_MP_REASON where mtrc_period_id = ? and mpr_display_order = ? and mpr_id != ? ";
		String checkFlagSQL = "select mpr_display_order, mpr_std_yn,mpr_stdized_on_dtm,mpr_stdized_by_uid  from MTRC_MP_REASON where mpr_id = ?";
		try {
			conn.setAutoCommit(false);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			validatePrepStmt = conn.prepareStatement(validateOrdSQL);
			checkFlagPrepStmt = conn.prepareStatement(checkFlagSQL);

			String mprID = null;
			String metricPeriodID = null;
			String displayText = null;
			String isStandard = null;
			String userId = null;
			String desc = null;
			String displayOrder = null;
			Timestamp stdDte = null;
			String stdUsr = null;
			String curStdFlg = null;
			String curOrder = null;
			Timestamp curStdDte = null;
			String curStdUsr = null;
			if (!rawInputData.has("mpr_id")|| rawInputData.getString("mpr_id").equals(""))
			{
				updatePrepStmt.close();
				checkFlagPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Metric Period Reason ID is Required");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} else 
			{
				mprID = rawInputData.getString("mpr_id").toString().trim();
			}

			if (!rawInputData.has("mpr_std_yn")|| rawInputData.getString("mpr_std_yn").trim().equals(""))
			{
				updatePrepStmt.close();
				checkFlagPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Standard Y or N is Required");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} else 
			{
				isStandard = rawInputData.getString("mpr_std_yn").toString().trim();
			}
			if (!rawInputData.has("mpr_display_text")|| rawInputData.getString("mpr_display_text").trim().equals("")) 
			{
				updatePrepStmt.close();
				checkFlagPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Display Text is Required");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} 
			else 
			{
				displayText = rawInputData.getString("mpr_display_text").toString().trim();
			}

			if (!rawInputData.has("mtrc_period_id")|| rawInputData.getString("mtrc_period_id").equals(""))
			{
				updatePrepStmt.close();
				checkFlagPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Metric Period Id is Required ");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} 
			else 
			{
				metricPeriodID = rawInputData.getString("mtrc_period_id").toString();
			}
			if (!rawInputData.has("user_id")|| rawInputData.getString("user_id").equals("")) 
			{
				updatePrepStmt.close();
				checkFlagPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: User Id is Required");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} else
			{
				userId = rawInputData.getString("user_id").toString();
			}

			if (isStandard.equals("Y")) 
			{
				
				checkFlagPrepStmt.setString(1, mprID);
				rs = checkFlagPrepStmt.executeQuery();
				while (rs.next()) 
				{
					curStdFlg = rs.getString("mpr_std_yn");
					curOrder = rs.getString("mpr_display_order");
					curStdDte = rs.getTimestamp("mpr_stdized_on_dtm");
					curStdUsr = rs.getString("mpr_stdized_by_uid");
					
				}
				if(curStdFlg.equals("Y"))
				{
					displayOrder = curOrder;
					stdDte = curStdDte;
					stdUsr = curStdUsr;
					
				}
				else if (!rawInputData.has("mpr_display_order")|| rawInputData.getString("mpr_display_order").equals(""))
				{
					updatePrepStmt.close();
					checkFlagPrepStmt.close();
					validatePrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record:Display Order is required for standard reasons ");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				} 
				else 
				{
					displayOrder = rawInputData.getString("mpr_display_order").toString();
					validatePrepStmt.setString(1, metricPeriodID);
					validatePrepStmt.setString(2, displayOrder);
					validatePrepStmt.setString(3, mprID);
					rs = validatePrepStmt.executeQuery();
					while (rs.next()) 
					{
						ordCount = rs.getInt("ord_count");
					}
					if (ordCount > 0) 
					{
						rs.close();
						updatePrepStmt.close();
						checkFlagPrepStmt.close();
						validatePrepStmt.close();
						conn.rollback();
						conn.close();
						retJson.put("result", "FAILED");
						retJson.put("resultCode", "200");
						retJson.put("message", "Failed to save record:Order " + displayOrder+ " already exists for metric period id " + metricPeriodID);
						rb = Response.ok(retJson.toString()).build();
						return rb;
					}
					rs.close();
					stdDte = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now());
					stdUsr = userId;

				}
			} // if(isStandard.equals("Y"))
			else
			{
				isStandard = "N";
			}
			
			updatePrepStmt.setString(1, displayText);
			updatePrepStmt.setString(2, isStandard);
			if (rawInputData.has("mpr_desc")) 
			{
				desc = rawInputData.getString("mpr_desc").toString();
				updatePrepStmt.setString(3, rawInputData.getString("mpr_desc").toString());
			} 
			else 
			{
				updatePrepStmt.setString(3, null);
			}
			updatePrepStmt.setString(4, displayOrder);
			
			if (isStandard.equals("Y")) {

				updatePrepStmt.setTimestamp(5, stdDte);
				updatePrepStmt.setString(6, userId);
			} 
			else
			{
				updatePrepStmt.setTimestamp(5, null);
				updatePrepStmt.setString(6, null);
			}
			updatePrepStmt.setString(7, mprID);
			updatePrepStmt.setString(8, metricPeriodID);
			updatePrepStmt.executeUpdate();
						
			JSONObject obj = new JSONObject();
			obj.put("mpr_id", mprID);
			obj.put("mtrc_period_id", metricPeriodID);
			obj.put("mpr_display_text", displayText);
			obj.put("mpr_desc", desc);
			obj.put("mpr_display_order", displayOrder);
			obj.put("mpr_std_yn", isStandard);
			obj.put("mpr_stdized_on", stdDte);
			obj.put("mpr_stded_by_uid", stdUsr);
			savedData.put(obj);
			updatePrepStmt.close();
			checkFlagPrepStmt.close();
			validatePrepStmt.close();
			conn.commit();
			conn.close();
		} // end of try
		catch(Exception e){
			try {
				
				conn.rollback();		
				if (checkFlagPrepStmt != null) {
			        try {
			        	checkFlagPrepStmt.close();
			        } catch (SQLException e1) {  }
			    }	
				if (updatePrepStmt != null) {
				        try {
				        	updatePrepStmt.close();
				        } catch (SQLException e1) {  }
				    }	
				if (validatePrepStmt != null) {
			        try {
			        	validatePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }
				conn.close();
			} catch (SQLException e1) 
			{
				e1.printStackTrace();
			}
			e.printStackTrace();
			
		
			retJson.put("result", "FAILED");
		    retJson.put("resultCode", "200");
		    retJson.put("message", "Failed to save record: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
			return rb;
		}
		
		finally 
		{		    
		    if (updatePrepStmt != null) {
		        try {
		        	updatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (validatePrepStmt != null) {
		        try {
		        	validatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (checkFlagPrepStmt != null) {
		        try {
		        	checkFlagPrepStmt.close();
		        } catch (SQLException e1) {  }
		    }
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}
        
        retJson.put("result", "Success");
        retJson.put("resultCode", "100");
        retJson.put("message", "Changes have been saved");
        retJson.put("savedData",savedData);
	    rb=Response.ok(retJson.toString()).build();
		return rb;
	}

	public Response removeReason(JSONObject inputJsonObj) throws JSONException
	{

		Response rb = null;
		JSONObject retJson = new JSONObject();
		JSONObject rawInputData = inputJsonObj;
		Connection conn = null;
		PreparedStatement  deletePrepStmt = null;
		PreparedStatement  validatePrepStmt = null;
		ResultSet rs = null;
		String mprID = null;
		String deleteSQL = "delete from MTRC_MP_REASON where mpr_id = ?";
		String validateSQL = "select count(*) arcount from mtrc_mpv_reasons where mpr_id = ?";
		int arnum = 0;//number of assigned reasons assigned to a reason that we're trying to delete
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
		try
		{
			conn.setAutoCommit(false);
			deletePrepStmt = conn.prepareStatement(deleteSQL);
			validatePrepStmt = conn.prepareStatement(validateSQL);
			
			if (!rawInputData.has("mpr_id")|| rawInputData.getString("mpr_id").equals(""))
			{

				deletePrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to delete record: Metric Period Reason ID is Required");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} else 
			{
				mprID = rawInputData.getString("mpr_id").toString().trim();
			}
			validatePrepStmt.setString(1, mprID);
			rs = validatePrepStmt.executeQuery();
			if(rs != null && rs.next()) 
			{								
				arnum =rs.getInt("arcount");
			}
			if(arnum>0)
			{
				deletePrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();

				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Reason cannot be deleted. Assigned Reason exists!!!");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			}
			
			deletePrepStmt.setString(1, mprID);
			deletePrepStmt.executeUpdate();
			deletePrepStmt.close();
			validatePrepStmt.close();
			conn.commit();
			conn.close();						
		}//end of try
		catch(Exception e){
			try {
				
				conn.rollback();		
				if (deletePrepStmt != null) {
			        try {
			        	deletePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }	
				if (validatePrepStmt != null) {
				        try {
				        	validatePrepStmt.close();
				        } catch (SQLException e1) {  }
				    }	
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();				
			retJson.put("result", "FAILED");
		    retJson.put("resultCode", "200");
		    retJson.put("message", "Failed to delete record: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
			return rb;
		}		
		finally 
		{		    
		    if (deletePrepStmt != null) {
		        try {
		        	deletePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (validatePrepStmt != null) {
		        try {
		        	validatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}

        retJson.put("result", "Success");
        retJson.put("resultCode", "100");
        retJson.put("message", "Reason has been deleted");
	    rb=Response.ok(retJson.toString()).build();
		return rb;
				
	}

	public Response reorderReasons(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		JSONObject rawInputData = inputJsonObj;
		Set<String> tempSet = new HashSet<String>();
		int totalStdCount = 0;//total count of standardized reasons. the user is supposed to send the same number of reasons to reorder
		Connection conn = null;
		PreparedStatement updatePrepStmt = null;
		PreparedStatement validatePrepStmt = null;
		PreparedStatement validateCountPrepStmt = null;

		ResultSet rs = null;
		
		String validateSQL = "select mpr_id,mtrc_period_id,mpr_display_text,mpr_display_order,mpr_std_yn from MTRC_MP_REASON where mpr_id = ?";
		String validateCountSQL ="select count(*) total_std  from MTRC_MP_REASON where mpr_std_yn ='Y' and  mtrc_period_id = ? ";
		String updateSQL = " update MTRC_MP_REASON"
				         + " set mpr_display_order = ?"
				         + " where mpr_id = ?";

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
		
		try
		{
			conn.setAutoCommit(false);
			updatePrepStmt = conn.prepareStatement(updateSQL);
			validatePrepStmt = conn.prepareStatement(validateSQL);
			validateCountPrepStmt = conn.prepareStatement(validateCountSQL);
			String metricPeriodID = null;
			if (!rawInputData.has("mtrc_period_id")|| rawInputData.getString("mtrc_period_id").equals(""))
			{
				updatePrepStmt.close();
				validateCountPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Metric Period Id is Required ");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			} 
			else 
			{
				metricPeriodID = rawInputData.getString("mtrc_period_id").toString();
			}
			validateCountPrepStmt.setString(1,metricPeriodID);
			rs = validateCountPrepStmt.executeQuery();
			if(rs!=null && rs.next())
			{
				totalStdCount = rs.getInt("total_std");
				
			}			
			JSONArray reasons = rawInputData.getJSONArray("reasons");
			if(totalStdCount !=reasons.length())
			{
				updatePrepStmt.close();
				validateCountPrepStmt.close();
				validatePrepStmt.close();
				conn.rollback();
				conn.close();
				retJson.put("result", "FAILED");
				retJson.put("resultCode", "200");
				retJson.put("message", "Failed to save record: Number of submitted reasons("+reasons.length()+") doesn't match the total number of standard reasons("+totalStdCount+")");
				rb = Response.ok(retJson.toString()).build();
				return rb;
			}
			
			for(int i =0;i<reasons.length();i++)
			{
				String dbMetricPeriodID = null;
				String dbStdFlg = null;
				String dbDispTxt = null;
				String mprID = null;
				String dispOrder = null;
				
				if(!reasons.getJSONObject(i).has("mpr_id")||reasons.getJSONObject(i).getString("mpr_id").toString().equals(""))
				{
					updatePrepStmt.close();
					validatePrepStmt.close();
					validateCountPrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record: mpr_id is Required ");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				}
				else
				{
					mprID = reasons.getJSONObject(i).getString("mpr_id").toString();
				}
				if(!reasons.getJSONObject(i).has("mpr_display_order")||reasons.getJSONObject(i).getString("mpr_display_order").toString().equals(""))
				{
					updatePrepStmt.close();
					validatePrepStmt.close();
					validateCountPrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record: Display Order is Required ");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				}
				else
				{
					dispOrder = reasons.getJSONObject(i).getString("mpr_display_order").toString();
				}
				validatePrepStmt.setString(1, mprID);
				rs = validatePrepStmt.executeQuery();
				if(rs!=null && rs.next())
				{
					dbMetricPeriodID = rs.getString("mtrc_period_id");
					dbStdFlg = rs.getString("mpr_std_yn");
					dbDispTxt = rs.getString("mpr_display_text");
					rs.close();
				}
				
				if(!dbMetricPeriodID.equals(metricPeriodID))
				{
					updatePrepStmt.close();
					validatePrepStmt.close();
					validateCountPrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record:Metric Period ID doesn not match for reason "+mprID+"("+dbDispTxt+")");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				}
				if(dbStdFlg.equals("N"))
				{
					updatePrepStmt.close();
					validatePrepStmt.close();
					validateCountPrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record:Reason "+mprID+"("+dbDispTxt+") is not a standard reason");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				}
				if(!tempSet.add(dispOrder))//Set data structure doesn't allow duplicates, so if add(mprID) returns false, that means there's a duplicate order
				{
					updatePrepStmt.close();
					validatePrepStmt.close();
					validateCountPrepStmt.close();
					conn.rollback();
					conn.close();
					retJson.put("result", "FAILED");
					retJson.put("resultCode", "200");
					retJson.put("message", "Failed to save record:Display Order List Contains duplicates");
					rb = Response.ok(retJson.toString()).build();
					return rb;
				}
				updatePrepStmt.setString(1, dispOrder);
				updatePrepStmt.setString(2, mprID);
				updatePrepStmt.addBatch();
			}//end of for loop
			
			updatePrepStmt.executeBatch();
			
			validatePrepStmt.close();
			updatePrepStmt.close();
			validateCountPrepStmt.close();
			conn.commit();
			conn.close();
						
		}//end of try
		catch(Exception e){
			try {
				
				conn.rollback();							
				if (updatePrepStmt != null) {
				        try {
				        	updatePrepStmt.close();
				        } catch (SQLException e1) {  }
				    }	
				if (validateCountPrepStmt != null) {
			        try {
			        	validateCountPrepStmt.close();
			        } catch (SQLException e1) {  }
			    }	
				if (validatePrepStmt != null) {
			        try {
			        	validatePrepStmt.close();
			        } catch (SQLException e1) {  }
			    }
				conn.close();
			} catch (SQLException e1) {

				e1.printStackTrace();
			}
			e.printStackTrace();
					
			retJson.put("result", "FAILED");
		    retJson.put("resultCode", "200");
		    retJson.put("message", "Failed to save record: "+e.getMessage());
			rb = Response.ok(retJson.toString()).build();
			return rb;
		}		
		finally 
		{		    
		    if (updatePrepStmt != null) {
		        try {
		        	updatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (validatePrepStmt != null) {
		        try {
		        	validatePrepStmt.close();
		        } catch (SQLException e) {  }
		    }
		    if (validateCountPrepStmt != null) {
		        try {
		        	validateCountPrepStmt.close();
		        } catch (SQLException e1) {  }
		    }
		    if (conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException e) {  }
		   
		    }
		}//end of finally
		
	    retJson.put("result", "Success");
        retJson.put("resultCode", "100");
        retJson.put("message", "Changes have been saved");
	    rb=Response.ok(retJson.toString()).build();
		return rb;
	}//end of reorderReasons
}
