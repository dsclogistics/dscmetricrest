package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.ws.rs.core.Response;


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


import java.io.*;
import java.net.*;
 
import com.dsc.mtrc.dao.ConnectionManager;
import com.dsc.mtrc.util.DateHelper;

public class MetricAutoLoader {
	
		
	public Response loadMetric(JSONObject inputJsonObj) throws JSONException {
	    Response rb = null;
		String  msg = null;
		String theurl="";
		String dwurl="";
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		String insstmt=null;
		String mtrcnayn=null;
        StringBuffer sb = new StringBuffer();  
        String insertSQL = null;
        String updateSQL = null;
        String SQL = null;
        String tptName = null;
        String metricId = null;
		String pkagename = null;		//pkachage name represents MTRC_METRIC.mtrc_token value
		String dwEndpoint = null;       // End Point for a specific DW api. 
		String loadStatusPackageName = null; //package name that DW loadstatus api can accept
		List<Integer> editableBuildings = new ArrayList<Integer>();		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn= ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
            msg="Metric DB Connection Failed.";
            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            return rb;

		}

		if (((! inputJsonObj.has("packagename"))&&(! inputJsonObj.has("mtrc_id"))) || (! inputJsonObj.has("calyear")) || (! inputJsonObj.has("calmonth")) )
		{
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "packagename or mtrc_id and calmonth and calyear   json tags required for this API"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	       	return rb;			
		}
		if(inputJsonObj.getString("mtrc_id")!=null ||!inputJsonObj.getString("mtrc_id").isEmpty())
		{
			metricId = inputJsonObj.get("mtrc_id").toString();
			SQL = "select mtrc_token" +
				  " from  MTRC_METRIC" +
				  " where mtrc_id = "+ metricId;
			System.out.println(SQL);
			try
			{
				//conn= ConnectionManager.mtrcConn().getConnection();
				stmt = conn.createStatement();
	      	    rs = stmt.executeQuery(SQL);
	      		while(rs.next())
	      		{
	      			pkagename= rs.getString("mtrc_token");

	      		}
	      		rs.close();
	      		stmt.close();
	      		//conn.close();
	      		if(pkagename ==null){	      			
		            msg="Invalid value for metric id or metric token.";
		            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
			            rb=Response.ok(sb.toString()).build();
			            if (conn != null) { try {
							conn.close();
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}} 
			            return rb;
	      			
	      		}
	      		
			}
			catch (Exception e) 
	      	{
	      		e.printStackTrace();
	            msg="Cannot find metric token.";
	            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
		            rb=Response.ok(sb.toString()).build();
		            if (conn != null) { try {
						conn.close();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}} 
		            return rb;
	 	 
	      	 }
		}
		else if(inputJsonObj.getString("packagename")!=null ||!inputJsonObj.getString("packagename").isEmpty())
		{
			pkagename= inputJsonObj.get("packagename").toString().trim();
			SQL = "select mtrc_id" +
					  " from  MTRC_METRIC" +
					  " where mtrc_token = '"+ pkagename+"'";
				try
				{
					//conn= ConnectionManager.mtrcConn().getConnection();
					stmt = conn.createStatement();
		      		rs = stmt.executeQuery(SQL);
		      		while(rs.next())
		      		{
		      			metricId= rs.getString("mtrc_id");

		      		}
		      		rs.close();
		      		stmt.close();
		      		//conn.close();
				}
				catch (Exception e) 
		      	{
		      		e.printStackTrace();
		      		msg="Cannot Find Metric ID";
		            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
			            rb=Response.ok(sb.toString()).build();
			            if (conn != null) { try {
							conn.close();
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}} 
			            return rb;
		 	 
		      	 }
			
		}
		else
		{
			  msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "packagename or mtrc_id required for this API"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	       	return rb;
		}
	    String calMonth  = inputJsonObj.get("calmonth").toString();
	    String thisyear  =inputJsonObj.get("calyear").toString();
	    tptName = inputJsonObj.get("tptname").toString();
	    
	    //now we need to figure our what data warehouse end point and package name for load status are based on package name(metric token)
	    switch(pkagename){
		case "NET_FTE":
			dwEndpoint ="dscwmsnetfte";
			loadStatusPackageName ="netfte";			
			break;
		case "TRAINEES_PCT":
			dwEndpoint = "dscwmstrainees";
			loadStatusPackageName ="trainee";
		     break;
		case "THROUGHPUT_CHG_PCT":
			dwEndpoint ="dscwmsvolume";
			loadStatusPackageName = "volume";
			break;
			default:
				 msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
		        		   "Cannot determine DW API to call."  +"\"}";
		           rb=Response.ok(msg.toString()).build();
		       	return rb;
		}
	    
      	JSONArray tput = new JSONArray();
 		 
		try {
		    Context ctx = new InitialContext();
		    ctx = (Context) ctx.lookup("java:comp/env");
		    theurl = (String) ctx.lookup("mtrcurl");
		}
		catch (NamingException e) {
	           
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "Cannot access/find mtrcurl in context.xml"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	       	return rb;
		}	
		
		try {
		    Context ctx = new InitialContext();
		    ctx = (Context) ctx.lookup("java:comp/env");
		    dwurl = (String) ctx.lookup("dwurl");
		}
		catch (NamingException e) {
	          
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "Cannot access/find dwurl in context.xml"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	       	return rb;
		}	
 
        // check load status from DW to see if the metric is ready to load
		
		// String pkagename="volume";
	
		msg=LoadStatus(dwurl,loadStatusPackageName,calMonth,thisyear);
		if (! msg.equals("0"))
		{
			 msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   pkagename +" DW load was not complete for "+calMonth +" " +thisyear +" "  +"\"}";
			   rb=Response.ok(msg.toString()).build();
		       	return rb;
		}

		
		SQL = " select b.tm_period_id, c.mtrc_period_id "+
			  " from MTRC_TIME_PERIOD_TYPE a "+
		      " join MTRC_TM_PERIODS b on a.tpt_id = b.tpt_id "+
			  " join MTRC_METRIC_PERIOD c on a.tpt_id =c.tpt_id"+
		      " where a.tpt_name ='" +tptName +"'"+
		      " and CONVERT(VARCHAR(10),b.tm_per_start_dtm,20)='"+DateHelper.getMonthFirstDay(calMonth, thisyear) +"'"+
		      " and CONVERT(VARCHAR(10),tm_per_end_dtm,20)= '"+DateHelper.getMonthLastDay(calMonth, thisyear) +"'"+
		      " and c.mtrc_id =" +metricId; 
		try
		{
			//conn= ConnectionManager.mtrcConn().getConnection();
			stmt = conn.createStatement();
      		rs = stmt.executeQuery(SQL);
      		while(rs.next())
      		{
      			mtrcperiodid= rs.getString("mtrc_period_id");
      			tmperiodid= rs.getString("tm_period_id");
      		}
      		rs.close();
      		stmt.close();
      		//conn.close();
		}
		catch (Exception e) 
      	{
      		e.printStackTrace();
            msg="Cannot Find Metric Period ID or TM Period ID .";
            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            if (conn != null) { try {
					conn.close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}} 
	            return rb;
 	 
      	 }
		/* before we call dw api and to get all the metric data for buildings we need to determine the list of buildings
		 * that we are allowed to update
		 * if editable flag is Y or building is not effective we DO NOT update the table mtrc_metric_period_value.
		*/
		
		//So, lets get the list of buildings we're allowed to update first:
		//We only need to select building that were open during this period with editable flag set to N
		
		SQL = "select bm.dsc_mtrc_lc_bldg_id as building_id"
				+ " from MTRC_BLDG_MTRC_PERIOD bm "
				+ " join DSC_MTRC_LC_BLDG b"
				+ " on bm.dsc_mtrc_lc_bldg_id = b.dsc_mtrc_lc_bldg_id"
				+ " where bm.mtrc_period_id ="+mtrcperiodid
				+ " and bm.bmp_is_editable_yn ='N'"
				+ " and (select cast(tm_per_start_dtm as date)  from MTRC_TM_PERIODS where MTRC_TM_PERIODS.tm_period_id="+tmperiodid+" )"
				+ " between  b.dsc_mtrc_lc_bldg_eff_start_dt and b.dsc_mtrc_lc_bldg_eff_end_dt"
				+ " and (select cast(tm_per_end_dtm as date) from MTRC_TM_PERIODS where MTRC_TM_PERIODS.tm_period_id="+tmperiodid+" )"
				+ " between b.dsc_mtrc_lc_bldg_eff_start_dt and b.dsc_mtrc_lc_bldg_eff_end_dt";
		
		try
		{
			 //conn= ConnectionManager.mtrcConn().getConnection();
			 stmt = conn.createStatement();
			 rs = stmt.executeQuery(SQL);
			 while(rs.next())
			 {				 
				 editableBuildings.add(rs.getInt("building_id"));			 
			 }
			 rs.close();
	      	 stmt.close();
	      	 //conn.close();			 			 
		}//end of try
		catch (Exception e) 
      	{
      		e.printStackTrace();
            msg="Cannot find the list of buildings to update .";
            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            if (conn != null) { try {
					conn.close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}} 
	            return rb;
 	 
      	 }//end of catch
		
		
		// get metric
		tput =dscwmsvolume(dwurl,calMonth, thisyear,metricId,dwEndpoint);
		 PreparedStatement  updatePrepStmt = null;
		 PreparedStatement  insertPrepStmt = null;
		try 
		{
		   	
		   //conn= ConnectionManager.mtrcConn().getConnection();
		   conn.setAutoCommit(false);
		   updateSQL = "update mtrc_metric_period_value set mtrc_period_val_value = ?, mtrc_period_val_upd_dtm = ?, mtrc_period_val_upd_by_user_id = ?  where mtrc_period_id = ? and tm_period_id = ? and dsc_mtrc_lc_bldg_id = ? "; 
		   insertSQL = "insert into mtrc_metric_period_value (mtrc_period_id,dsc_mtrc_lc_bldg_id,tm_period_id,mtrc_period_val_added_dtm,mtrc_period_val_added_by_usr_id,mtrc_period_val_upd_dtm,mtrc_period_val_upd_by_user_id,mtrc_period_val_is_na_yn,mtrc_period_val_value)"+
		               "values(?,?,?,?,?,?,?,?,?)";
		   stmt = conn.createStatement(); 
		   updatePrepStmt = conn.prepareStatement(updateSQL);		   
	       insertPrepStmt = conn.prepareStatement(insertSQL);
	       int updateCounter = 0;
	       int insertCounter = 0;
		   for(int i=0; i<tput.length(); i++)         
	        { 
	        	JSONObject s1 =  (JSONObject) tput.get(i);
	        	
	        	if(!editableBuildings.isEmpty() && (editableBuildings.indexOf(s1.getInt("dsc_mtrc_lc_bldg_id"))!=-1))
	        	  {//if list of editable buildings is not empty 
	        		     //and building id returned by dw api is in the list of editable buildings
	        		SQL = " select count(*) as row_count from mtrc_metric_period_value  where mtrc_period_id="+mtrcperiodid+
	 	        	 	   " and tm_period_id ="+tmperiodid+
	 	        	 	   " and dsc_mtrc_lc_bldg_id ="+ s1.getString("dsc_mtrc_lc_bldg_id");
	 	        	 System.out.println("check sql is "+SQL);
	 	        	 rs = stmt.executeQuery(SQL);
	 	             while (rs.next()) 
	 		       	  {      
	 		       			if(rs.getInt("row_count")>0)//check if this record already exists in the db
	 		       			{
	 		       				updateCounter++;
	 		       				updatePrepStmt.setString(1, pkagename.equals("NET_FTE")?s1.getString("TOTAL_NET_FTE"):s1.getString("PeriodValue"));
	 		       				updatePrepStmt.setTimestamp(2, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
	 		       				updatePrepStmt.setString(3, "API");
	 		       				updatePrepStmt.setInt(4, Integer.parseInt(mtrcperiodid));
	 		       				updatePrepStmt.setInt(5, Integer.parseInt(tmperiodid));
	 		       				updatePrepStmt.setInt(6, s1.getInt("dsc_mtrc_lc_bldg_id"));	
	 		       				updatePrepStmt.addBatch();
	 		       			}
	 		       			else
	 		       			{
	 		       				insertCounter++;
	 		       				insertPrepStmt.setInt(1, Integer.parseInt(mtrcperiodid));
	 		       				insertPrepStmt.setInt(2, s1.getInt("dsc_mtrc_lc_bldg_id"));	
	 		       				insertPrepStmt.setInt(3, Integer.parseInt(tmperiodid));
	 		       				insertPrepStmt.setTimestamp(4, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
	 		       				insertPrepStmt.setString(5, "API");
	 		       				insertPrepStmt.setTimestamp(6, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));		
	 		       				insertPrepStmt.setString(7, "API");
	 		       				insertPrepStmt.setString(8, "N");
	 		       				insertPrepStmt.setString(9, pkagename.equals("NET_FTE")?s1.getString("TOTAL_NET_FTE"):s1.getString("PeriodValue"));
	 		       				insertPrepStmt.addBatch();	  
	 		       			}
	 		       		}//end of while
	 	                rs.close();	 	        		
	        	  }
	        
	        } // for each array
		   if(updateCounter>0){
			   
			   updatePrepStmt.executeBatch();
		   }
		   if(insertCounter>0){
			   
			   insertPrepStmt.executeBatch();
		   }
		   stmt.close();
      	   updatePrepStmt.close();
      	   insertPrepStmt.close();
		   conn.commit();		   
      	   conn.close();		   
		} catch (Exception e) 
		{
				
			try 
			{
				conn.rollback();
		
			} 
			catch(SQLException e1)
			{
				e1.printStackTrace();
			}
			if (stmt != null)
			{ 
				try 
				{
					stmt.close();
				}
				catch (SQLException e1c) 
				{
					// TODO Auto-generated catch block
					e1c.printStackTrace();
				}
			}
			if (insertPrepStmt != null)
			{ 
				try 
				{
					insertPrepStmt.close();
				}
				catch (SQLException e1c) 
				{
					// TODO Auto-generated catch block
					e1c.printStackTrace();
				}
			}
			if (updatePrepStmt != null)
			{ 
				try 
				{
					updatePrepStmt.close();
				}
				catch (SQLException e1c) 
				{
					// TODO Auto-generated catch block
					e1c.printStackTrace();
				}
			}
			if (conn != null)
			{ 
				try 
				{
					conn.close();
				}
				catch (SQLException e1c) 
				{
					// TODO Auto-generated catch block
					e1c.printStackTrace();
				}
			}
			
			//
               msg="Metric DB Connection Failed.";
              sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
 	          rb=Response.ok(sb.toString()).build();
 	          return rb;
		}
		finally{
			 try {
			        if (rs != null)
			            rs.close();
			    } catch (Exception e) {
			        e.printStackTrace();
			    }
			 try {
			        if (stmt != null)
			            stmt.close();
			    } catch (Exception e) {
			    	e.printStackTrace();}
			 try {
			        if (conn != null)
			        	conn.close();
			    } catch (Exception e) {
			    	e.printStackTrace();}
			 
		}

         System.out.println("Success");
 	 	 msg="Metric Loaded Successfully .";
         sb.append("{\"result\":\"SUCCESS\",\"resultCode\":100,\"message\":\""+msg+"\"}");
   	     rb=Response.ok(sb.toString()).build();
  	return rb;
}
	
	// ***********************************************************************************************
	
 public String WMSBuilding (String theurl ,String LCBLD)
 {
         String   msg = null;
		JSONObject api = new JSONObject();
		JSONObject obj1 = new JSONObject();
		JSONArray tput = new JSONArray();
	    String query=null;	
	     URL url = null;
	    URLConnection urlc = null;
        PrintStream ps = null;
        BufferedReader br = null;
        StringBuilder  responseStrBuildera = new StringBuilder();
		String data=null;
	    String l = null;
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		String insstmt=null;
		String mtrcnayn=null;
  	 
	    	 // Calling WMS Building 
 
	    		    url = null;
	    			try {
	    				url = new URL(theurl + "wmsbuilding");
	    			} catch (MalformedURLException e1) {
	    				// TODO Auto-generated catch block
	    				e1.printStackTrace();
	    			}
 
	    		     query =  " {'building':'" + LCBLD +"'}";
	    		     // result will be:  {"building":"BP2"}
	    		     //make connection
	    		      urlc = null;
	    				try {
	    					urlc = url.openConnection();
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    		        urlc.setRequestProperty("Content-Type","application/json");
	    		        //use post mode
	    		        urlc.setDoOutput(true);
	    		        urlc.setAllowUserInteraction(false);

	    		        //send query
	    		          ps = null;
	    				try {
	    					ps = new PrintStream(urlc.getOutputStream());
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    		        ps.print(query);
	    		        ps.close();

	    		        //get result
	    		          br = null;
	    				try {
	    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    				  data=null;
	    		          l = null;
	    		         responseStrBuildera = new StringBuilder();



	    			        try {
	    						while ((l=br.readLine())!=null) {
	    							data=data+l;
	    							responseStrBuildera.append(l);
	    						 //   System.out.println(l);
	    						}
	    					} catch (IOException e1) {
	    						// TODO Auto-generated catch block
	    						e1.printStackTrace();
	    					}
	    		        try {
	    					br.close();
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}		
	    		       try
	    		       {
	    		        if (data != null)     api = new JSONObject(responseStrBuildera.toString());
	    	 
	    		         if(api.has("dsc_mtrc_lc_bldg_id")) 
	    		        	 {
	    		        	 dscmtrclcbldid=api.get("dsc_mtrc_lc_bldg_id").toString();    
	    		        	 }
	    		         else
	    		         {
	    			   
	    			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	    			        		   "wmsbuilding api failed to retrun WMS LC BLD ID"  +"\"}";
	    			          
	    		         }
	    		       }
	    		       catch (Exception e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}	
	    		  
	return dscmtrclcbldid;
	 
	 
 }
//***********************************************************************************************
  public JSONArray dscwmsvolume(String dwurl,String previousMonth, String thisyear,String metricid, String endpoint)
  {
      String   msg = null;
		JSONObject api = new JSONObject();
		JSONObject obj1 = new JSONObject();
		JSONArray tput = new JSONArray();
	    String query=null;	
	     URL url = null;
	    URLConnection urlc = null;
      PrintStream ps = null;
      BufferedReader br = null;
		String data=null;
	    String l = null;
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		String insstmt=null;
		String mtrcnayn=null;
		String jsonArrayName =null;
    		 // =========================================================   
		    		         
		    		  // call DW metric for each of the json array call wms building to get building to insert data
	 
		    		    		    url = null;
		    		    			try {
		    		    				url = new URL(dwurl + endpoint);
		    		    			} catch (MalformedURLException e1) {
		    		    				// TODO Auto-generated catch block
		    		    				e1.printStackTrace();
		    		    			}
		    		    		      
		    		    		     query= " {\"productname\":\"Red Zone\", \"tptname\":\"Month\",\"mtrcid\":"+metricid +",\"calmonth\":\""+
		    		    		     previousMonth +"\",\"calyear\":"+thisyear +"}";
		    		    		     // result will be:  {"building":"BP2"}
		    		    		     
		    		    		     
		    		      System.out.println("query being sent:"+query);
		    		   	     System.out.println("URL is:"+url.toString());
		    		    		      urlc = null;
		    		    				try {
		    		    					urlc = url.openConnection();
		    		    				} catch (IOException e1) {
		    		    					// TODO Auto-generated catch block
		    		    					e1.printStackTrace();
		    		    				}
		    		    		        urlc.setRequestProperty("Content-Type","application/json");
		    		    		        //use post mode
		    		    		        urlc.setDoOutput(true);
		    		    		        urlc.setAllowUserInteraction(false);

		    		    		        //send query
		    		    		          ps = null;
		    		    				try {
		    		    					ps = new PrintStream(urlc.getOutputStream());
		    		    				} catch (IOException e1) {
		    		    					// TODO Auto-generated catch block
		    		    					e1.printStackTrace();
		    		    				}
		    		    		        ps.print(query);
		    		    		        ps.close();

		    		    		        //get result
		    		    		          br = null;
		    		    				try {
		    		    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
		    		    				} catch (IOException e1) {
		    		    					// TODO Auto-generated catch block
		    		    					e1.printStackTrace();
		    		    				}
		    		    				  data=null;
		    		    		          l = null;
		    		    		          StringBuilder responseStrBuilderb = new StringBuilder();



		    		    			        try {
		    		    						while ((l=br.readLine())!=null) {
		    		    							data=data+l;
		    		    							responseStrBuilderb.append(l);
		    		    						     System.out.println(l);
		    		    						}
		    		    					} catch (IOException e1) {
		    		    						// TODO Auto-generated catch block
		    		    						e1.printStackTrace();
		    		    					}
		    		    		        try {
		    		    					br.close();
		    		    				} catch (IOException e1) {
		    		    					// TODO Auto-generated catch block
		    		    					e1.printStackTrace();
		    		    				}		
		    		    		
		    		    		      try
		    		    		      {
		    		    		        if (data != null)     api = new JSONObject(responseStrBuilderb.toString());
		    		    		        switch(endpoint)
		    		    		        {
		    		    		       
		    		    				   case "dscwmsvolume":
		    		    					   jsonArrayName ="DSCWMSVolumes";		    		    					
		    		    				   break;
		    		    				   case "dscwmstrainees":
		    		    					   jsonArrayName ="DSCWMSTrainees";
  		    		    				    break;
		    		    				   case "dscwmsnetfte":
		    		    					jsonArrayName ="DSCWMSNetFTE";
		    		    				   break;		    		    		        
		    		    		        }		    		    	 
		    		    		         if(api.has(jsonArrayName)) 
		    		    		        	 {
		    		    		        	 tput=(JSONArray) api.get(jsonArrayName);    
		    		    		        	 }
		    		    		         else
		    		    		         {
		    		    			         //  msg[0]="-1";
		    		    			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
		    		    			        		   "wmsbuilding api failed to retrun WMS LC BLD ID"  +"\"}";
		    		    			         // return msg; 
		    		    		         }
		    		    		      }
		    		    		      catch (Exception e1) {
		    		    					// TODO Auto-generated catch block
		    		    					e1.printStackTrace();
		    		    				}	
		    	
		    		  
	return tput;
	  
	  
  }
  
//***********************************************************************************************
	public String [] metricname(String theurl,String previousMonth, String thisyear)
	{
        String [] msg = new String [2];
		JSONObject api = new JSONObject();
		JSONObject obj1 = new JSONObject();
		JSONArray results = new JSONArray();
	    String query=null;	
	     URL url = null;
	    URLConnection urlc = null;
        PrintStream ps = null;
        BufferedReader br = null;
		String data=null;
	    String l = null;
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		String insstmt=null;
		String mtrcnayn=null;
 
		    			try {
		    				url = new URL(theurl + "metricname");
		    			} catch (MalformedURLException e1) {
		    				// TODO Auto-generated catch block
		    				e1.printStackTrace();
		    				msg[0]="Malformed metricname url:"+url;
		    				return msg;
		    			}
		    		     query =  " {\"productname\":\"Red Zone\", \"tptname\":\"Month\",\"calmonth\":\"" +
		    			           previousMonth +"\",\"calyear\":"+thisyear +",\"metricname\":\"Throughput Chg %\"}";
		    		//    System.out.println("query is:"+query);
		    		     // result will be:  {
		    		   //  "metricdetail": {
		    		   // 	    "tptname": "Month",
		    		   // 	    "productname": "Red Zone"
		    		   // 	  },
		    		   // 	  "metriclist": [
		    		   // 	    {
		    		   // 	      "mtrc_name": "Throughput Chg %",
		    		   // 	      "mtrc_id": "18",
		    		   // 	      "mtrc_period_id": "6"
		    		   // 	    }
		    		   // 	  ]
		    		   // 	}
		    		     //make connection
		    		      urlc = null;
		    				try {
		    					urlc = url.openConnection();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
			    				msg[0]="Open connection failed to metricname url:"+url;
			    				return msg;
		    				}
		    		        urlc.setRequestProperty("Content-Type","application/json");
		    		        //use post mode
		    		        urlc.setDoOutput(true);
		    		        urlc.setAllowUserInteraction(false);

		    		        //send query
		    		          ps = null;
		    				try {
		    					ps = new PrintStream(urlc.getOutputStream());
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
			    				msg[0]="Sending query failed to metricname url:"+url;
			    				return msg;
		    				}
		    		        ps.print(query);
		    		        ps.close();

		    		        //get result
		    		          br = null;
		    				try {
		    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
			    				msg[0]="Read Json string failed from metricname url:"+url+" "+e1.getMessage();
			    				return msg;
		    				}
		    				  data=null;
		    		          l = null;
		    		          StringBuilder  responseStrBuildera = new StringBuilder();



		    			        try {
		    						while ((l=br.readLine())!=null) {
		    							data=data+l;
		    							responseStrBuildera.append(l);
		    						 //   System.out.println(l);
		    						}
		    					} catch (IOException e1) {
		    						// TODO Auto-generated catch block
		    						e1.printStackTrace();
				    				msg[0]="Read Json string failed from metricname url:"+url;
				    				return msg;
		    					}
		    		        try {
		    					br.close();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
			    				msg[0]="Closing POST read  failed from metricname url:"+url;
			    				return msg;
		    				}		
		    		
		    		        if (data != null)
								try {
									api = new JSONObject(responseStrBuildera.toString());
								} catch (JSONException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
				    				msg[0]="Cannot parse JSON from metricname url:"+responseStrBuildera.toString();
				    				return msg;
								}
		    	 
		    		         if(api.has("metriclist")) 
		    		        	 {
		    		         // md =  (JSONObject) api.get("metricdetail"); 
		    		          try {
								results = api.getJSONArray("metriclist");
							} catch (JSONException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
			    				msg[0]="Published metricname API missing metriclist JSONArray. Invalid Json";
			    				return msg;
							}
		    		        	    
		    		        	 }
		    		         else
		    		         {
		    			         //  msg[0]="-1";
				    				msg[0]="Published metricname API missing metriclist JSONArray. Invalid Json";
				    				return msg;
		    		         }
 
		    try
		    {
		    	for (int i=0; i<results.length(); i++) 
		    	{
		    	JSONObject first = results.getJSONObject(i);
		    	if (first.has("mtrc_period_id")){
		    		//obj1.put("mtrcpid", first.get("mtrc_period_id").toString());
		    		//obj1.put("mtrcnayn", first.get("mtrc_period_na_allow_yn").toString());
		    		mtrcperiodid=first.get("mtrc_period_id").toString();
		    		mtrcnayn=first.get("mtrc_period_na_allow_yn").toString();
		    		}
		    	// System.out.println("mtrci period id is:"+first.get("mtrc_period_id").toString());}

		    	}
		    }
	    	     catch (JSONException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
				msg[0]="Published metricname API missing mtrc_period_id & mtrc_period_na_allow_yn tags. Invalid Json";
				return msg;
    	    } 
			msg[1]=mtrcperiodid +","+mtrcnayn;   
		   
		return msg;
		
	}
	// ************************************************************************************
	public String metrictimeperiod(String theurl,String previousMonth, String thisyear)
	{
		JSONObject api = new JSONObject();
	    String query=null;	
	    String msg="";
	     URL url = null;
		try {
			url = new URL(theurl + "metrictimeperiod");
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			
		}
	     query =  "{ 'tptname':'Month','calmonth':'" +previousMonth +"','calyear':"+thisyear +"}";
	     System.out.println("Metricperiod url is:"+url +" param:"+query);
	     // result will be:"tm_period_id":"4","tpt_id":"6"
	     //make connection
	        URLConnection urlc = null;
			try {
				urlc = url.openConnection();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				msg="1"; // 1 = URL Open connection failed
				return msg;
			}
	        urlc.setRequestProperty("Content-Type","application/json");
 	        //use post mode
	        urlc.setDoOutput(true);
	        urlc.setAllowUserInteraction(false);

	        //send query
	        PrintStream ps = null;

			try {
				ps = new PrintStream(urlc.getOutputStream());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				msg="2"; // 2 = URL Catch back failed
				return msg;
			}
	        ps.print(query);
	        ps.close();

	        //get result
	        BufferedReader br = null;
	       JSONObject xx = new JSONObject();
	      
			try {
				br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				msg="3"; // 3 = Input Read buffer failed
				return msg;
			}
			 
		//	System.out.println("buffer read after the call"+br.toString());
  			String data=null;
	        String l = null;
	        StringBuilder responseStrBuilder = new StringBuilder();

	        try {
				while ((l=br.readLine())!=null) {
					data=data+l;
					responseStrBuilder.append(l);
				 //   System.out.println(l);
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				msg="3"; // 3 = Input Read buffer failed
				return msg;
			}
	        try {
				br.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				msg="3"; // 3 = Input Read buffer failed
				return msg;
			}		


	     catch (Exception e) {
	         e.printStackTrace();
				msg="3"; // 3 = Input Read buffer failed
				return msg;
	       }
	        
        
	         if (data != null) 
	        	 {
	        	  try {
					api = new JSONObject(responseStrBuilder.toString());
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	  
	        	 
	        	 }

	         if(api.has("tm_period_id")) 
	        	 {
	        	 try {
					msg=api.get("tm_period_id").toString();
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}    
	        	 }
	         else
	         {
		         
		           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
		        		   "metrictimeperiod api failed to retrun Time Period ID"  +"\"}";
		          
	         }
	        
 
		return msg;
		
	}
	
	// verify if we can load the metrics
	// ************************************************************************************
		public String LoadStatus(String theurl,String pkagename,String previousMonth, String thisyear)
		{
			JSONObject api = new JSONObject();
		    String query=null;	
		    String msg="9";
		     URL url = null;
			try {
				url = new URL(theurl + "loadstatus");
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				
			}
		     query =  "{ 'packagename':'"+pkagename +"','calmonth':'" +previousMonth +"','calyear':"+thisyear +"}";
	         System.out.println(" URL being called:"+url +" query:"+query);
		     //make connection
		        URLConnection urlc = null;
				try {
					urlc = url.openConnection();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					msg="1"; // 1 = URL Open connection failed
					return msg;
				}
		        urlc.setRequestProperty("Content-Type","application/json");
	 	        //use post mode
		        urlc.setDoOutput(true);
		        urlc.setAllowUserInteraction(false);

		        //send query
		        PrintStream ps = null;

				try {
					ps = new PrintStream(urlc.getOutputStream());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					msg="2"; // 2 = URL Catch back failed
					return msg;
				}
		        ps.print(query);
		        ps.close();

		        //get result
		        BufferedReader br = null;
		       JSONObject xx = new JSONObject();
		      
				try {
					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					msg="3"; // 3 = Input Read buffer failed
					return msg;
				}
				 
			//	System.out.println("buffer read after the call"+br.toString());
	  			String data=null;
		        String l = null;
		        StringBuilder responseStrBuilder = new StringBuilder();

		        try {
					while ((l=br.readLine())!=null) {
						data=data+l;
						responseStrBuilder.append(l);
					 //   System.out.println(l);
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					msg="3"; // 3 = Input Read buffer failed
					return msg;
				}
		        try {
					br.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					msg="3"; // 3 = Input Read buffer failed
					return msg;
				}		


		     catch (Exception e) {
		         e.printStackTrace();
					msg="3"; // 3 = Input Read buffer failed
					return msg;
		       }
		        
	        
		         if (data != null) 
		        	 {
		        	  try {
						api = new JSONObject(responseStrBuilder.toString());
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        	  
		        	 
		        	 }

		         if(api.has("loadstatus")) 
		        	 {
		        	 msg="0";    
		        	 }
		         else
		         {
			         
			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
			        		   "metrictimeperiod api failed to retrun Time Period ID"  +"\"}";
			           msg="9";
			          
		         }
		        
	 
			return msg;
			
		}
}

 
