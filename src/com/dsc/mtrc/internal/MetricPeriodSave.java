 
package com.dsc.mtrc.internal;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.mtrc.dao.*;


public class MetricPeriodSave  {
	 
	
	public String[] MetricPeriodSave(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		 int loccount=0;
		 StringBuffer sb = new StringBuffer();
		 String [] msg = new String[2];
		// JSONObject obj1 = new JSONObject();
		 System.out.println(inputJsonObj);
		 Connection conn = null;
			try {
				conn= ConnectionManager.mtrcConn().getConnection();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
               msg[0]="-1";
               msg[1]= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
            		   "Metric DB Connection Failed."  +"\"}";
	          return msg;
			}
			
		 
		 // get Metric Detail info and location details
 
     	JSONObject md =  (JSONObject) inputJsonObj.get("metricdetail"); 
      	JSONArray results = inputJsonObj.getJSONArray("locationdetails");
      	
      	String tmperstartdtm=md.get("tm_per_start_dtm").toString();
      	String tmperenddtm=md.get("tm_per_end_dtm").toString();
      	String mtrcperiodid=md.get("mtrc_period_id").toString();
    	String mtrcid=md.get("mtrc_id").toString();
    	String pctyn="N";
    	if (md.has("data_type_token")) 
    	{
    		if (md.getString("data_type_token").toString().equals("pct")) 
    			{
                     pctyn="Y";
    			}
    	}
    	 
      
      	// Get TPT ID
      	String SQL="select tm_period_id,tpt_id from mtrc_tm_periods where tm_per_start_dtm ='" +tmperstartdtm +"' and " +
      			   " tm_per_end_dtm ='" + tmperenddtm +"'";
      	int tptid=0;
        int tmperiodid=0;
      	try
      	{
      	
      		Statement stmt = conn.createStatement();
      		ResultSet rs = stmt.executeQuery(SQL);
      		ResultSetMetaData rsmd = rs.getMetaData();
      		int numColumns = rsmd.getColumnCount(); 
      		while (rs.next()) 
      		{      	
      			tptid= rs.getInt("tpt_id");
      			tmperiodid= rs.getInt("tm_period_id");
      		}
      		rs.close();
      		
         	stmt.close();	
      		 
      		 
      	}
      	 catch (SQLException e) 
      	{
      		e.printStackTrace();
            msg[0]="-1";
            msg[1]= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
         		   "Metric DB Connection Failed."  +"\"}";
            if (conn != null) { try {
				conn.close();
			} catch (SQLException e1b) {
				// TODO Auto-generated catch block
				// e1.printStackTrace();
			}} 
	          return msg;  		 
      	 }
      	// for each location insert/update period values
     	for (int i=0; i<results.length(); i++) 
     	{
     		JSONObject first = results.getJSONObject(i);
     		String dscmtrclcbldingid=first.get("dsc_mtrc_lc_bldg_id").toString();
     		String mtrcperiodvalid=first.get("mtrc_period_val_id").toString().trim();
     		String mtrcperiodvalvalue=first.get("mtrc_period_val_value").toString();
     		String mtrcchangeuid=first.get("UserId").toString();
     		String mtrcallowna = first.getString("bmp_na_allow_yn").toString();
     		String mtrcvalisnayn=first.getString("mtrc_period_val_is_na_yn").toString();
     		
     		float cvalue=0;
     		try
     		{
     			if (pctyn.equals("Y"))
     			{
     				cvalue=Float.parseFloat(mtrcperiodvalvalue)/100; 					 
     				mtrcperiodvalvalue=Float.toString(cvalue); 			
     			}
     		}
     		catch(Exception e)
     		{
     			// System.out.println("Save on pctyn exception:" +mtrcperiodvalvalue);
     		}
     		// System.out.println("mtrcperiodvalid vlaue is:"+mtrcperiodvalid +"...");
     		String insupd="";
     		if ((mtrcperiodvalid.equals("")) || (mtrcperiodvalid == null))
     		{
     			insupd="insert into mtrc_metric_period_value " +
     				    "([mtrc_period_id],[dsc_mtrc_lc_bldg_id],[tm_period_id] "+
     				    ",[mtrc_period_val_added_dtm],[mtrc_period_val_added_by_usr_id] "+
     				    ",[mtrc_period_val_upd_dtm],[mtrc_period_val_upd_by_user_id],[mtrc_period_val_value] " +
     				    ",[mtrc_period_val_is_na_yn])"+
     				    " values ("+mtrcperiodid +","+dscmtrclcbldingid+","+tmperiodid+",getdate(),'"+
     				     mtrcchangeuid +"','','','"+mtrcperiodvalvalue+"','" +mtrcvalisnayn+"')";  				    
     		}
     		else
     		{
     			insupd="update mtrc_metric_period_value set mtrc_period_val_value='" +mtrcperiodvalvalue +
     					"', mtrc_period_val_upd_by_user_id='"+mtrcchangeuid+"',mtrc_period_val_upd_dtm=getdate()" +
     					",[mtrc_period_val_is_na_yn]='" +mtrcvalisnayn +"'" +
     					" where mtrc_period_val_id="+mtrcperiodvalid +" and mtrc_period_id="+ mtrcperiodid +
     					" and dsc_mtrc_lc_bldg_id="+ dscmtrclcbldingid +" and tm_period_id="+tmperiodid ;
     		}
     	//	 System.out.println("INSUPD is:"+insupd);
     		try
     		{
     			Statement stmt = conn.createStatement();
     		    stmt.executeUpdate(insupd);
     		     loccount++;
     		     stmt.close();
     		}
         	 catch (SQLException e) 
          	{
          		e.printStackTrace();
                msg[0]="0";
                msg[1]= "{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""  +
             		   "Metric DB Connection Failed."  +"\"}";
                if (conn != null) { try {
					conn.close();
				} catch (SQLException e1a) {
					// TODO Auto-generated catch block
					e1a.printStackTrace();
				}} 
 	          return msg;  		 
          	 }
   
            
     	} // for each locationdetail
     	
     	 try
     	 {
     		 if (conn != null) conn.close();
     	 }
     	 catch(Exception e)
     	 {
     	 }
        msg[0]="0";
        msg[1]= "{\"result\":\"SUCCESS\",\"resultCode\":200,\"message\":\""  +
        		loccount +" Location Metric's DB Update/Insert successfull for Metric ID:"+mtrcid  +"\"}";
       return msg;
	}
}

 

