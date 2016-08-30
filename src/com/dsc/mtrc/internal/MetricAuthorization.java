
package com.dsc.mtrc.internal;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.mtrc.dao.*;


public class MetricAuthorization  {
	 
	
	public Response MetricAuthorization(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONObject obj1 = new JSONObject();
        JSONObject objg = new JSONObject();
        
        String username= inputJsonObj.get("username").toString();
 
        	 
			 Connection conn = null;
 				try {
					conn= ConnectionManager.mtrcConn().getConnection();
					conn.setReadOnly(true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	          rb=Response.ok(obj1.toString()).build();
	   	          return rb;
				}
  
		 try {
			 
			 // first get header level data
 
			 
	 		 
			 
 
    		  String SQL =" SELECT   [mma_id] ,mma.[mtrc_prod_id] ,[mma_dsc_ad_username] " +
    			    	   ",[mma_eff_start_date] ,[mma_eff_end_date],mp.prod_name "+
    			    	   " ,mmpr.mtrc_period_name, mmp.mtrc_period_id, mmpr.mtrc_id  "+
    			    	  "  FROM [dbo].[MTRC_MGMT_AUTH] mma, "+
    			    	  "  [dbo].[MTRC_METRIC_PRODUCTS] mmp, "+
    			    	  "	[dbo].[MTRC_PRODUCT] mp, "+
    			    	  "	[dbo].[MTRC_METRIC_PERIOD] mmpr "+
    			    	  "	where mma.mtrc_prod_id = mmp.mtrc_period_id "+ 
    			    	  "	and mp.prod_id = mmp.prod_id "+
    			    	  "	and mmpr.mtrc_period_id=mmp.mtrc_prod_id "+
    			    	  "	and mmp.mtrc_prod_top_lvl_parent_yn = 'Y' "+
    			    	  " and mma_dsc_ad_username='"+username +"'";
 
	         
	          
		    
    		 // System.out.println("sql:"+SQL );
	          Statement stmt = conn.createStatement();
	             
			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
			//        System.out.println("result set created" );
 
					int numColumns = rsmd.getColumnCount(); 
					//  System.out.println("NumColumns for locationdetails:"+numColumns);
					while (rs.next()) {
 
					JSONObject obj = new JSONObject();
 
					for (int i=1; i<numColumns+1; i++) {
				        
				        String column_name = rsmd.getColumnName(i);
				        String colvalue=rs.getString(i);
				        if (colvalue  == null ) {colvalue="";}
				          obj.put(column_name, colvalue);
     
					} // for numcolumns
					 json.put(obj);
					} // while loop
 
  						obj1.put("authorizationdetails",json);
		 
			              rs.close();		     
			             stmt.close();			             
			             if (conn != null) { conn.close();} 
      
				  }
				   catch (SQLException e) {
					   
					// TODO Auto-generated catch block
					e.printStackTrace();
					try {
						 if (conn != null) { conn.close();} 
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
	                String msg="Metric DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	            rb=Response.ok(obj1.toString()).build();
	   	            return rb;
				   }
 
	         rb=Response.ok(obj1.toString()).build();
	         if (conn != null) 
	         {
	      	   try{
	      		   conn.close();
	      		  } catch(SQLException e)
	      	      {e.printStackTrace(); }
	         } 
             return rb;
	}
}

 

