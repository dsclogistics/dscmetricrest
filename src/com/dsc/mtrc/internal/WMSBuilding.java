package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;

public class WMSBuilding {
	 
	
	public Response WMSBuilding(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		//sb=null;
		JSONObject obj1 = new JSONObject();
 
       String data="{";
       int reccount=0;
       if (inputJsonObj.has("building"))
       {
        String bldid= inputJsonObj.get("building").toString();
         System.out.println("WMS Building API invoked. Json building was:"+bldid);
       	 
			 Connection conn = null;
				try {
					conn= ConnectionManager.mtrcConn().getConnection();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	          rb=Response.ok(sb.toString()).build();
	   	          return rb;
				}
 
		 try {

   		  String SQL = "  select [dsc_mtrc_lc_bldg_id]  "+
                       " FROM [dbo].[MTRC_WMS_BLDG_XREF] where [lc_bldg_physical_id]='"+bldid +"'" +
				         " and BLDG_XREF_INVALID_YN = 'N' " +
				         " and (CAST(DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0 )as date) >= cast( bldg_xref_eff_start_dt  as date) "+
				         "  and CAST(DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0)) as DATE) <=cast(bldg_xref_eff_end_dt as date)) ";
 
	          
		    
	        
	          Statement stmt = conn.createStatement();
	        //     System.out.println("statement connect done" );
			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
			//        System.out.println("result set created" );
			   
					int numColumns = rsmd.getColumnCount(); 
					while (rs.next()) {
				     reccount++;
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);
                          obj1.put(column_name,rs.getString(i));
				        
					} // for numcolumns
					  
					} // while loop
					// if (data.length() >3) sb.append(data +"}");
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();}      
				     
				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	            rb=Response.ok(sb.toString()).build();
	   	            if (conn != null) 
	   	            {
	   	            	try{
	   	            		conn.close();
	   	            	} catch(SQLException e1)
	   	            	{e1.printStackTrace(); }
	   	            } 
	   	            return rb;
				   }
		// System.out.println("Before return json is:"+obj1.toString()); 
		     if (reccount == 0)
		     {
		    	 String msg="No matching building found for:"+bldid;
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	            rb=Response.ok(sb.toString()).build();
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
       }
       else
       {
    	   String msg="Json parameter building required for this API";
           sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
           System.out.println("sb value is:"+sb.toString());
	            rb=Response.ok(sb.toString()).build();
	            
    	   
       }
            return rb;
	}

}
