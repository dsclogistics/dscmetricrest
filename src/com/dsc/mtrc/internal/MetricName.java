   
package com.dsc.mtrc.internal;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.mtrc.dao.*;


public class MetricName  {
	 
	
	public Response MetricName(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONObject obj1 = new JSONObject();
        
        // Get tptname 
     	 
     	// if (s1.get("tpt_name").toString().equals("COLLECTED")) 
       	 
         String tptname= inputJsonObj.get("tptname").toString();
         String productname=inputJsonObj.get("productname").toString();
         String mtrcname="";
         if(    inputJsonObj.has("metricname")) mtrcname=inputJsonObj.get("metricname").toString();
         
         JSONObject jo = new JSONObject();
         jo.put("tptname", tptname);
         jo.put("productname", productname);
        	 
			 Connection conn = null;
 				try {
					conn= ConnectionManager.mtrcConn().getConnection();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	          rb=Response.ok(sb.toString()).build();
	   	          return rb;
				}
  
		 try {
 
    		  String SQL = " select mm.mtrc_name,mm.mtrc_id ,mmper.mtrc_period_id ,mmper.mtrc_period_na_allow_yn,mmp.mtrc_prod_display_text,mmp.mtrc_prod_display_order from mtrc_product mp  "+
                           " join MTRC_METRIC_PRODUCTS mmp on  mp.prod_id = mmp.prod_id  "+
    				  
				         
				         " and (CAST(DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0 )as date) >= cast(mmp.mtrc_prod_eff_start_dt  as date) "+
				         "  and CAST(DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0)) as DATE) <=cast(mmp.mtrc_prod_eff_end_dt as date)) "+

                           
                           " join MTRC_METRIC_PERIOD  mmper on  mmp.mtrc_prod_id = mmper.Mtrc_period_id " +
                           " join MTRC_METRIC mm on  mmper.mtrc_id = mm.mtrc_id " +
                           " join  MTRC_TIME_PERIOD_TYPE pt on  mmper.tpt_id = pt.tpt_id "+
                         //  " where mp.prod_name ='Red Zone'    and pt.tpt_name = 'Month' ";
                          " where mp.prod_name ='"+productname +"'    and pt.tpt_name = '"+tptname +"' "+
                         " and mmp.mtrc_prod_top_lvl_parent_yn='Y'" +
                          
  		  	  " and (CAST(DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0 )as date) >= cast( mm.mtrc_eff_start_dt  as date) "+ 
		  	  " and CAST(DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0)) as DATE) <=cast(mm.mtrc_eff_end_dt as date)) ";
 
    		  if (mtrcname.length() > 3) 
    		            	 SQL=SQL + " and mm.mtrc_name='"+mtrcname +"'";
	         
	          SQL=SQL+" order by mtrc_prod_display_order";
    		//  System.out.println("MetricName SQL:"+SQL ); 
	        
	          Statement stmt = conn.createStatement();
	        //     System.out.println("statement connect done" );
			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
			//        System.out.println("result set created" );
			       
					int numColumns = rsmd.getColumnCount(); 
					while (rs.next()) {

					JSONObject obj = new JSONObject();
 
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);

				          obj.put(column_name, rs.getString(i));
				       
				        
					} // for numcolumns
					 json.put(obj);
					} // while loop
	 
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();} 
					 obj1.put("metricdetail", jo);     
				     obj1.put("metriclist",(Object)json);      

				  }
				   catch (SQLException e) {
				        if (conn != null) 
				         {
				      	   try{
				      		   conn.close();
				      		  } catch(SQLException e1)
				      	      {e1.printStackTrace(); }
				         } 
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
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
             return rb;
	}
}

 


