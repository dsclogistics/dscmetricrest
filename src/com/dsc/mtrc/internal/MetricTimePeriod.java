package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;

public class MetricTimePeriod {

	public Response MetricTimePeriod(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		JSONObject obj1 = new JSONObject();
        String tptname= inputJsonObj.get("tptname").toString();
        String calmonth =  inputJsonObj.get("calmonth").toString();
        String calyear =  inputJsonObj.get("calyear").toString();  
        String strtdtm ="";
        int monthnum=0;
        String data="{";
        sb=null;
     
      //  System.out.println("Received calmonth of:"+calmonth);
        try
        {
        Date date = new SimpleDateFormat("MMM").parse(calmonth);//put your month name here
         Calendar cal = Calendar.getInstance();
         cal.setTime(date);
          monthnum=cal.get(Calendar.MONTH)+1;
       //   if (monthnum == 11) monthnum++;
 
        	    NumberFormat f = new DecimalFormat("00");
        	     strtdtm = String.valueOf(f.format(monthnum));
               strtdtm = calyear + strtdtm + "01";
              // System.out.println("Start Date is :"+strtdtm);
        }
        catch(Exception e)
        {
 
        	   System.out.println("Calendar month sent:"+calmonth +" Number is not there"); 
        }
	  
       	 
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

   		  String SQL = "  select mtp.tm_period_id,mtpt.tpt_id from   [dbo].[MTRC_TIME_PERIOD_TYPE] mtpt "+
                       " join [dbo].[MTRC_TM_PERIODS] mtp on mtp.tpt_id = mtpt.tpt_id "+
  
                       
				//         " and (CAST(DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0 )as date) >= cast(mtp.tm_per_start_dtm as date) "+
				  //       "  and CAST(DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0)) as DATE) <=cast(mtp.tm_per_end_dtm as date)) "+
				                               
                       " where mtpt.[tpt_name]='"+tptname +"' and tm_per_start_dtm='"+strtdtm +"'";
	          
		   System.out.println("sql is:"+SQL);
	        
	          Statement stmt = conn.createStatement();
	        //     System.out.println("statement connect done" );
			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
			//        System.out.println("result set created" );
			       
					int numColumns = rsmd.getColumnCount(); 
					while (rs.next()) {
      
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);
                          obj1.put(column_name,rs.getString(i));
	 
				       
				        
					} // for numcolumns
				 
					} // while loop
					// if (data.length() >3) sb.append(data+"}");
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();}      
 
				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	            rb=Response.ok(sb.toString()).build();
	   	            return rb;
				   }
          //   System.out.println("Before return json is:"+obj1.toString());
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
