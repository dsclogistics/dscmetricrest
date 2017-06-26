package com.dsc.mtrc.internal;
 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
 
import com.dsc.mtrc.dao.*;
import com.dsc.mtrc.util.DateHelper;
import com.dsc.mtrc.util.StringsHelper;
import com.dsc.mtrc.util.MetricPeriodHelper;


public class MetricPeriodClose {

 
	 
	
	public Response MetricPeriodClose(JSONObject inputJsonObj) throws JSONException {
	     int tptid =0;
	     int prodId = 0;//prodId is the value that represents the RED ZONE product db number. 
	     int tmPeriodId =0;
	     String mpgLessVal = null;
	     String mpgLessEqVal = null;
	     String mpgEqualVal = null;
	     String mpgGreaterVal = null;
	     String mpgGreaterEqVal = null;
	     double mpValue = 0.0;// this value represents a single value from MTRC_METRIC_PERIOD_VALUE table
	     String goalMet = "N"; // valid options are Y/N/X
	     MetricPeriodHelper mph = new MetricPeriodHelper();
	     List<Integer> bldgs = new ArrayList<Integer>();
	     String mpgAllowBldgOverride = null; //this variable will be used later to implement building override goals
	     String startDate;
	     String endDate; 
		 Response rb = null;
		 int loccount=0;
		 		 
		 
		 StringBuffer sb = new StringBuffer();
		    String msg="";
  		 Connection conn = null;
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
	        String tptname= inputJsonObj.get("tptname").toString();
	        String prodname=inputJsonObj.get("productname").toString();
	        String calmonth=inputJsonObj.get("calmonth").toString();
	        String calyear=inputJsonObj.get("calyear").toString();
	        String mtrcid=inputJsonObj.get("mtrcid").toString();
	        String updusr=inputJsonObj.get("user_id").toString();
	        String mtrpid=inputJsonObj.get("mtrc_period_id").toString();
	        startDate = DateHelper.getMonthFirstDay(calmonth, calyear);
	        endDate = DateHelper.getMonthLastDay(calmonth, calyear);
	        
	        
	        
	        //First lets get the product id from product name passed from the user
	        
	        String SQL="select prod_id as prodID  from mtrc_product where prod_name='"+prodname +"'";

		      try
		      	{
		      	
		      		Statement stmt = conn.createStatement();
		      		ResultSet rs = stmt.executeQuery(SQL);
		      		ResultSetMetaData rsmd = rs.getMetaData();
		      		int numColumns = rsmd.getColumnCount(); 
		      		while (rs.next()) 
		      		{     
		      			prodId=rs.getInt("prodID");
		      		}
		      		rs.close();
		      		
		         	stmt.close();	
		      		 
		      		 
		      	}  
		     	 catch (SQLException e) 
		      	{
		      		e.printStackTrace();
		            msg="Metric DB Connection Failed.";
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
	   
	    // Get tpt_id from mtrc_time_period_type using tptname 
	        
	     SQL="select tpt_id as tptid from mtrc_time_period_type where tpt_name='"+tptname +"'";

	      try
	      	{
	      	
	      		Statement stmt = conn.createStatement();
	      		ResultSet rs = stmt.executeQuery(SQL);
	      		ResultSetMetaData rsmd = rs.getMetaData();
	      		int numColumns = rsmd.getColumnCount(); 
	      		while (rs.next()) 
	      		{     
	      			tptid=rs.getInt("tptid");
	      		}
	      		rs.close();
	      		
	         	stmt.close();	
	      		 
	      		 
	      	}  
	     	 catch (SQLException e) 
	      	{
	      		e.printStackTrace();
	            msg="Metric DB Connection Failed.";
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
	      SQL=" select tm_period_id" +
	      	  " from mtrc_tm_periods"+
	      	  " where tpt_id='"+tptid +"'"+
	      	  " and CONVERT(VARCHAR(10),tm_per_start_dtm,20) = '"+startDate +"'"+
	      	  " and CONVERT(VARCHAR(10),tm_per_end_dtm,20)= '"+endDate +"'";

	      try
	      	{
	      	
	      		Statement stmt = conn.createStatement();
	      		ResultSet rs = stmt.executeQuery(SQL);
	      		ResultSetMetaData rsmd = rs.getMetaData();
	      		int numColumns = rsmd.getColumnCount(); 
	      		while (rs.next()) 
	      		{     
	      			tmPeriodId=rs.getInt("tm_period_id");
	      		}
	      		rs.close();
	      		
	         	stmt.close();	
	      		 
	      		 
	      	}  
	     	 catch (SQLException e) 
	      	{
	      		e.printStackTrace();
	            msg="Metric DB Connection Failed.";
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
	      System.out.println("TM-Period_ID = "+tmPeriodId);
      	// Verify if any null values:
	       SQL= " select count(*) as rowcounts from mtrc_metric_period_value where " +
      	             " cast(mtrc_period_id as varchar(10)) +'X'+ cast(tm_period_id  as varchar(10)) in "+
	                 " (select cast(a.mtrc_period_id as varchar(10)) +'X'+ cast(b.tm_period_id as varchar(10)) from mtrc_metric_period a, mtrc_tm_periods b where "+
	                 " a.mtrc_id="+mtrcid +"  and a.tpt_id =( select distinct tpt_id from MTRC_TIME_PERIOD_TYPE "+
	                 " where tpt_name='"+tptname +"')  and (Datename(month,b.tm_per_start_dtm) ='"+calmonth +"'  and Year(b.tm_per_start_dtm) ="+calyear  +
	                 " ) and (Datename(month,b.tm_per_end_dtm) ='"+calmonth +"' and Year(b.tm_per_start_dtm) ="+calyear+" ))  "+
	                 " and (mtrc_period_val_is_na_yn ='N' and (MTRC_PERIOD_val_VALUE is null or MTRC_PERIOD_val_VALUE =''))  UNION ALL "+
	                 " select count(*) as rowcounts from mtrc_metric_period_value where " +
      	             " cast(mtrc_period_id as varchar(10)) +'X'+ cast(tm_period_id as varchar(10))  in "+
	                 " (select cast(a.mtrc_period_id as varchar(10)) +'X'+ cast(b.tm_period_id as varchar(10)) from mtrc_metric_period a, mtrc_tm_periods b where "+
	                 " a.mtrc_id="+mtrcid +"  and a.tpt_id =( select distinct tpt_id from MTRC_TIME_PERIOD_TYPE "+
	                 " where tpt_name='"+tptname +"')  and (Datename(month,b.tm_per_start_dtm) ='"+calmonth +"'  and Year(b.tm_per_start_dtm) ="+calyear  +
	                 " ) and (Datename(month,b.tm_per_end_dtm) ='"+calmonth +"' and Year(b.tm_per_start_dtm) ="+calyear+" ))  "+
	                " and ([mtrc_period_val_is_na_yn] = 'Y' and (MTRC_PERIOD_val_VALUE = 'N/A'  or MTRC_PERIOD_val_VALUE ='' or MTRC_PERIOD_val_VALUE != null))";
	
	  //System.out.println("Sql is:"+SQL);
	       int recount=0;  
	       
	       int bldcount=0;
 
		   SQL= "select count(*) as rowcounts FROM [dbo].[DSC_MTRC_LC_BLDG] a " +
				 "  left join [dbo].[MTRC_BLDG_MTRC_PERIOD] b on a.[dsc_mtrc_lc_bldg_id] = b.[dsc_mtrc_lc_bldg_id] "+
				"  where getdate() between [dsc_mtrc_lc_bldg_eff_start_dt] and [dsc_mtrc_lc_bldg_eff_end_dt] " +
		        " and b.mtrc_period_id=" +mtrpid;
		     System.out.println("sql to update is:"+SQL);
	     	try
	     	{
	     	
	     		Statement stmt = conn.createStatement();
	     		ResultSet rs = stmt.executeQuery(SQL);
	     		ResultSetMetaData rsmd = rs.getMetaData();
	     		int numColumns = rsmd.getColumnCount(); 
	     		while (rs.next()) 
	     		{     
	     			 
	     			bldcount=rs.getInt("rowcounts");
	     		}
	     		rs.close();
	     		
	        	stmt.close();	
	     		 
	     		 
	     	}
	     	 catch (SQLException e) 
	     	{
	     		e.printStackTrace();
	           msg="Metric DB Connection Failed.";
	           sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
		            rb=Response.ok(sb.toString()).build();
		            if (conn != null) { try {
						conn.close();
					} catch (SQLException e1a) {
						// TODO Auto-generated catch block
						e1a.printStackTrace();
					}} 
		            return rb;
		 
	     	 }
	 
 	       

	   // verify if all building are in period value you are trying to close
	       
	   SQL= "select count(*) as rowcounts from MTRC_METRIC_PERIOD_VALUE where  mtrc_period_id="+mtrpid +
			 " and tm_period_id="+tmPeriodId;
       System.out.println("sql to update is:"+SQL);
     	try
     	{
     	
     		Statement stmt = conn.createStatement();
     		ResultSet rs = stmt.executeQuery(SQL);
     		ResultSetMetaData rsmd = rs.getMetaData();
     		int numColumns = rsmd.getColumnCount(); 
     		while (rs.next()) 
     		{     
     			 
               recount=rs.getInt("rowcounts");
     		}
     		rs.close();
     		
        	stmt.close();	
     		 
     		 
     	}
     	 catch (SQLException e) 
     	{
     		e.printStackTrace();
           msg="Metric DB Connection Failed.";
           sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            if (conn != null) { try {
					conn.close();
				} catch (SQLException e1b) {
					// TODO Auto-generated catch block
					e1b.printStackTrace();
				}} 
	            return rb;
	 
     	 }
 
	 //  if (recount ==0 || recount < bldcount )
    	 if (recount ==0 )
	   {
           msg="Not all buildings were found for Metric Period:"+mtrpid +" and time period:"+tptid +
        		   ". Building count should be:" +bldcount +", found:"+recount +" in metric value";
           sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            if (conn != null) { try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}} 
	            return rb;
	   }
	   

	   // continue checking rest
	   SQL="select count(*) as rowcounts from MTRC_METRIC_PERIOD_VALUE a " +
	       " left join MTRC_BLDG_MTRC_PERIOD b on a.mtrc_period_id = b.mtrc_period_id "+
	       " and a.dsc_mtrc_lc_bldg_id = b.dsc_mtrc_lc_bldg_id and a.mtrc_period_id = b.mtrc_period_id" +
	       " where a.mtrc_period_id="+mtrpid +" and tm_period_id="+tmPeriodId + 
	       "  and (a.mtrc_period_val_value is null or a.mtrc_period_val_value = '') union all "+
	       "select count(*) as rowcounts from MTRC_METRIC_PERIOD_VALUE a " +
	       " left join MTRC_BLDG_MTRC_PERIOD b on a.mtrc_period_id = b.mtrc_period_id "+
	       " and a.dsc_mtrc_lc_bldg_id = b.dsc_mtrc_lc_bldg_id and a.mtrc_period_id = b.mtrc_period_id" +
	       " where a.mtrc_period_id="+mtrpid +" and tm_period_id="+tmPeriodId + 
	       "and b.bmp_na_allow_yn = 'N' and (a.mtrc_period_val_value = 'N/A')";     
 
      	int nullvalue=0;
        int navalue=0;
        recount=0;
      	try
      	{
      	
      		Statement stmt = conn.createStatement();
      		ResultSet rs = stmt.executeQuery(SQL);
      		ResultSetMetaData rsmd = rs.getMetaData();
      		int numColumns = rsmd.getColumnCount(); 
      		while (rs.next()) 
      		{     
      			if (recount==0)
      			{
      				navalue =rs.getInt("rowcounts");
      			}
      			else
      			{
      				nullvalue=rs.getInt("rowcounts");
      			}
                recount++;
      		}
      		rs.close();
      		
         	stmt.close();	
      		 
      		 
      	}
      	 catch (SQLException e) 
      	{
      		e.printStackTrace();
            msg="Metric DB Connection Failed.";
            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            if (conn != null) { try {
					conn.close();
				} catch (SQLException e1c) {
					// TODO Auto-generated catch block
					e1c.printStackTrace();
				}} 
	            return rb;
 	 
      	 }
  
 
     		
      	if (nullvalue == 0 && navalue == 0)
      	{
      		
//Before closing period we need to update all the goal values.
 
      		//First, lets get the goal values
      	   SQL = " select mpg_less_val,"+
      		     " mpg_less_eq_val,"+
      			 " mpg_equal_val,"+
      			 " mpg_greater_val,"+
      		     " mpg_greater_eq_val, "+
      			 " mpg_allow_bldg_override"+
      		     " from mtrc_mpg where mtrc_period_id ="+mtrpid +
      		     " and prod_id ="+prodId+
      			 " and mpg_start_eff_dtm <='"+startDate +"'"+
      		     " and mpg_end_eff_dtm >='"+endDate +"'";
      		try
          	{
          	
          		Statement stmt = conn.createStatement();
          		ResultSet rs = stmt.executeQuery(SQL);
          		ResultSetMetaData rsmd = rs.getMetaData();
          		while (rs.next()) 
          		{     
          			mpgLessVal = rs.getString("mpg_less_val");
          			mpgLessEqVal = rs.getString("mpg_less_eq_val"); 
          			mpgEqualVal = rs.getString("mpg_equal_val");
          		    mpgGreaterVal = rs.getString("mpg_greater_val");;
          			mpgGreaterEqVal =rs.getString("mpg_greater_eq_val");
          			mpgAllowBldgOverride = rs.getString("mpg_allow_bldg_override");          			
          		}
          		rs.close();          		
             	stmt.close();         		 
          	}
          	 catch (SQLException e) 
          	{
          		e.printStackTrace();
                msg="Metric DB Connection Failed.";
                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
    	            rb=Response.ok(sb.toString()).build();  	            
    	            if (conn != null) { try {
    					conn.close();
    				} catch (SQLException e1c) {
    					// TODO Auto-generated catch block
    					e1c.printStackTrace();
    				}} 
    	            return rb;
     	 
          	 }
            //Now, lets get the values for the period/metric that we're trying to close
      		String SQL1="select "+
      				    " a.mtrc_period_val_value,"+
      		            " a.mtrc_period_val_id, "+
      				    " a.dsc_mtrc_lc_bldg_id"+
      		            " from MTRC_METRIC_PERIOD_VALUE a " +
      		            " left join MTRC_BLDG_MTRC_PERIOD b" +
      		            " on a.mtrc_period_id = b.mtrc_period_id "+
      		            " and a.dsc_mtrc_lc_bldg_id = b.dsc_mtrc_lc_bldg_id"+
      		            " and a.mtrc_period_id = b.mtrc_period_id" +
      		            " where a.mtrc_period_id="+mtrpid +
      		            " and tm_period_id="+tmPeriodId;
      		System.out.println("SQL to get period values: "+SQL1);
      		try
          	{
          	
      			conn.setAutoCommit(false);
      			String insertSQL = "insert into rz_mtrc_period_val_goal "+
      					           "(mtrc_period_val_id, rz_mpvg_goal_met_yn, rz_mpvg_created_dtm)"+
      					           " values(?,?,?)";
      			SQL ="update [dbo].[RZ_MTRC_PERIOD_STATUS] set [rz_mps_status]='Closed',"+
      	      		     " [rz_mps_closed_on_dtm]=getdate() " + ",rz_mps_closed_by_usr_id='"+updusr +"' " +
      	      		     " where [mtrc_period_id] ="+mtrpid +" and  [tm_period_id]= "+
      	      		     " (select distinct tm_period_id from MTRC_tm_periods "+
      		                 " where (Datename(month,tm_per_start_dtm) ='"+calmonth +"'  and Year(tm_per_start_dtm) ="+calyear  +
      		                 " ) and (Datename(month,tm_per_end_dtm) ='"+calmonth +"' and Year(tm_per_end_dtm) ="+calyear+" ))";
      			
      			String actionPlanSQL = "insert into RZ_BLDG_ACTION_PLAN"
      					              +"(dsc_mtrc_lc_bldg_id,tm_period_id,rz_bap_created_on_dtm)"
      					              +"values (?,?,?)";
      			//query to check if builging has override goals for this period
      			String overrideGoalSQL ="select mpbg_less_val,mpbg_less_eq_val,mpbg_equal_val,mpbg_greater_val,mpbg_greater_eq_val,mpbg_display_text"
      					+ " from MTRC_MPBG"
      					+ " where dsc_mtrc_lc_bldg_id = ?"
                        + " and mpbg_start_eff_dtm<= ?"
                        + " and mpbg_end_eff_dtm>=?"
                        + " and mtrc_period_id = ?"
                        + " and prod_id = ?";

      			PreparedStatement actionPlanPstmt = conn.prepareStatement(actionPlanSQL,PreparedStatement.RETURN_GENERATED_KEYS);
      			PreparedStatement bapMetricPrepStmt = null;
      			PreparedStatement valueIdPrepStmt = null;
      			PreparedStatement getBuildingsPrepStmt = null;
          		Statement stmt = conn.createStatement();
          		PreparedStatement pstmt = conn.prepareStatement(insertSQL);
          		PreparedStatement overrideGoalPs = conn.prepareStatement(overrideGoalSQL);
          		ResultSet rs = stmt.executeQuery(SQL1);
          		
          		ResultSetMetaData rsmd = rs.getMetaData();
          		while (rs.next()) 
          		{   
          			pstmt.setInt(1, rs.getInt("mtrc_period_val_id"));
          			if(mpgAllowBldgOverride.equals("Y"))//if enterprise goal can be overwritten we need to check for building specific goals 
          			{
          				overrideGoalPs.setInt(1,rs.getInt("dsc_mtrc_lc_bldg_id"));
          				overrideGoalPs.setString(2, startDate);
          				overrideGoalPs.setString(3, endDate);
          				overrideGoalPs.setString(4, mtrpid);
          				overrideGoalPs.setInt(5, prodId);
          				ResultSet overrideGoalRs = overrideGoalPs.executeQuery();          				   				
          				if(overrideGoalRs.next())//if query returned any data
          				{
          					if(overrideGoalRs.getString("mpbg_less_val")!=null||
          					   overrideGoalRs.getString("mpbg_less_eq_val")!=null||
          					   overrideGoalRs.getString("mpbg_equal_val")!=null||
          					   overrideGoalRs.getString("mpbg_greater_val")!=null||
          					   overrideGoalRs.getString("mpbg_greater_eq_val")!=null){
          						//if we're here, that means building override goal exists and we need to use override goal values
          						pstmt.setString(2, StringsHelper.isGoalMet(rs.getString("mtrc_period_val_value"),overrideGoalRs.getString("mpbg_less_val")
          								        ,overrideGoalRs.getString("mpbg_less_eq_val"),overrideGoalRs.getString("mpbg_equal_val"),
          								        overrideGoalRs.getString("mpbg_greater_val"),overrideGoalRs.getString("mpbg_greater_eq_val")));
          					}//end if
          					else//use enterprise goals
          					{
          						pstmt.setString(2, StringsHelper.isGoalMet(rs.getString("mtrc_period_val_value"),mpgLessVal,mpgLessEqVal,mpgEqualVal,mpgGreaterVal,mpgGreaterEqVal));
          					}
          				}
          				else
          				{
          					pstmt.setString(2, StringsHelper.isGoalMet(rs.getString("mtrc_period_val_value"),mpgLessVal,mpgLessEqVal,mpgEqualVal,mpgGreaterVal,mpgGreaterEqVal));
          				}
          				          			
          			}//end of if(mpgAllowBldgOverride.equals("Y"))
          			else{//use enterprise goal          				
          				pstmt.setString(2, StringsHelper.isGoalMet(rs.getString("mtrc_period_val_value"),mpgLessVal,mpgLessEqVal,mpgEqualVal,mpgGreaterVal,mpgGreaterEqVal));
          			}
          			
			        pstmt.setTimestamp(3, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())); //java.sql.Date.valueOf(java.time.LocalDate.now()) is only available in java 8 
			        pstmt.addBatch();
          		}//end of while
          		pstmt.executeBatch();
          		stmt.executeUpdate(SQL);
/****************************Now we need to check if action plan is required *******************************************/          
          		System.out.println("Checking how many periods are closed");
          		int closedMtrcQty = mph.getClosedMetricCount(conn, tmPeriodId);
          		System.out.println("Closed "+closedMtrcQty);
          		if(closedMtrcQty>=3)
          		{
          		
          			System.out.println("Closed more than 2 metrics");
          			String getBuildingsSQL = " select v.dsc_mtrc_lc_bldg_id," 
                                            +" count(*) as  cnt,"
                                            + "coalesce(a.rz_bap_id,-1) as bap_id"
                                            +" from mtrc_metric_period_value v"  
                                            +" join rz_mtrc_period_status s"
       	                                    +" on v.tm_period_id = s.tm_period_id"
                                            +" and v.mtrc_period_id = s.mtrc_period_id"
       	                                    +" join  RZ_MTRC_PERIOD_VAL_GOAL g"
       	                                    +" on v.mtrc_period_val_id = g.mtrc_period_val_id"
       	                                    +" left outer join RZ_BLDG_ACTION_PLAN a "
       	                                    +" on v.dsc_mtrc_lc_bldg_id = a.dsc_mtrc_lc_bldg_id"
       	                                    +" and v.tm_period_id = a.tm_period_id"
                                            +" where s.tm_period_id =? "
                                            +" and s.rz_mps_status = 'Closed'"
                                            +" and g.rz_mpvg_goal_met_yn='N'"                                         
                                            +" group by "
                                            +" v.dsc_mtrc_lc_bldg_id,"
                                            +" a.rz_bap_id"
                                            +" having count(*)>=3"
                                            +" order by v.dsc_mtrc_lc_bldg_id"; 
          			
          			getBuildingsPrepStmt = conn.prepareStatement(getBuildingsSQL);
          			
          			String getValIdSQL = "select v.mtrc_period_val_id "
          					            + " from MTRC_METRIC_PERIOD_VALUE v"
          					            + " join RZ_MTRC_PERIOD_VAL_GOAL g "
          					            + " on v.mtrc_period_val_id = g.mtrc_period_val_id"
          					            + " where dsc_mtrc_lc_bldg_id = ?"
          					            + " and tm_period_id = ?"
          					            + " and g.rz_mpvg_goal_met_yn='N' "
          					            + " and not exists"
          					            + " (select 1 from RZ_BAP_METRICS where v.mtrc_period_val_id = RZ_BAP_METRICS.mtrc_period_val_id )";
          			
          			String bapMetricSQL = "insert into rz_bap_metrics "
				                 +"(rz_bap_id,mtrc_period_val_id,rz_bapm_status,rz_bapm_created_on_dtm)"
				                 + "values(?,?,?,?) ";     
          			
          			valueIdPrepStmt = conn.prepareStatement(getValIdSQL);
          			bapMetricPrepStmt = conn.prepareStatement(bapMetricSQL);
          			
          			getBuildingsPrepStmt.setInt(1,tmPeriodId);
          			rs = getBuildingsPrepStmt.executeQuery();
          			while(rs.next())
          			{
          				
          				int bldgId = rs.getInt("dsc_mtrc_lc_bldg_id");
          				System.out.println("Action Plan needed for building id "+bldgId);
          				int bldgCount = rs.getInt("cnt");
          				int bapId = rs.getInt("bap_id");
          				ResultSet tempRes = null;
          				if(bapId ==-1)//-1 means this is a new rz_bldg_action_plan record. That means we need to get rz_bap_id value first
          				{
          					System.out.println("Adding new AP record for building id "+bldgId);
          					actionPlanPstmt.setInt(1, bldgId);
          					actionPlanPstmt.setInt(2, tmPeriodId);
          					actionPlanPstmt.setTimestamp(3, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
          					actionPlanPstmt.executeUpdate();
          					tempRes = actionPlanPstmt.getGeneratedKeys();          					
          					tempRes.next();
          					bapId = tempRes.getInt(1);
          				}
          				
          				valueIdPrepStmt.setInt(1,bldgId );
          				valueIdPrepStmt.setInt(2, tmPeriodId);         				
          				tempRes = valueIdPrepStmt.executeQuery();
          				while(tempRes.next())
          				{
          					System.out.println("Adding rz_bap_record for mtrc_period_val_id =  "+tempRes.getInt("mtrc_period_val_id"));
          					bapMetricPrepStmt.setInt(1, bapId);
          					bapMetricPrepStmt.setInt(2, tempRes.getInt("mtrc_period_val_id"));
          					bapMetricPrepStmt.setString(3, "Not Started");
          					bapMetricPrepStmt.setTimestamp(4,java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
          					bapMetricPrepStmt.executeUpdate();
          				}//end of tempRes while
          				
          				
          			}//end of while
          			//bldgs = mph.getBuildingsForActionPlan(conn, tmPeriodId);       			          			          			
          		}//if(mph.getClosedMetricCount(conn, tmPeriodId)>=3)         		
    
          		rs.close();  
          		if(actionPlanPstmt!=null)
          		{
          			actionPlanPstmt.close();
          		}
          		if(bapMetricPrepStmt!=null)
          		{
          			bapMetricPrepStmt.close();
          		}
          		if(valueIdPrepStmt!=null)
          		{
          			valueIdPrepStmt.close();
          		}
          		System.out.println("Commiting transaction...");
          		conn.commit();
          		System.out.println("Done Committing...");         		
          		pstmt.close();         		 		
             	stmt.close();
             	System.out.println("Closing Connection...");
             	conn.close();
             	System.out.println("Connection is closed");
          	}//end of try
      		catch(Exception e)
      		{
      			e.printStackTrace();
          		try {
					conn.rollback();
					conn.close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
          		msg="Metric DB Connection Failed.";
                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
    	        rb=Response.ok(sb.toString()).build();
    	            if (conn != null) { try {
    					conn.close();
    				} catch (SQLException e1c) {
    					// TODO Auto-generated catch block
    					e1c.printStackTrace();
    				}} 
    	            return rb;
      		}
  	
      	}
      	else
      	{
            msg="Unanswered question count:"+nullvalue +" and Unanswered N/A count:"+navalue  ;
            sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	         rb=Response.ok(sb.toString()).build();
	         if (conn != null) { try {
				conn.close();
			} catch (SQLException ee) {
				ee.printStackTrace();
			}} 
	            return rb;
      	}

     	 try
     	 {
     		 if (conn != null) conn.close();
     	 }
     	 catch(Exception e)
     	 {
     		 
     	 }
         msg= "Successfully closed MetricID:"+mtrcid +" for year:"+calyear +" and  Month of:"+calmonth ;
         sb.append("{\"result\":\"SUCCESS\",\"resultCode\":100,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
	            return rb;     	 
 
	}


}
