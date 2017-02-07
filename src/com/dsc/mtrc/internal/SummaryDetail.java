package com.dsc.mtrc.internal;
 
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.mtrc.dao.*;


public class SummaryDetail {
	 
	
	public Response SummaryDetail(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONArray json1 = new JSONArray();
        JSONArray json2 = new JSONArray();
        JSONObject obj1 = new JSONObject();
        JSONObject objg = new JSONObject();
         DecimalFormat df2 = new DecimalFormat("0.00");
         df2.setRoundingMode(RoundingMode.UP);


        String bldid= "";
        String mtrcid="";
        String calmonth="January";
        
        if (inputJsonObj.has("dsc_mtrc_lc_bldg_id")) bldid=inputJsonObj.get("dsc_mtrc_lc_bldg_id").toString();
        if (inputJsonObj.has("mtrc_id")) mtrcid=inputJsonObj.get("mtrc_id").toString();
        
        String tptname= inputJsonObj.get("tptname").toString();
        String prodname=inputJsonObj.get("productname").toString();
        if (inputJsonObj.has("calmonth")) calmonth=inputJsonObj.get("calmonth").toString();
        String calyear=inputJsonObj.get("calyear").toString();
//        String mtrcid=inputJsonObj.get("mtrcid").toString();
        String master="N";
        String detail="N";
        String pctyn="N";
        String intyn="N";
        String mm="";
        String t="";
        String fullyear="N";
        String mtrcpassyn="N";
		 SimpleDateFormat inputFormat = new SimpleDateFormat("MMMM");
		  Calendar cal = Calendar.getInstance();		 
		  
	        if  ( (calmonth.equals(null)) || (calmonth.trim().isEmpty()) )
		        {
	        	 calmonth="January";
		        	// System.out.println(" calmonth is blank");
		        	if (((mtrcid.equals(null)) || (mtrcid.trim().isEmpty())) &&
		        	     ((!bldid.equals(null)) && (!bldid.trim().isEmpty())))
		        	{
		        		//System.out.println(" First condition  ");
		        		fullyear="Y";
		        		
		        	} else if
	        	               (((!mtrcid.equals(null)) || (!mtrcid.trim().isEmpty())) &&
			        	          ((bldid.equals(null)) || (bldid.trim().isEmpty())))
			        	     {
		        					//System.out.println(" Second Condition");
		        					fullyear="Y";
			        		
			        	     } else if 
	        	               (((!mtrcid.equals(null)) || (!mtrcid.trim().isEmpty())) &&
			        	          ((!bldid.equals(null)) || (!bldid.trim().isEmpty())))
			        	     {
		        					//System.out.println(" Second Condition");
		        					fullyear="Y";
			        		
			        	     }
			        	    	
		        	 
		        }	  
     //   if  (calmonth.equals(null) || calmonth.trim().isEmpty()) calmonth="January"; 			  
		  
		 try {
			cal.setTime(inputFormat.parse(calmonth));
			  SimpleDateFormat outputFormat = new SimpleDateFormat("MM") ;// 01-12
			    mm=outputFormat.format(cal.getTime());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}      	 
			 Connection conn = null;
 				try {
					conn= ConnectionManager.mtrcConn().getConnection();
					conn.setReadOnly(true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	                rb=Response.ok(sb.toString()).build();
	   	          return rb;
				}
 				
 				 String sdate="" ;
 				 String edate="" ;

 				 			 
 				   sdate=calyear +"-"+mm +"-01" ;
 				   edate=calyear +"-"+mm +"-28" ;	
 				   
 			//	   System.out.println(" Sdate is:"+sdate +" End date of mm is:"+mm);
 
		 try {
			 
			// Get Mtrc Period ID. pass product name 
			 
			 String  mpids=get_mtrcperiodid(prodname,conn);
			 if (mpids.length() <=0 )
			 {
	                String msg="Product Name:"+prodname +"  not found in the DB.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	                rb=Response.ok(sb.toString()).build();
	                if (conn != null) { try {
	    				conn.close();
	    			} catch (SQLException e1a) {
	    				// TODO Auto-generated catch block
	    				// e1.printStackTrace();
	    			}} 
	   	          return rb;
			 }
			 
				 // get tpt_id and tm_period_id now pass start and end dates.

				 String  pid=get_periodid(sdate,edate,conn);
				 String [] xptid=pid.split("|");				
			 if (xptid.length <=1)
 
			 {
	                String msg="No Start/End Dates for this period year:"+calyear +" Month:"+ calmonth +" found in the DB.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	                rb=Response.ok(sb.toString()).build();
	                if (conn != null) { try {
	    				conn.close();
	    			} catch (SQLException e1b) {
	    				// TODO Auto-generated catch block
	    				// e1.printStackTrace();
	    			}} 
	   	          return rb;
			 }	
		 String tptid=xptid[0];
		 String tmpid=xptid[1];			 
		 // if  (!bldid.equals(null) && !bldid.trim().isEmpty())
		  if (fullyear.equals("Y"))
           {
   				   sdate=calyear +"-01-01" ;
    			   edate=calyear +"-12-28" ;
    			   tptid=get_periodids(sdate,edate,conn);
  		   }
			 //  first get a list off metrics for a given time period:
 
			 String SQL1="SELECT  distinct  convert(varchar(10),b.mtrc_period_id) +'-'+ d.[mtrc_name] as MtrcName "+
			             " from [dbo].[MTRC_METRIC_PERIOD_VALUE] a "+
			             "   join mtrc_tm_periods e on  (('"+sdate +"' >= e.tm_per_start_dtm) and ('" +edate +"' <=e.tm_per_end_dtm ))" +
			             "   and a. tm_period_id=  e.tm_period_id  "+					 
			             " left join MTRC_METRIC_PERIOD b on a.mtrc_period_id = b.mtrc_period_id and  b.tpt_id=e.tpt_id "+
			             " left join dsc_mtrc_lc_bldg c on c.dsc_mtrc_lc_bldg_id = a.dsc_mtrc_lc_bldg_id "+
			             " left join mtrc_metric d on d.mtrc_id=b.mtrc_id ";

			 SQL1="select a.mtrc_id,mtrc_name from mtrc_metric a " +
				  " left join MTRC_METRIC_PERIOD b on a.mtrc_id = b.mtrc_id and  b.tpt_id in "+
		             "  (select tpt_id  from mtrc_tm_periods e where  (('"+sdate +"' >= e.tm_per_start_dtm) and "+
				     " ('" +edate +"' <=e.tm_per_end_dtm )))" +
			        " left join rz_mtrc_period_status c on mtrc_period_id = b.mtrc_peirod_id and c.tm_period_id in " +
		             "  (select tpt_id  from mtrc_tm_periods e where  (('"+sdate +"' >= e.tm_per_start_dtm) and "+
				     " ('" +edate +"' <=e.tm_per_end_dtm )))" +
			         " left join mtrc_metric_products d on d.mtrc_period_id=b.mtrc_period_id and prod_id in " +
		             "  (select prod_id  from mtrc_product where prod_name ='Red Zone')";
		       String s="";
		       if  (!mtrcid.equals(null) && !mtrcid.trim().isEmpty())
		       s ="  and mm.mtrc_id="+mtrcid ; 
				     
			 SQL1="select a.mtrc_id,mtrc_name from mtrc_metric a where  a.mtrc_id in (" +
			      " Select mtrc_id from mtrc_metric_period where mtrc_period_id in ("+mpids +") " +s + " and tpt_id in ("+tptid +"))" ;

	          SQL1="select mm.mtrc_id,mm.mtrc_name,mmprd.mtrc_prod_display_text,mmprd.mtrc_prod_display_order," +
	               "  mmpg.mpg_display_text , mmp.mtrc_period_desc from MTRC_METRIC_PERIOD mmp  "+
		        		 "   join MTRC_metric as mm on mmp.mtrc_id = mm.mtrc_id "+ s +
		        	     " join mtrc_mpg as mmpg on mmpg.mtrc_period_id = mmp.mtrc_period_id " +
		        		 "   join MTRC_METRIC_PRODUCTS mmprd on mmprd.mtrc_period_id = mmp.mtrc_period_id " +
		        		 " where   mmp.mtrc_period_id in ("+mpids +") and mmp.tpt_id in ("+tptid +") order by mmprd.mtrc_prod_display_order ";


			     
 		  //    System.out.println(" Header Sql:"+SQL1);	
 	          Statement stmt = conn.createStatement();
 	    	  // String[] retval=null;
	          ResultSet rs = stmt.executeQuery(SQL1);
	          ResultSetMetaData rsmd = rs.getMetaData();
                   int  numColumns = rsmd.getColumnCount(); 

					while (rs.next()) {
	                     JSONObject jo = new JSONObject(); 
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);
				        String colvalue=rs.getString(i);
 			        if (colvalue  == null ) {colvalue="";}
 			             // retval=colvalue.split("-",2);
 			             // colvalue=retval[1];
 			              
				          jo.put(column_name, colvalue);   
					 } // for numcolumns
					 json.put(jo);
					} // while loop

					  rs.close();

                 // BUILDING ARRAY
				       
				         s=" join mtrc_tm_periods e on (('"+sdate +"' >= e.tm_per_start_dtm ) and ('" +edate +"' <=e.tm_per_end_dtm)) ";
				      // if  (!bldid.equals(null) && !bldid.trim().isEmpty())
				        if (fullyear.equals("Y"))
				       s =" join mtrc_tm_periods e on (('"+ sdate +"' >= e.tm_per_start_dtm ) and (e.tm_per_end_dtm <='"+ edate+"')) ";
				       
				   /*    SQL1="  SELECT d.mtrc_name, b.mtrc_period_id,b.mtrc_id,c.dsc_mtrc_lc_bldg_id,a.tm_period_id, "+
				    		 " c.dsc_lc_id,c.dsc_mtrc_lc_bldg_name,b.mtrc_period_name,a.mtrc_period_val_value "+ */
				       SQL1="  SELECT distinct dsc_mtrc_lc_bldg_name, c.dsc_mtrc_lc_bldg_id  "+
				    		 " from [dbo].[MTRC_METRIC_PERIOD_VALUE] a "+ s +
				    		   
				    		//  " join mtrc_tm_periods e on (( e.tm_per_start_dtm >='"+sdate +"') and (e.tm_per_end_dtm <='"+ edate+"')) "+
				    		 " and e.tm_period_id = a.tm_period_id  "+
				    		 " left join MTRC_METRIC_PERIOD b on a.mtrc_period_id = b.mtrc_period_id and b.tpt_id=e.tpt_id " +
				    		 " left join dsc_mtrc_lc_bldg c on c.dsc_mtrc_lc_bldg_id = a.dsc_mtrc_lc_bldg_id "+
				    		 " left join mtrc_metric d on d.mtrc_id=b.mtrc_id  " ;
				    	 
				            if  (!bldid.equals(null) && !bldid.trim().isEmpty())
				            
				    		 {
				    			 SQL1=SQL1 +" where a.dsc_mtrc_lc_bldg_id=" +bldid ;
				    		 }
				    		 else
				    		 {
				    			 SQL1=SQL1+" order by dsc_mtrc_lc_bldg_name";
				    		 }
				            
				       SQL1="  SELECT distinct dsc_mtrc_lc_bldg_name, dsc_mtrc_lc_bldg_id from dsc_mtrc_lc_bldg "+
				       " where  '"+sdate +"' >=dsc_mtrc_lc_bldg_eff_start_dt    and '"+edate +"' <=dsc_mtrc_lc_bldg_eff_end_dt " +
				       " order by dsc_mtrc_lc_bldg_name";	   
		    //  System.out.println(" Building Sql:"+SQL1);	
			 	        stmt = conn.createStatement();
			 	    	 int  mpid=0;
				         rs = stmt.executeQuery(SQL1);
				         rsmd = rs.getMetaData();
				        
			              numColumns = rsmd.getColumnCount(); 

								while (rs.next()) {
				                     JSONObject jo = new JSONObject(); 
								for (int i=1; i<numColumns+1; i++) {
									
							        String column_name = rsmd.getColumnName(i);
							        String colvalue=rs.getString(i);
							       
							        
			 			        if (colvalue  == null ) {colvalue="";}
							          jo.put(column_name, colvalue);   
								 } // for numcolumns
								 json1.put(jo);
								} // while loop

								  rs.close();

								  
								 // BUILDINGs  and Metrics ARRAY 
							        s=" join mtrc_tm_periods e on (('"+sdate +"' >= e.tm_per_start_dtm ) and ('" +edate +"' <=e.tm_per_end_dtm)) ";
						            t= " and  (('"+sdate +"' >= mmpg.mpg_start_eff_dtm ) and ('" +edate +"' <=mmpg.mpg_end_eff_dtm)) ";   
							      // if  (!bldid.equals(null) && !bldid.trim().isEmpty())
							        if (fullyear.equals("Y"))
							        {
							        s =" join mtrc_tm_periods e on (( e.tm_per_start_dtm >='"+sdate +"') and (e.tm_per_end_dtm <='"+ edate+"')) ";
							        t =" and  (( mmpg.mpg_start_eff_dtm >='"+sdate +"') and (mmpg.mpg_end_eff_dtm <='"+ edate+"')) ";
							        }  
							       SQL1="  SELECT d.mtrc_name, b.mtrc_period_id,b.mtrc_id,c.dsc_mtrc_lc_bldg_id,a.tm_period_id, "+
						    		 " c.dsc_lc_id,c.dsc_mtrc_lc_bldg_name,d.[mtrc_name],a.mtrc_period_val_value, "+
								     " d.mtrc_min_val,d.mtrc_max_val,f.data_type_token,  DATENAME(MONTH, e.tm_per_start_dtm) AS MonthName ," +
						    		 " mmprd.mtrc_prod_display_order,mmprd.mtrc_prod_display_text,mpg_display_text " +
								     " , mmpg.mpg_less_val, mmpg.mpg_less_eq_val,mmpg.mpg_greater_val, mmpg.mpg_greater_eq_val,mmpg.mpg_equal_val "+
						    		 ", rmps.rz_mps_status, g.rz_mpvg_goal_met_yn   from [dbo].[MTRC_METRIC_PERIOD_VALUE] a "+ s+
								     
						    	//	 " join mtrc_tm_periods e on (( e.tm_per_start_dtm >='"+sdate +"') and (e.tm_per_end_dtm <='"+ edate+"' )) "+
						    		 " and e.tm_period_id = a.tm_period_id  "+
						    	     " join rz_mtrc_period_status as rmps on rmps.tm_period_id = e.tm_period_id and " +
						    		 " rmps.mtrc_period_id = a.mtrc_period_id " +
						    	     " join mtrc_mpg as mmpg on mmpg.mtrc_period_id =a.mtrc_period_id " + t+ 					    	 
						    		 " left join MTRC_METRIC_PERIOD b on a.mtrc_period_id = b.mtrc_period_id and b.tpt_id=e.tpt_id " +
						    		 " left join dsc_mtrc_lc_bldg c on c.dsc_mtrc_lc_bldg_id = a.dsc_mtrc_lc_bldg_id "+
						    		 " left join mtrc_metric d on d.mtrc_id=b.mtrc_id " +
						    		 " left join mtrc_data_type f on f.data_type_id=d.data_type_id "+						    		
							         " left join mtrc_metric_products as mmprd on mmprd.mtrc_period_id = a.mtrc_period_id "+
							         " left join RZ_MTRC_PERIOD_VAL_GOAL g on a.Mtrc_period_val_id = g.Mtrc_period_val_id ";
						    		 
						       	 if  (!bldid.equals(null) && !bldid.trim().isEmpty())
							          
						    		 {
						    			 SQL1=SQL1 +" where a.dsc_mtrc_lc_bldg_id=" +bldid ;
						    		 }
							    	  if  ((!bldid.equals(null) && !bldid.trim().isEmpty()) && 
							    				 (!mtrcid.equals(null) && !mtrcid.trim().isEmpty()))
							    		 {
							    			 SQL1=SQL1 +" and b.mtrc_id="+mtrcid  ;
							    			 SQL1 =SQL1 +" order by a.tm_period_id,b.mtrc_id";		
							    		 }
							    		 
							    		 if  (( bldid.equals(null) || bldid.trim().isEmpty()) && 
							    				 (!mtrcid.equals(null) && !mtrcid.trim().isEmpty()))
							    		 {
							    			 SQL1=SQL1 +" where b.mtrc_id="+mtrcid  ;
							    			 SQL1 =SQL1 +" order by a.tm_period_id,b.mtrc_id";		
							    		 }					    		 
							    		 
							    		 if  ((bldid.equals(null)  || bldid.trim().isEmpty()) &&
							    				 (mtrcid.equals(null) ||  mtrcid.trim().isEmpty()))
							    		 {
							    			 SQL1=SQL1+"order by dsc_mtrc_lc_bldg_name, b.mtrc_id";
							    		 }						    			 
						    			 
 
					           System.out.println(" BuildingMetric Sql:"+SQL1);	
					 	        stmt = conn.createStatement();
					 	    	 
						         rs = stmt.executeQuery(SQL1);
						         rsmd = rs.getMetaData();
						        
					              numColumns = rsmd.getColumnCount(); 
							    	Double cvalue;
										while (rs.next()) {
											pctyn="N";
											intyn="N";
											if (rs.getString("data_type_token").equals("int")) intyn="Y";
											if (rs.getString("data_type_token").equals("pct")) pctyn="Y";
											
									        mtrcpassyn="N";
									        //first we need to check if goal met y/n values already exist in the db goal table
									        if((rs.getString("rz_mpvg_goal_met_yn")!=null)&&
										       (!rs.getString("rz_mpvg_goal_met_yn").trim().isEmpty()))
										      {
										          mtrcpassyn = rs.getString("rz_mpvg_goal_met_yn");
										      }
										      else// if values don't exist, that means what period/metric isn't closed
										    	  // and we need to dynamically determine the mtrcpassyn value
										      {
										    	  if ((!rs.getString("mtrc_period_val_value").equals(null)) &&
													    	 (!rs.getString("mtrc_period_val_value").trim().isEmpty()))
													      {		  
															if (((rs.getString("data_type_token").equals("int"))  ||
															   (rs.getString("data_type_token").equals("dec"))  ||
															   (rs.getString("data_type_token").equals("cur"))  ||
															   (rs.getString("data_type_token").equals("pct"))) &&
															    (!rs.getString("mtrc_period_val_value").equals("N/A")))
															{		
													        // NOW CHECK TO SEE IF FAILED GOALS
												           
													        if ((rs.getString("mpg_less_eq_val") != null) && 
													           (rs.getString("mpg_greater_eq_val") != null)) 
													           {
													        
														         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
													        	 if ((cvalue >= Double.parseDouble(rs.getString("mpg_greater_eq_val"))) &&
													        		(cvalue  <=	 Double.parseDouble(rs.getString("mpg_less_eq_val"))))
													        		 mtrcpassyn="Y";
													           }
													       
													        if ((rs.getString("mpg_less_eq_val") == null) && 
															           (rs.getString("mpg_greater_eq_val") != null)) 
															           {
															        
																         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
															        	 if (cvalue >= (rs.getDouble("mpg_greater_eq_val")))  
															        		 mtrcpassyn="Y";
															           }								        
										 
													        if ((rs.getString("mpg_less_eq_val") !=null) && 
															           (rs.getString("mpg_greater_eq_val") == null))
															           {
															        
																         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
															        	 if (cvalue <= (rs.getDouble("mpg_less_eq_val")))  
															        		 mtrcpassyn="Y";
															           }		
															}  // not equal NA
													      } // end of null value in values
													      if ((rs.getString("mtrc_period_val_value").equals(null)) ||
													    	 (rs.getString("mtrc_period_val_value").trim().isEmpty())||
													    	  rs.getString("mtrc_period_val_value").equals("N/A"))
													      {
													    	  mtrcpassyn="X";
													    	  
													      }
										      }
									        // only do the validation if the value is not null
									      //  System.out.println(" value is:" +rs.getString("mtrc_period_val_value").trim() );
									 
									     /* if ((!rs.getString("mtrc_period_val_value").equals(null)) &&
									    	 (!rs.getString("mtrc_period_val_value").trim().isEmpty()))
									      {		  
											if (((rs.getString("data_type_token").equals("int"))  ||
											   (rs.getString("data_type_token").equals("dec"))  ||
											   (rs.getString("data_type_token").equals("cur"))  ||
											   (rs.getString("data_type_token").equals("pct"))) &&
											    (!rs.getString("mtrc_period_val_value").equals("N/A")))
											{		
									        // NOW CHECK TO SEE IF FAILED GOALS
								           
									        if ((rs.getString("mpg_less_eq_val") != null) && 
									           (rs.getString("mpg_greater_eq_val") != null)) 
									           {
									        
										         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
									        	 if ((cvalue >= Double.parseDouble(rs.getString("mpg_greater_eq_val"))) &&
									        		(cvalue  <=	 Double.parseDouble(rs.getString("mpg_less_eq_val"))))
									        		 mtrcpassyn="Y";
									           }
									       
									        if ((rs.getString("mpg_less_eq_val") == null) && 
											           (rs.getString("mpg_greater_eq_val") != null)) 
											           {
											        
												         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
											        	 if (cvalue >= (rs.getDouble("mpg_greater_eq_val")))  
											        		 mtrcpassyn="Y";
											           }								        
						 
									        if ((rs.getString("mpg_less_eq_val") !=null) && 
											           (rs.getString("mpg_greater_eq_val") == null))
											           {
											        
												         cvalue=Double.parseDouble(rs.getString("mtrc_period_val_value"));
											        	 if (cvalue <= (rs.getDouble("mpg_less_eq_val")))  
											        		 mtrcpassyn="Y";
											           }		
											}  // not equal NA
									      } // end of null value in values
									      if ((rs.getString("mtrc_period_val_value").equals(null)) ||
									    	 (rs.getString("mtrc_period_val_value").trim().isEmpty())||
									    	  rs.getString("mtrc_period_val_value").equals("N/A"))
									      {
									    	  mtrcpassyn="X";
									    	  
									      }
											
									    
									        
									        // end checking of goals
									         
									         */
                                    
						                     JSONObject jo = new JSONObject(); 
										for (int i=1; i<numColumns+1; i++) {
									        String column_name = rsmd.getColumnName(i);
									        String colvalue=rs.getString(i);
									        if (column_name.equals("mtrc_period_id")) mpid=rs.getInt(i);
 
								        try
								        {
								            if ((pctyn.equals("Y"))  && column_name.equals("mtrc_period_val_value") && (!colvalue.equals(null) || !colvalue.trim().isEmpty()))
									        {
									        	 cvalue=(double) 0;
												if (column_name.equals("mtrc_period_val_value"))
												{
													// System.out.println("Metric Value is:"+colvalue +" bld:"+rs.getString(1));
											 
													  cvalue=Double.parseDouble(colvalue)*100;
													 // df2.format(input));
									        		 // cvalue=Float.parseFloat(colvalue)*100;
									        		 // System.out.println("Cvalue after multiply is:"+cvalue);
												     // colvalue=Float.toString(cvalue);
													  colvalue=df2.format(cvalue);
												     // System.out.println("Cvalue float to string is:"+colvalue);
												     }	
												      	        	
									        	}
								        }
								        catch(Exception e)
								        {
								          // System.out.println("Invalid Percent value:"+colvalue);	
								        }
	// convert double to int for int data types
								        try
								        {
								            if ((intyn.equals("Y"))  && (column_name.equals("mtrc_min_val")  || column_name.equals("mtrc_max_val"))
								            		&& (!colvalue.equals(null) || !colvalue.trim().isEmpty()))
									        {
								            	cvalue=(double) 0;
 											         cvalue=Double.parseDouble(colvalue);
											         int y = (int)Math.round(cvalue);
 													  colvalue=Integer.toString(y);
										 
												       	
									        	}
								        }
								        catch(Exception e)
								        {
								          // System.out.println("Invalid Percent value:"+colvalue);	
								        }	
								        
						        
							        		
								         
								         									        
					 			        if (colvalue  == null ) {colvalue="";}
									          jo.put(column_name, colvalue);   
										 } // for numcolumns
										jo.put("mpg_mtrc_passyn", mtrcpassyn);
										 json2.put(jo);
										} // while loop

										  rs.close();	
										  
//            Get the previous and current month data 
										  
										     SQL1="select max(a.tm_period_id) from mtrc_tm_periods a " +
											     	  " left join rz_mtrc_period_status b on a.tm_period_id=b.tm_period_id and b.mtrc_period_id in ("+mpids +")" +
											     	  // + "="+mpid +
											    	  " where  tm_per_start_dtm  < " +
											          " DateAdd(month,-1,'"+calyear +"-"+calmonth +"-28') union all " + 
											          " select max(a.tm_period_id) from mtrc_tm_periods a " +
											    	  " left join rz_mtrc_period_status b on a.tm_period_id=b.tm_period_id and b.mtrc_period_id in ("+mpids +")" +
											          " where  tm_per_start_dtm  < " +
											          " DateAdd(month,+1,'"+calyear +"-"+calmonth +"-28') ";
							//		 	 	   System.out.println("Find Min Max Sql:"+SQL1);			    	 
									              int rcount=0;
									              String tmid="";
									              String strt="Y";
										          rs = stmt.executeQuery(SQL1);
										          rsmd = rs.getMetaData();
										          numColumns = rsmd.getColumnCount(); 
													while (rs.next()) 
													{
														rcount++;
														if (rcount == 1)
														{ 
															 if (rs.getInt(1) == 0)strt="N";
															 tmid=rs.getInt(1) +",";
														}
														if (rcount == 2) tmid=tmid +rs.getInt(1); 			
													}	
												   rs.close();	
												   
									            if (rcount != 2)
									            {
									                String msg="Cannot find Min and Max Calendar Days.";
									                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
									                rb=Response.ok(sb.toString()).build();
									                if (conn != null) { try {
									    				conn.close();
									    			} catch (SQLException e1c) {
									    				// TODO Auto-generated catch block
									    				// e1.printStackTrace();
									    			}} 
									   	          return rb;	            	
									            	
									            }	
									        String [] mpa=mpids.split(",");
											SQL1="select  datename(month, tm_per_start_dtm) +'-'+Convert(varchar(4),Year(tm_per_start_dtm)) as yearmonth " +
													" from rz_mtrc_period_status a  " +
												    " left join mtrc_tm_periods b on a.tm_period_id = b.tm_period_id " +
												   " where a.tm_period_id in ("+ tmid +") and mtrc_period_id ="+mpa[0] +" and rz_mps_status <> 'Inactive' "+
											       " order by a.tm_period_id ";
									    //     System.out.println(" Look Back Sql is:"+SQL1);
									             rcount=0;
											      String minprd="";
											      String maxprd="";
											      
										          rs = stmt.executeQuery(SQL1);
										          rsmd = rs.getMetaData();
										          numColumns = rsmd.getColumnCount(); 
													while (rs.next()) 
													{
													   rcount++;
													   if (rcount == 1 ) 
													   {
														    if(strt.equals("N")) maxprd=rs.getString(1);
														    if(strt.equals("Y")) minprd=rs.getString(1);
													   }
													   if (rcount == 2)maxprd=rs.getString(1);
 
								 			
													}
													
													 obj1.put("previousperiod", minprd); 
													 obj1.put("nextperiod", maxprd);

													 
										              rs.close();											  
 
										  
										  obj1.put("buildings",json1);
										  obj1.put("metrics",json);			
										  obj1.put("buildingsmetrics",json2);	
										  stmt.close();
							       rb=Response.ok(obj1.toString()).build();
						            if (conn != null) { try {
										conn.close();
									} catch (SQLException e1d) {
										// TODO Auto-generated catch block
										// e1.printStackTrace();
									}} 
				        
 		      return rb;
             
		 }
		 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
             String msg="Metric DB Connection Failed.";
             sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
             rb=Response.ok(sb.toString()).build();
             if (conn != null) { try {
 				conn.close();
 			} catch (SQLException e1e) {
 				// TODO Auto-generated catch block
 				// e1.printStackTrace();
 			}} 
	          return rb;
			}
		 
	}
	
	 
	//   GET PERIOD ID
	private String  get_periodid(String sdate,String edate, Connection conn)
	{
		    String pid="";
		    String SQL1=" select tpt_id,tm_period_id from mtrc_tm_periods where "+
		    		   " ([tm_per_start_dtm] <= '"+sdate +"' and  [tm_per_end_dtm]  >= '"+edate +"')";
		    try
		    {
              Statement stmt = conn.createStatement();
 
	          ResultSet rs = stmt.executeQuery(SQL1);
	          ResultSetMetaData rsmd = rs.getMetaData();
	        
                 int  numColumns = rsmd.getColumnCount(); 

					while (rs.next())
					{
						pid=rs.getString(1) +"|" +rs.getString(2); 
					} // while loop
					  rs.close();
		    }
		    catch(Exception e)
		    {
		    	System.out.println("Something went wrong");
		    }
		     return pid;
	}
	
	// get period id's 
	
	//   GET PERIOD ID
	private String  get_periodids(String sdate,String edate, Connection conn)
	{
		    String pid="";
		    int rcount=0;
		    String SQL1=" select tm_period_id from mtrc_tm_periods where "+
		    		   " ([tm_per_start_dtm] >= '"+sdate +"' and  [tm_per_end_dtm]  <= '"+edate +"')";
		     //System.out.println("sql is:"+SQL1);
		    try
		    {
              Statement stmt = conn.createStatement();
 
	          ResultSet rs = stmt.executeQuery(SQL1);
	          ResultSetMetaData rsmd = rs.getMetaData();
	        
                 int  numColumns = rsmd.getColumnCount(); 

					while (rs.next())
					{
						if (rcount > 0) pid=pid+",";
						pid=pid+rs.getString(1) ; 
						rcount++;
					} // while loop

					  rs.close();
		    }
		    catch(Exception e)
		    {
		    	
		    }
		     return pid;
	}
	
	
	private String get_mtrcperiodid(String prodname, Connection conn)
	{
		    String   mpids="";
		    String SQL1="SELECT  a.[mtrc_period_id]  FROM [dbo].[MTRC_METRIC_PRODUCTS] a "+
		    		    " where a.prod_id in (select prod_id from mtrc_product where prod_name ='"+prodname +"')"+ 
                        " and ( [mtrc_prod_eff_start_dt] <= getdate() and  [mtrc_prod_eff_end_dt]  >= getdate()) "+
                        " and a.[mtrc_prod_top_lvl_parent_yn]='Y'";
		 //   System.out.println("sql is:"+SQL1);
		    try
		    {
              Statement stmt = conn.createStatement();
 
	          ResultSet rs = stmt.executeQuery(SQL1);
	          ResultSetMetaData rsmd = rs.getMetaData();
	        
                 int  numColumns = rsmd.getColumnCount(); 
                  int rowcount=0;
					while (rs.next())
					{
						if (rowcount > 0) mpids=mpids+",";
						mpids=mpids +rs.getString(1);  
						rowcount++;
 
					} // while loop

					  rs.close();
		    }
		    catch(Exception e)
		    {
		    	
		    }
		 //   System.out.println("Rturn metricds of:"+mpids);
		     return mpids;
	}
}
	
	
		 
 


