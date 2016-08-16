package com.dsc.mtrc.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.*;
 
import com.dsc.mtrc.dao.ConnectionManager;

public class ThroughPutLoad {
	
		
	public Response ThroughPutLoad(JSONObject inputJsonObj) throws JSONException {
	    Response rb = null;
		String  msg = null;
		String theurl="";
		String dwurl="";
	 
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		String insstmt=null;
		String mtrcnayn=null;
		
		if ((! inputJsonObj.has("packagename")) || (! inputJsonObj.has("calmonth")) || (! inputJsonObj.has("calmonth")) )
		{
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "packagename and calmonth and calyear  json tag required for this API"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	       	return rb;			
		}
		String pkagename= inputJsonObj.get("packagename").toString();
	    String previousMonth  = inputJsonObj.get("calmonth").toString();
	    String thisyear  =inputJsonObj.get("calyear").toString();
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
		/*
		msg=LoadStatus(dwurl,pkagename,previousMonth,thisyear);
		if (! msg.equals("0"))
		{
			 msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   pkagename +" DW load was not complete for "+previousMonth +" " +thisyear +" "  +"\"}";
			   rb=Response.ok(msg.toString()).build();
		       	return rb;
		}
			*/ 
	    // get periodid
		tmperiodid=metrictimeperiod(theurl,previousMonth, thisyear);
		

		// get metric data
		String [] mdata=metricname(theurl,previousMonth, thisyear);
       
		if (mdata[0] != null)
		{
			if (mdata[0].length() > 10)
			{
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		  msg  +"\"}";
	           rb=Response.ok(msg.toString()).build();
	           return rb;			
			}
		}
		else
		{
			String [] darray=mdata[1].split(",");
			
 			mtrcperiodid=darray[0];			
 			mtrcnayn=darray[1];		
			
		}
		
		// get wmsvolume
		  tput =dscwmsvolume(dwurl,previousMonth, thisyear,mtrcperiodid);
		 
		  
 	       String  brkbldid= null;         
	    	// Now you have JSON array TPUt. Go through each get the building info and string to a insert
	        // statement and display that in system out.
	    		    		         
	    		String instmpt="INSERT INTO [dbo].[MTRC_METRIC_PERIOD_VALUE] "+
                              " ([mtrc_period_id] ,[dsc_mtrc_lc_bldg_id] "+
                              " ,[tm_period_id], [mtrc_period_val_added_dtm] "+
                              " ,[mtrc_period_val_added_by_usr_id] ,[mtrc_period_val_upd_dtm] "+
                              " ,[mtrc_period_val_upd_by_user_id] ,[mtrc_period_val_is_na_yn] "+
                              " ,[mtrc_period_val_value]) VALUES (";
	    		
	    		float  thruputchg=0;      
	            for(int i=0; i<tput.length(); i++)         
		        { 
		        	// System.out.println("The " + i + " element of the array: "+jsonArr.get(i));
		        	JSONObject s1 =  (JSONObject) tput.get(i);
               		 insstmt=insstmt+instmpt +mtrcperiodid +","+s1.getString("dsc_mtrc_lc_bldg_id").toString().trim() +
               				 ","+tmperiodid +"," +
	    	                            " getdate() ,'API','','','"+mtrcnayn+"','"+
	    	                            s1.getString("PeriodValue").toString() +"');";
		        	// System.out.println(" JSON RESULT FROM DSCWMSVOLUME:"+insstmt.toString());
		        } // for each array
	    		    		        
	    		    		 // ========================================================= 	
	            if (insstmt.equals(null)) insstmt=" ";
	         System.out.println("insert is:"+insstmt);
	         StringBuffer sb = new StringBuffer();  
	       
	        if (insstmt.length() > 100)
	        {
	        	insstmt="delete from [MTRC_METRIC_PERIOD_VALUE] where [mtrc_period_id]="+mtrcperiodid +
	        			" and [tm_period_id]="+tmperiodid + 
	        			" and [mtrc_period_val_added_by_usr_id]='API';" +insstmt;
	     	
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
				
		   		try
	     		{
	     			Statement stmt = conn.createStatement();
	     			System.out.println("SQL STATMENT:"+insstmt);
	     		    stmt.executeUpdate(insstmt);
	     		     
	     		}
	         	 catch (SQLException e) 
	          	{
	          		e.printStackTrace();
	          	}
		   		
		         if (conn != null) 
		         {
		      	   try{
		      		   conn.close();
		      		  } catch(SQLException e)
		      	      {e.printStackTrace(); }
		         } 		   		
	        } // end if
 	          //  responseStrBuildera=null;
	 	            msg="Through Put (volume) Metric Loaded Successfully .";
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
  public JSONArray dscwmsvolume(String dwurl,String previousMonth, String thisyear,String metricid)
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
    		 // =========================================================   
		    		         
		    		  // call DW metric for each of the json array call wms building to get building to insert data
	 
		    		    		    url = null;
		    		    			try {
		    		    				url = new URL(dwurl + "dscwmsvolume");
		    		    			} catch (MalformedURLException e1) {
		    		    				// TODO Auto-generated catch block
		    		    				e1.printStackTrace();
		    		    			}
		    		    		      
		    		    		     query= " {\"productname\":\"Red Zone\", \"tptname\":\"Month\",\"mtrcid\":"+metricid +",\"calmonth\":\""+
		    		    		     previousMonth +"\",\"calyear\":"+thisyear +"}";
		    		    		     // result will be:  {"building":"BP2"}
		    		    		     //make connection
		    		    		     
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
		    		    	 
		    		    		         if(api.has("DSCWMSVolumes")) 
		    		    		        	 {
		    		    		        	 tput=(JSONArray) api.get("DSCWMSVolumes");    
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
			    				msg[0]="Read Json string failed from metricname url:"+url;
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

 
