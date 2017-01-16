package com.dsc.mtrc.rest;



//here is where I added ldap stuff

import java.sql.Timestamp;


import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.internal.*; 

@Path("/v1/metric")
public class Metric {

@GET
	@Produces(MediaType.TEXT_HTML)
	public String returnTitle()
	{
	java.util.Date date= new java.util.Date();
		return "<p>Default LocalHost Metric Service</p>"+new Timestamp(date.getTime());
	}

//****************  Authenication Service
/*@Path("/whoami")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response whoami(JSONObject inputJsonObj) throws Exception {
	  
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());
	  
	 Response rb = null;
	 ldap vr = new ldap();
	 rb=vr.ldap(inputJsonObj);
 
 
	     return rb;
	  
	}*/

//****************  Building LC
@Path("/buildinglc")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response buildinglc(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	// System.out.println("You are in buildinglc rest"); 
	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());
	  

	 BuildingLc bldlc = new BuildingLc();
	  rb=bldlc.BuildingLc(inputJsonObj);
       
	     return rb;
	  
	}
//****************  All Metric Metric Periods
@Path("/mmperiods")
@GET
@Produces(MediaType.APPLICATION_JSON)
public Response getAllMMperiods ()throws Exception
{
	 Response rb = null;
	 AllMetricPeriods amp = new AllMetricPeriods();
	 rb=amp.getAllMetricPeriods();
	 return rb;
}
//****************  Time  Peirod
@Path("/timeperiod")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response timeperiod(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	System.out.println("You are in buildinglc rest"); 
	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());
	  

	 BuildingLc bldlc = new BuildingLc();
	  rb=bldlc.BuildingLc(inputJsonObj);
      
	     return rb;
	  
	}

//****************  Metric Name
@Path("/metricname")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricname(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 MetricName mtrcname = new MetricName();
	  rb=mtrcname.MetricName(inputJsonObj);

	     return rb;
	  
	}



//****************  Metric Period
@Path("/metricperiod")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricperiod(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 MetricPeriod mtrcperiod = new MetricPeriod();
	  rb=mtrcperiod.MetricPeriod(inputJsonObj);

	     return rb;
	  
	}

//****************  Metric  Authorization
@Path("/metricauthorization")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricauthorization(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	MetricAuthorization mtrcauthorization = new MetricAuthorization();
	  rb=mtrcauthorization.MetricAuthorization(inputJsonObj);
     return rb;
	  
	}

//****************  Metric Period Save
@Path("/metricperiodsave")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricperiodsave(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	// System.out.println("You are in MetricPeriodSave");
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 MetricPeriodSave mtrcperiodsave = new MetricPeriodSave();
	 //String [] msg=mtrcperiodsave.MetricPeriodSave(inputJsonObj);
  // String r=msg[1];
   //rb=Response.ok(r.toString()).build();
   //return rb;
	     return mtrcperiodsave.saveMetricPeriod(inputJsonObj);
	  
	}


//****************  WMSBuilding 
@Path("/wmsbuilding")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response wmsbuilding(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 WMSBuilding wmsbuilding  = new WMSBuilding();
	  rb=wmsbuilding.WMSBuilding(inputJsonObj);

	     return rb;
	  
	}

//****************  Time Period
@Path("/metrictimeperiod")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metrictimeperiod(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 MetricTimePeriod mtimepeirod  = new MetricTimePeriod();
	  rb=mtimepeirod.MetricTimePeriod(inputJsonObj);

	     return rb;
	  
	}

//****************  Load Throughput
@Path("/autouploadmetric")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response throughputload(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	MetricAutoLoader mtimepeirod  = new MetricAutoLoader();
	rb =mtimepeirod.loadMetric(inputJsonObj);
    System.out.println("Message :"+rb);
	 return rb;
	  
	}

//****************  Load Throughput
@Path("/metricperiodclose")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricperiodclose(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	MetricPeriodClose mtpclose  = new MetricPeriodClose();
	rb =mtpclose.MetricPeriodClose(inputJsonObj);
 //   System.out.println("Message :"+rb);
	 return rb;
	  
	}

//**************** AllMetrics
@Path("/buildingsmetrics")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response allmetrics(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	AllMetrics allmtrcs  = new AllMetrics();
	rb =allmtrcs.AllMetrics(inputJsonObj);
 // System.out.println("Message :"+rb);
	 return rb;
	  
	}

//**************** AllMetrics
@Path("/summary")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response summary(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	SummaryDetail summary  = new SummaryDetail();
	rb =summary.SummaryDetail(inputJsonObj);
// System.out.println("Message :"+rb);
	 return rb;
	  
	}
//**************** AllMetrics
@Path("/buildingsummary")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response buildingsummary(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	BuildingSummary buildingsummary  = new BuildingSummary();
	rb =buildingsummary.BuildingSummary(inputJsonObj);
// System.out.println("Message :"+rb);
	 return rb;
	  
	}
//**************** AllMetrics
@Path("/metricsummary")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricsummary(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 String msg = null;
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	MetricSummary metricsummary  = new MetricSummary();
	rb =metricsummary.MetricSummary(inputJsonObj);
// System.out.println("Message :"+rb);
	 return rb;
	  
	}
//****************  All Reasons
@Path("/reasons")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response listAllReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.getReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  All Assigned Reasons
@Path("/assignedreasons")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response listAllAssignedReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.getAssignedReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Save/Update Assigned Reasons
@Path("/saveassignedreason")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveAssignedReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.addUpdateAssignedReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Remove Assigned Reasons
@Path("/removeassignedreason")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response removeAssignedReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.removeAssignedReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Add  Reason
@Path("/savereason")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.addReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Update  Reason
@Path("/updatereason")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.editReason(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Remove  Reason
@Path("/removereason")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response removeReason(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.removeReason(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}

//****************  Reorder  Reasons
@Path("/reorderreasons")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response reorderReasons(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ReasonManager reasonManager  = new ReasonManager();
	 rb =reasonManager.reorderReasons(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Get Action Plan by rz_bapm_id
@Path("/getactplanbyid")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAPbyId(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.getAPforBampId(inputJsonObj);
   System.out.println("Message :"+rb);
	 return rb;
	}

//****************  Submit action plan 
@Path("/submitactionplan")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response submitActionPlanDetail(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.submitActionPlan(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Save action plan 
@Path("/saveactionplan")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveActionPlanDetail(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.saveActionPlan(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}

//****************  Submit action plan review
@Path("/submitapreview")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response submitActionPlanreview(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.submitAPReview(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}

//****************  Get Prior Action Plans
@Path("/getpriorap")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getPriorActPlan(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.getPriorAP(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Get Prior Action Plans
@Path("/lookupap")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response lookupActPlan(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 ActionPlanManager apManager  = new ActionPlanManager();
	 rb = apManager.lookupActionPlan(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Get User Roles
@Path("/getuserroles")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUserRole(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 if(!inputJsonObj.has("sso_id")||(inputJsonObj.get("sso_id")==null)||(inputJsonObj.get("sso_id").equals("")))
		{				
			JSONObject retJson = new JSONObject();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "sso_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			
			RZAuthenticationManager authManager  = new RZAuthenticationManager();
			rb = authManager.loginUser(inputJsonObj.getString("sso_id"));
		}
	
System.out.println("Message :"+rb);
	 return rb;
	}

//*****************************This method Authenticates REDZONE  user against LDAP, and provides user authorization 
@Path("/loginrzuser")
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
	public Response loginRZUser(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 if(!inputJsonObj.has("sso_id")||(inputJsonObj.get("sso_id")==null)||(inputJsonObj.get("sso_id").equals("")))
		{				
			JSONObject retJson = new JSONObject();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "sso_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
	 if(!inputJsonObj.has("password")||(inputJsonObj.get("password")==null)||(inputJsonObj.get("password").equals("")))
		{				
			JSONObject retJson = new JSONObject();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "password is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			
			RZAuthenticationManager authManager  = new RZAuthenticationManager();
			rb = authManager.loginRZUser(inputJsonObj.getString("sso_id"), inputJsonObj.getString("password"));
		}
	
     System.out.println("Message :"+rb);
	 return rb;
}

//*****************************This method Authenticates REDZONE  user against LDAP, and provides user authorization 
@Path("/loginrzdmuser")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response loginDMRZUser(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 if(!inputJsonObj.has("sso_id")||(inputJsonObj.get("sso_id")==null)||(inputJsonObj.get("sso_id").equals("")))
		{				
			JSONObject retJson = new JSONObject();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "sso_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
	 if(!inputJsonObj.has("password")||(inputJsonObj.get("password")==null)||(inputJsonObj.get("password").equals("")))
		{				
			JSONObject retJson = new JSONObject();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "password is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			
			RZAuthenticationManager authManager  = new RZAuthenticationManager();
			rb = authManager.loginRZDMUser(inputJsonObj.getString("sso_id"), inputJsonObj.getString("password"));
		}
	
System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Get Tasks Summary
@Path("/gettaskscount")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTasks(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 TaskManager tManager  = new TaskManager();
	 rb = tManager.getTasksSummary(inputJsonObj);
     System.out.println("Message :"+rb);
	 return rb;
	}

//****************  Get Tasks Details
@Path("/gettaskdetails")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTaskDetails(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 TaskManager tManager  = new TaskManager();
	 rb = tManager.getTaskDetails(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}
//****************  Get Tasks Details
@Path("/getmyteamactivities")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMyTeamActivities(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;
	 TaskManager tManager  = new TaskManager();
	 rb = tManager.getMyTeamActivities(inputJsonObj);
System.out.println("Message :"+rb);
	 return rb;
	}

/*Method to get the list of user responsible for a specific Action Plan
 * */
@Path("/getusersforap")
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public Response getUserForAP(JSONObject inputJsonObj) throws Exception{
	
	TaskManager tManager = new TaskManager();
	Response rb = tManager.getUsersForAp(inputJsonObj);
	System.out.println("Message:"+rb);
	return rb;
	
}
/*Method to get the tasks summary for rz_bldg_user role
 * */
@Path("/getmyteamtaskscount")
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public Response getMyTeamTasksSummary(JSONObject inputJsonObj) throws Exception{
	
	TaskManager tManager = new TaskManager();
	Response rb = tManager.getMyTeamActivitySummary(inputJsonObj);
	System.out.println("Message:"+rb);
	return rb;
	
}
/*Method for get all metadata about a specific metric period value id
 * */
@Path("/getmpvalueinfo")
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public Response getMetricPeriodValueInfo(JSONObject inputJsonObj) throws Exception{
	
	HelpersAPI helper = new HelpersAPI();
	Response rb = helper.getMetricPeriodValueInfo(inputJsonObj);
	System.out.println("Message:"+rb);
	return rb;
	
}

}
