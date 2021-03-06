package com.dsc.mtrc.rest;



//here is where I added ldap stuff
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
//import javax.servlet.ServletContext;
import javax.naming.NamingEnumeration;
import javax.naming.AuthenticationException;
import javax.naming.AuthenticationNotSupportedException;
import javax.naming.Context;
import javax.naming.NameClassPair;
//import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
//import org.apache.http.HttpEntity;
//import org.apache.http.util.EntityUtils;

import java.sql.Timestamp;
import java.util.Hashtable; 
//ending lDAP stuff

//new import for json

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
@Path("/whoami")
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
	  
	}

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

//****************  Metric  Period Save
@Path("/metricauthorization")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response metricauthorization(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	// System.out.println("You are in MetricPeriodSave");
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
	 String [] msg=mtrcperiodsave.MetricPeriodSave(inputJsonObj);
   String r=msg[1];
   rb=Response.ok(r.toString()).build();
	     return rb;
	  
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


}
