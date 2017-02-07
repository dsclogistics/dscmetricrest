package com.dsc.mtrc.internal;

import javax.ws.rs.core.Response;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.mtrc.dao.ConnectionManager;
/*This class contains lookup APIs to help the front end application to get some additional info it might need */
public class HelpersAPI {
	
	public Response getMetricPeriodValueInfo(JSONObject inputJsonObj) throws JSONException
	{
		Response rb = null;
		JSONObject retJson = new JSONObject();
		String prodName = null;
		int valId = -1;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		DecimalFormat df2 = new DecimalFormat("0.00");
        df2.setRoundingMode(RoundingMode.UP);
		
		
		if(!inputJsonObj.has("productname"))
		{				
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "productname is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		else
		{
			prodName = inputJsonObj.getString("productname");
		}
		if(inputJsonObj.has("mtrc_period_val_id") && inputJsonObj.getString("mtrc_period_val_id")!=null && !inputJsonObj.getString("mtrc_period_val_id").equals(""))
		{	

			valId = inputJsonObj.getInt("mtrc_period_val_id");							
		}
		else
		{
			
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "mtrc_period_val_id is required");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		try {
			conn = ConnectionManager.mtrcConn().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			retJson.put("result", "FAILED");
			retJson.put("resultCode", "200");
			retJson.put("message", "DB Connection Failed");
			rb = Response.ok(retJson.toString()).build();			
			return rb;
		}
		
		String SQL = " select month(tp.tm_per_start_dtm) as month, year(tp.tm_per_start_dtm) as year,"
				+ " mpv.mtrc_period_val_id, mpv.mtrc_period_val_value,bldg.dsc_mtrc_lc_bldg_name,bldg.dsc_mtrc_lc_bldg_id,"
				+ " mprod.mtrc_prod_display_text,mpv.mtrc_period_id,(coalesce(MTRC_MPBG.mpbg_display_text, MTRC_MPG.mpg_display_text)) as goal_txt,"
				+ " MTRC_DATA_TYPE.data_type_token,mgoal.rz_mpvg_goal_met_yn"
				+ " from MTRC_METRIC_PERIOD_VALUE mpv	"
				+ " inner join DSC_MTRC_LC_BLDG bldg"
				+ " on mpv.dsc_mtrc_lc_bldg_id = bldg.dsc_mtrc_lc_bldg_id"
				+ " inner join MTRC_METRIC_PERIOD mp"
				+ " on mpv.mtrc_period_id = mp.mtrc_period_id"
				+ " inner join MTRC_METRIC"
				+ " on mp.mtrc_id = MTRC_METRIC.mtrc_id"
				+ " inner join MTRC_DATA_TYPE"
				+ " on MTRC_METRIC.data_type_id =MTRC_DATA_TYPE.data_type_id"
				+ " inner join MTRC_METRIC_PRODUCTS mprod"
				+ " on mp.mtrc_period_id = mprod.mtrc_period_id"
				+ " inner join MTRC_PRODUCT prod"
				+ " on mprod.prod_id = prod.prod_id"
				+ " and prod.prod_name = ?"
				+ " inner join MTRC_TM_PERIODS tp"
				+ " on mpv.tm_period_id = tp.tm_period_id"
				+ " left outer join MTRC_MPBG"
				+ " on mp.mtrc_period_id =  MTRC_MPBG.mtrc_period_id"
				+ " and mpv.dsc_mtrc_lc_bldg_id = MTRC_MPBG.dsc_mtrc_lc_bldg_id"
				+ " and prod.prod_id = MTRC_MPBG.prod_id"
				+ " and tp.tm_per_start_dtm between  MTRC_MPBG.mpbg_start_eff_dtm and MTRC_MPBG.mpbg_end_eff_dtm"
				+ " and tp.tm_per_end_dtm between  MTRC_MPBG.mpbg_start_eff_dtm and MTRC_MPBG.mpbg_end_eff_dtm"
				+ " left outer join MTRC_MPG"
				+ " on mp.mtrc_period_id = MTRC_MPG.mtrc_period_id"
				+ " and prod.prod_id = MTRC_MPG.prod_id"
				+ " and tp.tm_per_start_dtm between MTRC_MPG.mpg_start_eff_dtm and MTRC_MPG.mpg_end_eff_dtm"
				+ " and tp.tm_per_end_dtm between MTRC_MPG.mpg_start_eff_dtm and MTRC_MPG.mpg_end_eff_dtm"
				+ " left outer join RZ_MTRC_PERIOD_VAL_GOAL mgoal"
				+ " on mpv.mtrc_period_val_id = mgoal.mtrc_period_val_id"
				+ " where mpv.mtrc_period_val_id = ?";
		 
		try
		{
			ps = conn.prepareStatement(SQL);
			ps.setString(1, prodName);
			ps.setInt(2, valId);
			rs = ps.executeQuery();
			retJson.put("result", "Success");
			while(rs.next())
			{
				//retJson.put("result", "Success");
				retJson.put("month", rs.getString("month"));
				retJson.put("year", rs.getString("year"));
				retJson.put("dsc_mtrc_lc_bldg_name", rs.getString("dsc_mtrc_lc_bldg_name"));
				retJson.put("dsc_mtrc_lc_bldg_id", rs.getInt("dsc_mtrc_lc_bldg_id"));
				retJson.put("mtrc_prod_display_text", rs.getString("mtrc_prod_display_text"));
				retJson.put("mtrc_period_id", rs.getInt("mtrc_period_id"));				
				String value = rs.getString("mtrc_period_val_value");
				
				if(rs.getString("data_type_token").equals("pct"))
				{
					double valueNum = Double.parseDouble(value)*100;
					value = df2.format(valueNum);
					retJson.put("mtrc_period_val_value", value);
				}
				else
				{
					retJson.put("mtrc_period_val_value", value);
				}
				retJson.put("mtrc_period_val_id", rs.getInt("mtrc_period_val_id"));
				retJson.put("goal_txt", rs.getString("goal_txt"));
				retJson.put("data_type_token", rs.getString("data_type_token"));
				retJson.put("rz_mpvg_goal_met_yn", rs.getString("rz_mpvg_goal_met_yn"));
				
			}//end of while
			rb = Response.ok(retJson.toString()).build();
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			JSONObject errJson = new JSONObject();
			errJson.put("result", "FAILED");
			errJson.put("resultCode", "200");
			errJson.put("message", "Error:"+e.getMessage());
			rb = Response.ok(errJson.toString()).build();
						
		}//end of catch
		finally
		{
			if(ps!=null)
			{
				try{ps.close();} catch(Exception e){e.printStackTrace();}
			}
			if(conn!=null)
			{
				try{conn.close();} catch(Exception e){e.printStackTrace();}
			}
			
		}//end of finally
		
		return rb;
	}

}
