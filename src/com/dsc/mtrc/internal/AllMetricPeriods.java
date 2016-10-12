package com.dsc.mtrc.internal;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.sql.ResultSetMetaData;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import com.dsc.mtrc.dao.*;

public class AllMetricPeriods {

	public Response getAllMetricPeriods() throws JSONException {

		Response rb = null;
		StringBuffer sb = new StringBuffer();
		JSONObject obj1 = new JSONObject();
		JSONArray json = new JSONArray();
		String SQL = "select p.mtrc_period_id, mtrc_period_name,mtrc_period_token, m.mtrc_prod_display_text, mp.prod_name from MTRC_METRIC_PERIOD p ,MTRC_METRIC_PRODUCTS m, MTRC_PRODUCT mp"
                     +" where  p.mtrc_period_id=m.mtrc_period_id and m.mtrc_prod_top_lvl_parent_yn='Y'and  m.prod_id = mp.prod_id";
		
		try(Connection conn = ConnectionManager.mtrcConn().getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(SQL);)
		{
			conn.setReadOnly(true);
			ResultSetMetaData rsmd = rs.getMetaData();		
			int numColumns = rsmd.getColumnCount();
			while (rs.next()) 
			{
				JSONObject obj = new JSONObject();
				for (int i = 1; i < numColumns + 1; i++)
				{
					String column_name = rsmd.getColumnName(i);		
					obj.put(column_name, rs.getString(i)==null?"":rs.getString(i));
				} // for numcolumns
				json.put(obj);
			} // while loop
			obj1.put("mmperiod", (Object) json);
		}//end of try
		catch(Exception e){
			e.printStackTrace();
			String msg = "Failed to retrieve metric period information:"+e.getMessage();
			sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\"" + msg + "\"");
			rb = Response.ok(obj1.toString()).build();
			return rb;			
		}
		rb = Response.ok(obj1.toString()).build();
		return rb;

	}

}
