package com.dsc.mtrc.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetricPeriodHelper {
	
	public int getClosedMetricCount(Connection conn, int tmPeriodId)
	{
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		String SQL ="select count(*) closed_metrics	  from rz_mtrc_period_status where tm_period_id = ? and rz_mps_status = 'Closed'";
		int closedMtrCount = 0;
		
		try
		{
			ps = conn.prepareStatement(SQL);
			ps.setInt(1,tmPeriodId);
			rs = ps.executeQuery();
			while(rs.next())
			{
				closedMtrCount = rs.getInt("closed_metrics");
			}
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			closedMtrCount = -1;
		}
		finally
		{
			if(rs !=null)
			{
				try 
				{
					rs.close();
				} 
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}
			if(ps!=null)
			{
				try 
				{
					ps.close();
				}
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}			
		}//end of finally		
		return closedMtrCount;		
	}

	public List<Integer> getBuildingsForActionPlan(Connection conn, int tmPeriodId)
	{
		List<Integer> bldgIds = new ArrayList<Integer>();
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		String SQL =  " select v.dsc_mtrc_lc_bldg_id," 
                     +" count(*)  cnt"
                     +" from mtrc_metric_period_value v"  
                     +" join rz_mtrc_period_status s"
	                 +" on v.tm_period_id = s.tm_period_id"
                     +" and v.mtrc_period_id = s.mtrc_period_id"
	                 +" join  RZ_MTRC_PERIOD_VAL_GOAL g"
	                 +" on v.mtrc_period_val_id = g.mtrc_period_val_id"
                     +" where s.tm_period_id =? "
                     +" and s.rz_mps_status = 'Closed'"
                     +" and g.rz_mpvg_goal_met_yn='N'"
                     +" and not exists"
                     +" (select 1 "
                     +" from RZ_BLDG_ACTION_PLAN a "
                     +" where a.dsc_mtrc_lc_bldg_id = v.dsc_mtrc_lc_bldg_id"
                     +" and a.tm_period_id = ?)"
                     +" group by v.dsc_mtrc_lc_bldg_id"
                     +" having count(*)>=3";
		
		try
		{
			ps = conn.prepareStatement(SQL);
			ps.setInt(1,tmPeriodId);
			ps.setInt(2,tmPeriodId);
			rs = ps.executeQuery();
			while(rs.next())
			{
				
				bldgIds.add(rs.getInt("dsc_mtrc_lc_bldg_id"));
			}
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();			
		}
		finally
		{
			if(rs !=null)
			{
				try 
				{
					rs.close();
				} 
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}
			if(ps!=null)
			{
				try 
				{
					ps.close();
				}
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}			
		}//end of finally		
		
		
		
		return bldgIds;
		
	}	
	
	public int getActionPlanMpvId(Connection conn,int tmPeriodId, int metricPerId, int blgdId)
	{
		PreparedStatement ps = null;
		ResultSet rs = null;
		String SQL ="select mtrc_period_val_id val_id  from mtrc_metric_period_value where tm_period_id = ? and mtrc_period_id = ? and dsc_mtrc_lc_bldg_id = ? ";
		int valueId = 0;
		
		try
		{
			ps = conn.prepareStatement(SQL);
			ps.setInt(1,tmPeriodId);
			rs = ps.executeQuery();
			while(rs.next())
			{
				valueId = rs.getInt("val_id");
			}
			
		}//end of try
		catch(Exception e)
		{
			e.printStackTrace();
			valueId = -1;
		}
		finally
		{
			if(rs !=null)
			{
				try 
				{
					rs.close();
				} 
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}
			if(ps!=null)
			{
				try 
				{
					ps.close();
				}
				catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}			
		}//end of finally		
		return valueId;		
	}
}
