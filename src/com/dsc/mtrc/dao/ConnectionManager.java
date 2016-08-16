package com.dsc.mtrc.dao;


import javax.naming.*;
import javax.sql.*;

public class ConnectionManager {
	
	private static DataSource mtrcDS = null;
	private static Context  context = null;

	
	public static DataSource mtrcConn() throws Exception
	{
		if (mtrcDS != null){
			return mtrcDS;
		}
		try{
			
			if (context == null){
				context = new InitialContext();
			}
		  mtrcDS = (DataSource) context.lookup("java:/comp/env/mtrcDS");
		}
		catch( Exception e) {
			e.printStackTrace();
		}
		return mtrcDS;
	}

}
