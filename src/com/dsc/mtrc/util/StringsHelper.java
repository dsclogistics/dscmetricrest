package com.dsc.mtrc.util;

import java.util.ArrayList;
import java.util.Arrays;

public class StringsHelper {

	
	public static String arrayToInClause(ArrayList<String> input)
	{
		String output = "";
		for(String s:input )
		{
			output = output+"'"+s+"',";
		}
		output = output.substring(0, output.length()-1);
		
		return output;
	}
	public static ArrayList<String> stringToArray(String input)
	{
		
		ArrayList<String> list = new ArrayList<String>(Arrays.asList(input.split(",")));		
		return list;
	}
}