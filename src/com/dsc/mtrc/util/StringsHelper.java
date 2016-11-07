package com.dsc.mtrc.util;

import java.util.ArrayList;
import java.util.Arrays;

public class StringsHelper {

	/*
	 * takes in array list of strings and returns a string in the following format: 
	 * 'str1','str2','str3','str4'	 
	 */
	
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
	
	/*
	 * accepts comma separated list of strings and converts it to Array list 	 
	 */
	public static ArrayList<String> stringToArray(String input)
	{
		
		ArrayList<String> list = new ArrayList<String>(Arrays.asList(input.split(",")));		
		return list;
	}
}
