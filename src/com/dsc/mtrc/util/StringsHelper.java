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

	public static String isGoalMet(String value,String mpgLessVal,  String mpgLessEqVal,String mpgEqualVal, String mpgGreaterVal,String mpgGreaterEqVal ){
		
		if(value==null||value.trim().isEmpty()||value.equals("N/A"))return "X";
		else if(mpgEqualVal!=null)//checking equal condition
		{
			if(Double.parseDouble(value) == Double.parseDouble(mpgEqualVal))return "Y";
			else return "N";
				
		}
		else if(mpgLessVal!=null&&mpgGreaterVal==null&&mpgGreaterEqVal==null)// checking < (less than) only condition{
		{
			if(Double.parseDouble(value) < Double.parseDouble(mpgLessVal))return "Y";
			else return "N";
		}
		else if(mpgLessEqVal!=null&&mpgGreaterVal==null&&mpgGreaterEqVal==null)//checking <= only condition 
		{
			if(Double.parseDouble(value) <=Double.parseDouble(mpgLessEqVal))return "Y";
			else return "N";
		}
		else if(mpgGreaterVal!=null&&mpgLessVal==null&&mpgLessEqVal==null)//checking > only condition
		{
			if(Double.parseDouble(value) > Double.parseDouble(mpgGreaterVal))return "Y";
			else return "N";
		}
		else if(mpgGreaterEqVal!=null&&mpgLessVal==null&&mpgLessEqVal==null)//checking >= only condition
		{
			if(Double.parseDouble(value) > Double.parseDouble(mpgGreaterEqVal))return "Y";
			else return "N";
		}
		//checking for ranges now
		else if(mpgLessVal!=null&&mpgGreaterVal!=null)//range for less/greater than values
		{
			if(Double.parseDouble(mpgLessVal)>Double.parseDouble(mpgGreaterVal))//inside range
			{
				if(Double.parseDouble(value)>Double.parseDouble(mpgGreaterVal)&&Double.parseDouble(value)<Double.parseDouble(mpgLessVal))
					return "Y";
					else return"N";
			}
			else//outside range
			{
				if(Double.parseDouble(value)>Double.parseDouble(mpgGreaterVal)||Double.parseDouble(value)<Double.parseDouble(mpgLessVal))
					return "Y";
					else return"N";
			}
		}
		else if(mpgLessVal!=null&&mpgGreaterEqVal!=null)//range for less/greater equal than values
		{
			if(Double.parseDouble(mpgLessVal)>Double.parseDouble(mpgGreaterEqVal))//inside range
			{
				if(Double.parseDouble(value)>=Double.parseDouble(mpgGreaterEqVal)&&Double.parseDouble(value)<Double.parseDouble(mpgLessVal))
					return "Y";
					else return"N";
			}
			else//outside range
			{
				if(Double.parseDouble(value)>=Double.parseDouble(mpgGreaterEqVal)||Double.parseDouble(value)<Double.parseDouble(mpgLessVal))
					return "Y";
					else return"N";
			}
		}
		else if(mpgLessEqVal!=null&&mpgGreaterVal!=null)//range for less equal/greater than values
		{
			if(Double.parseDouble(mpgLessEqVal)>Double.parseDouble(mpgGreaterVal))//inside range
			{
				if(Double.parseDouble(value)>Double.parseDouble(mpgGreaterVal)&&Double.parseDouble(value)<=Double.parseDouble(mpgLessEqVal))
					return "Y";
					else return"N";
			}
			else//outside range
			{
				if(Double.parseDouble(value)>Double.parseDouble(mpgGreaterVal)||Double.parseDouble(value)<=Double.parseDouble(mpgLessVal))
					return "Y";
					else return"N";
			}
		}
		else if(mpgLessEqVal!=null&&mpgGreaterEqVal!=null)//range for less equal/greater equal than values
		{
			if(Double.parseDouble(mpgLessEqVal)>Double.parseDouble(mpgGreaterEqVal))//inside range
			{
				if(Double.parseDouble(value)>=Double.parseDouble(mpgGreaterEqVal)&&Double.parseDouble(value)<=Double.parseDouble(mpgLessEqVal))
					return "Y";
					else return"N";
			}
			else//outside range
			{
				if(Double.parseDouble(value)>=Double.parseDouble(mpgGreaterEqVal)||Double.parseDouble(value)<=Double.parseDouble(mpgLessEqVal))
					return "Y";
					else return"N";
			}
		}
		else return "N";
		
	}
}
