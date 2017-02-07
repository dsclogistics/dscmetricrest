package com.dsc.mtrc.util;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/*
 * @author:RMA 
 * This class contains a set of static methods to make manipulations with date and time objects 
 * */
public class DateHelper {
	public static String getPrevMonthBegDate(){

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, -1);// add -1 month to current month
        calendar.set(Calendar.DATE, 1);// set DATE to 1, so first date of previous month
        Date firstDateOfPreviousMonth = calendar.getTime();
        return  formatter.format(firstDateOfPreviousMonth);
    }//end getPrevMonthBegDate

    public static String getPrevMonthEndDate(){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, -1);// add -1 month to current month
        calendar.set(Calendar.DATE,calendar.getActualMaximum(Calendar.DAY_OF_MONTH));// set actual maximum date of previous month
        Date lastDateOfPreviousMonth = calendar.getTime();
        return  formatter.format(lastDateOfPreviousMonth);

    }//end getPrevMonthEndDate
    
    //This method returns the first day of the month in the yyyy-MM-dd format based on the passed month and year 
    public static String getMonthFirstDay(String month, String year){    	
    	
 	   Calendar cal = Calendar.getInstance();
 	   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
 	   try{
 		   
 		   Date date = new SimpleDateFormat("MMMM, yyyy").parse(month+", "+year);
 		   cal.setTime(date);
 		   Date firstDateOfMonth = cal.getTime();
 		   return formatter.format(firstDateOfMonth);
 	   }
 	   catch(Exception e){return"Invalid Month";}		 	   	   
     }//end of getMonthFirstDay
    
    
  //This method returns the last day of the month in the yyyy-MM-dd format based on the passed month and year    
    public static String getMonthLastDay(String month, String year){    	
    	
 	   Calendar cal = Calendar.getInstance();
 	   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
 	   try{
 		   
 		   Date date = new SimpleDateFormat("MMMM, yyyy").parse(month+", "+year);
 		   cal.setTime(date);
 		   cal.set(Calendar.DATE,cal.getActualMaximum(Calendar.DAY_OF_MONTH));
 		   Date lastDateOfMonth = cal.getTime();
 		   return formatter.format(lastDateOfMonth);
 	   }
 	   catch(Exception e){return"Invalid Month";}		 	   	   
     }//end of getMonthLastDay


}
