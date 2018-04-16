package com.lsw.other;

import java.text.ParseException;
import java.text.SimpleDateFormat;  
import java.util.ArrayList;  
import java.util.Calendar;  
import java.util.Date;  
import java.util.List; 
public class DayList {

	/* 
	 * To change this license header, choose License Headers in Project Properties. 
	 * To change this template file, choose Tools | Templates 
	 * and open the template in the editor. 
	 */  
	  
	/** 
	 * 
	 * @author njchenyi 
	 */  
	    public static void main(String[] args) throws Exception {  
	        Calendar cal = Calendar.getInstance();  
	        String start = "2013/12/01";  
	        String end = "2014/03/02";  
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");  
//	        Date dBegin = sdf.parse(start);  
//	        Date dEnd = sdf.parse(end);  
	        List<Date> lDate = findDates(start, end);  
	        for (Date date : lDate) {  
	            System.out.println(sdf.format(date));  
	        }  
	    }  
	  
	    public static List<Date> findDates(String startdate, String stopdate) {  
	    	String s = startdate.replace('/', '-');
			String e = stopdate.replace('/', '-');
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date dBegin = null, dEnd = null;
			try {
				dBegin = sdf.parse(s);
				dEnd = sdf.parse(e);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	    	List lDate = new ArrayList();  
	        lDate.add(dBegin);  
	        Calendar calBegin = Calendar.getInstance();  
	        // 使用给定的 Date 设置此 Calendar 的时间    
	        calBegin.setTime(dBegin);  
	        Calendar calEnd = Calendar.getInstance();  
	        // 使用给定的 Date 设置此 Calendar 的时间    
	        calEnd.setTime(dEnd);  
	        // 测试此日期是否在指定日期之后    
	        while (dEnd.after(calBegin.getTime())) {  
	            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量    
	            calBegin.add(Calendar.DAY_OF_MONTH, 1);  
	            lDate.add(calBegin.getTime());  
	        }  
	        return lDate;  
	    }  
	

}
