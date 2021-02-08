/*
 * Copyright (C) 2021 The UserBehaviorAnalysis Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivan.analysis.util;

import org.joda.time.DateTime;
import org.joda.time.DateTime.Property;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 日期时间工具类。
 */
public class DateUtils {
	public static final String MONTH = "yyyy-MM";
	public static final String DAY = "yyyy-MM-dd";
	public static final String MINUTE = "yyyy-MM-dd HH:mm";
	public static final String HOUR_MINUTE = "HH:mm";
	public static final String HOUR_MINUTE_S = "HH:mm:ss";
	public static final String SECOND = "yyyy-MM-dd HH:mm:ss";
	public static final String SECOND_GMT = "EEE MMM dd yyyy hh:mm:ss z";
	public static final String SECOND_CST = "EEE MMM dd HH:mm:ss z yyyy";
	public static final String MILLISECOND = "yyyy-MM-dd HH:mm:ss SSSS";
	public static final String MILLISECOND_1 = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String MILLISECOND_2 = "yyyy-MM-dd HH:mm:ss.SS";
	public static final String MILLISECOND_3 = "yyyy-MM-dd HH:mm:ss.S";
	public static final String MILLISECOND_4 = "yyyy-MM-dd HH:mm:ss.SSSS";
	public static final String MILLISECOND_T = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	public static final String MONTH_N = "yyyyMM";
	public static final String DAY_N = "yyyyMMdd";
	public static final String MINUTE_N = "yyyyMMddHHmm";
	public static final String SECOND_N = "yyyyMMddHHmmss";
	public static final String MILLISECOND_N = "yyyyMMddHHmmssSSSS";
	public static final String PDAY="yyyy/MM/dd";
	public static final String LOG_DAY="dd/MM/yyyy:HH:mm:ss";
	public static final String PAGODA_WEEK_BEGIN="pagodaWeekBegin";
	public static final String PAGODA_WEEK_END="pagodaWeekEnd";
	public static final String PAGODA_WEEK_REAL_END="pagodaWeekRealEnd";
	/**
	 * 每天开始时间
	 */
	public final static String TIME_START = "00:00:00";

	/**
	 * 每天截止时间
	 */
	public final static String TIME_END = "23:59:59";

	public static final String[] FORMATS = new String[] { DAY, MONTH, MINUTE,
			SECOND, MILLISECOND, MILLISECOND_1, MILLISECOND_2, MILLISECOND_3, MILLISECOND_4, DAY_N, MONTH_N, MINUTE_N, SECOND_N,
			MILLISECOND_N, SECOND_CST,LOG_DAY };

	/**
	 * 将字符串解析成Date对象。<br>
	 * 该方法尝试用[yyyy-MM/yyyy-MM-dd/ yyyy-MM-dd HH:mm/yyyy-MM-dd
	 * HH:mm:ss/yyyy-MM-dd HH:mm:ss SSSS/ yyyyMM/yyyyMMdd/yyyyMMddHHmm/
	 * yyyyMMddHHmmss/yyyyMMddHHmmssSSSS]格式进行解析，如果无法解析将抛出异常。
	 * 
	 * @param str
	 *            字符串
	 * @return 返回对应的Date对象。
	 */
	public static Date parse(String str) {
		String pattern = getDateFormat(str);
		return parse(str, pattern);
	}

	/**
	 * 获取当天日期字符串
	 * @return
	 */
	public static String getDays(){
		Date today=getToday();
		return format(today);
	}

	/**
	 * 获取当前详细时间
	 * @return
	 */
	public static String getNow(){
		return LocalDateTime.now().toString();
	}


	public static String getTimestamp(){

		return ""+System.currentTimeMillis();
	}

	public static String getNowymd(){
		return format(getToday(), DAY);
	}
	public static String getNowTimestamp(){
		return format(getDateTime().toDate(), MILLISECOND);
	}
	public static String getNowymdNoSplit(){
		return format(getToday(), "yyyyMMdd");
	}


	public static Integer getLastYear(){
		Calendar calendar= Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.YEAR, -1);
		return  calendar.get(Calendar.YEAR);
	}
	public static Integer getNowYear(){
		Calendar calendar= Calendar.getInstance();
		calendar.setTime(new Date());

		return  calendar.get(Calendar.YEAR);
	}
	/**
	 * 获取前一天
	 * @return
	 */
	public static String getYesterdayYMD(){
		Calendar calendar= Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		return  dateFormat.format(calendar.getTime());
	}
	public static String getNowYM(){
		return format(getToday(), "yyyyMM");
	}
	public static String getNowPreYM(){
		Calendar calendar= Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.MONTH, -1);
		return format(calendar.getTime(), "yyyyMM");
	}

	public static String getNowPreYMByDash(){
		Calendar calendar= Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.MONTH, -1);
		return format(calendar.getTime(), "yyyy-MM");
	}


	/**
	 * 获取前一天
	 * @return
	 */
	public static String getYesterday(){
		Calendar calendar= Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		return  dateFormat.format(calendar.getTime());
	}



	/**
	 * 将指定格式的字符串解析成Date对象。
	 * 
	 * @param str
	 *            字符串
	 * @param format
	 *            格式
	 * @return 返回对应的Date对象。
	 */
	public static Date parse(String str, String format) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
		return DateTime.parse(str, formatter).toDate();
	}
	/**
	 * 将指定格式的字符串解析成Date对象。
	 *
。
	 */
	public static String parseToStr(String str,String firstFormat ,String lastFormat) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(firstFormat);
		Date dt= DateTime.parse(str, formatter).toDate();
		return format(dt, lastFormat);
	}


	/**
	 * 将Date对象解析成yyyy-MM-dd格式的字符串。
	 * 
	 * @param date
	 *            Date对象
	 * @return 返回yyyy-MM-dd格式的字符串。
	 */
	public static String format(Date date) {
		return format(date, DAY);
	}

	/**
	 * 将Date对象解析成指定格式的字符串。
	 * 
	 * @param date
	 *            Date对象
	 * @param pattern
	 *            格式
	 * @return 返回指定格式的字符串。
	 */
	public static String format(Date date, String pattern) {
		return new DateTime(date).toString(pattern);
	}

	/**
	 * 获取字符串的日期格式。如果字符串不在[{@link #MONTH}/{@link #DAY}/ {@link #MINUTE} /
	 * {@link #SECOND}/ {@link #MILLISECOND} ]格式范围内将抛出异常。
	 * 
	 * @param str
	 *            字符串
	 * @return 返回字符串的日期格式。
	 */
	public static String getDateFormat(String str) {
		for (String format : FORMATS) {
			if (isDate(str, format)) {
				return format;
			}
		}
		throw new IllegalArgumentException("不支持的日期格式：" + str);
	}

	/**
	 * 判断字符串是否为日期格式的字符串。
	 * 
	 * @param str
	 *            字符串
	 * @return 如果是日期格式的字符串返回true，否则返回false。
	 */
	public static Boolean isDate(String str) {
		for (String format : FORMATS) {
			if (isDate(str, format)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 判断字符串是否为指定日期格式的字符串。
	 * 
	 * @param str
	 *            字符串
	 * @param format
	 *            日期格式
	 * @return 如果是指定日期格式的字符串返回true，否则返回false。
	 */
	public static Boolean isDate(String str, String format) {
		try {
			parse(str, format);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 获取当前日期（只取到日期，时间部分都为0）。
	 * 
	 * @return 返回当前日期。
	 */
	public static Date getToday() {
		return DateTime.now().toLocalDate().toDate();
	}

	public static  Date getNowDetail(){
		return getDateTime().toDate();
	}

	public static DateTime getDateTime(){
		return  new DateTime();
	}


	/**
	 * 获取当前日期前一天日期。
	 * 
	 * @return 返回当前日期前一天日期。
	 */
	public static Date getPrevDay() {
		return getPrevDay(getToday());
	}

	/**
	 * 获取当前日期前几天日期。
	 * 
	 * @param days
	 *            天数
	 * @return 返回当前日期前几天日期。
	 */
	public static Date getPrevDay(Integer days) {
		return getPrevDay(getToday(), days);
	}

	/**
	 * 获取指定日期前一天日期。
	 * 
	 * @param date
	 *            指定日期
	 * @return 返回指定日期前一天日期。
	 */
	public static Date getPrevDay(Date date) {
		return getPrevDay(date, 1);
	}

	/**
	 * 获取指定日期前几天日期。
	 * 
	 * @param date
	 *            指定日期
	 * @param days
	 *            天数
	 * @return 返回指定日期前几天日期。
	 */
	public static Date getPrevDay(Date date, Integer days) {
		return new DateTime(date).minusDays(days).toLocalDate().toDate();
	}

	/**
	 * 获取当前日期后一天日期（只取到日期，时间部分都为0）。
	 * 
	 * @return 返回后一天日期。
	 */
	public static Date getNextDay() {
		return getNextDay(getToday());
	}

	/**
	 * 获取当前日期后几天日期。
	 * 
	 * @param days
	 *            天数
	 * @return 返回当前日期后几天日期。
	 */
	public static Date getNextDay(Integer days) {
		return getNextDay(getToday(), days);
	}

	/*
	 * 获取指定日期后一天日期。
	 * 
	 * @param date
	 *            指定日期
	 * @return 返回指定日期的后一天日期。
	 */
	public static Date getNextDay(Date date) {
		return getNextDay(date, 1);
	}

	/**
	 * 获取指定日期后几天日期。
	 * 
	 * @param date
	 *            指定日期
	 * @param days
	 *            天数
	 * @return 返回指定日期后几天日期。
	 */
	public static Date getNextDay(Date date, Integer days) {
		return new DateTime(date).plusDays(days).toLocalDate().toDate();
	}

	/**
	 * 获取Joda Time的Duration对象。
	 * 
	 * @param beginDate
	 *            开始日期
	 * @param endDate
	 *            结束日期
	 * @return 返回Joda Time的Duration对象。
	 */
	public static Duration getDuration(Date beginDate, Date endDate) {
		return new Duration(new DateTime(beginDate), new DateTime(endDate));
	}

	/**
	 * 获取Joda Time的Peroid对象。
	 * 
	 * @param beginDate
	 *            开始日期
	 * @param endDate
	 *            结束日期
	 * @return 返回Joda Time的Peroid对象。
	 */
	public static Period getPeriod(Date beginDate, Date endDate) {
		return new Period(new DateTime(beginDate), new DateTime(endDate));
	}


	/**
	 * 获取两个日期之间相差的天数
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static Integer getPeriodBetweenDays(String beginDate, String endDate) {
		if(StringUtils.isEmpty(beginDate)||StringUtils.isEmpty(endDate)){
			return null;
		}
		if(!isDate(beginDate)||!isDate(endDate)){
			return null;
		}
		return java.time.Period.between(LocalDate.parse(beginDate),LocalDate.parse(endDate)).getDays()+1;
	}
	/**
	 * 获取两个日期之间相差的月
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static Integer getPeriodBetweenMonths(String beginDate, String endDate) {
		if(StringUtils.isEmpty(beginDate)||StringUtils.isEmpty(endDate)){
			return null;
		}
		if(!isDate(beginDate)||!isDate(endDate)){
			return null;
		}
		return java.time.Period.between(LocalDate.parse(beginDate),LocalDate.parse(endDate)).getMonths();
	}



	/**
	 * 获取两个日期之间相差的年数
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static Integer getPeriodBetweenYears(String beginDate, String endDate) {
		if(StringUtils.isEmpty(beginDate)||StringUtils.isEmpty(endDate)){
			return null;
		}
		if(!isDate(beginDate)||!isDate(endDate)){
			return null;
		}
		return java.time.Period.between(LocalDate.parse(beginDate),LocalDate.parse(endDate)).getYears();
	}
	/**
	 * 获取Joda Time的Interval对象。
	 * 
	 * @param beginDate
	 *            开始日期
	 * @param endDate
	 *            结束日期
	 * @return 返回Joda Time的Interval对象。
	 */
	public static Interval getInterval(Date beginDate, Date endDate) {
		return new Interval(new DateTime(beginDate), new DateTime(endDate));
	}

	/**
	 * 在某个时间加上分钟
	 * 
	 * @param dateTime
	 *            开始时间
	 * @param minute
	 *            增加分钟数
	 * @return 返回新增分钟后的时间。
	 */
	public static Date addMinute(Date dateTime, int minute) {
		return new DateTime(dateTime).plusMinutes(minute).toDate();
	}

	/**
	 * 在某个时间加上天
	 * 
	 * @param dateTime
	 *            开始时间
	 * @param day
	 *            增加天数
	 * @return 返回新增天数后的时间。
	 */
	public static Date addDay(Date dateTime, int day) {
		return new DateTime(dateTime).plusDays(day).toDate();
	}

	/**
	 * 在某个时间加上月
	 * 
	 * @param dateTime
	 *            开始时间
	 * @param month
	 *            增加月数
	 * @return 返回新增月数后的时间。
	 */
	public static Date addMonth(Date dateTime, int month) {
		return new DateTime(dateTime).plusMonths(month).toDate();
	}

	/**
	 * 将时间日期变量的小时变为指定时
	 * 
	 * @param date
	 *            日期时间变量
	 * @param amount
	 *            指定时
	 * @return 修改后的日期变量
	 */
	public static Date setHour(Date date, int amount) {
		Property hourPro = new DateTime(date).hourOfDay();
		return hourPro.setCopy(amount).toDate();
	}

	/**
	 * 将时间日期变量的分钟变为指定分
	 * 
	 * @param date
	 *            日期时间变量
	 * @param amount
	 *            指定分
	 * @return 修改后的日期变量
	 */
	public static Date setMinute(Date date, int amount) {
		Property minutePro = new DateTime(date).minuteOfHour();
		return minutePro.setCopy(amount).toDate();
	}


	/**
	 * 获取前14天的开始时间
	 *
	 * @return
	 */
	public static String getStartOfLastDay() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -15);
		String time = formatDate(calendar.getTime()) + " " + TIME_START;
		return time;
	}

	/**
	 * 获取前一天的结束时间
	 *
	 * @return
	 */
	public static String getEndOfLastDay() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		String time = formatDate(calendar.getTime()) + " " + TIME_END;
		return time;
	}

	/**
	 * 格式为时间字符串
	 *
	 * @param date 日期
	 * @return yyyy-MM-dd Date
	 */
	public static String formatDate(Date date) {
		try {
			return DateUtils.formatDate(date, DateUtils.DAY);
		} catch (Exception e) {
			return null;
		}
	}


	/**
	 * 按指定格式字符串格式时间
	 *
	 * @param date 日期或者时间
	 * @param pattern 格式化字符串 yyyy-MM-dd， yyyy-MM-dd HH:mm:ss, yyyy年MM月dd日 etc.</br>
	 * @return
	 */
	public static String formatDate(Date date, String pattern) {
		if(null == date){
			return "";
		}
		SimpleDateFormat format = new SimpleDateFormat(pattern.trim());
		return format.format(date);
	}



	public static int getDateHour(Date date) {
		return new DateTime(date).getHourOfDay();
	}

	public static int getDateHour(String date) {
		return DateUtils.getDateHour(DateUtils.parse(date, SECOND));
	}

	public static int getDateYear(Date date) {
		return new DateTime(date).getYear();
	}

	public static int getDateYear(String date) {
		return DateUtils.getDateYear(DateUtils.parse(date, SECOND));
	}

	public static int getDateMonth(String date) {
		return DateUtils.getDateMonth(DateUtils.parse(date, SECOND));
	}

	public static int getDateMonth(Date date) {
		return new DateTime(date).getMonthOfYear();
	}

	/**
	 * 将时间日期变量的秒变为指定秒
	 * 
	 * @param date
	 *            日期时间变量
	 * @param amount
	 *            指定秒
	 * @return 修改后的日期变量
	 */
	public static Date setSecond(Date date, int amount) {
		Property secondPro = new DateTime(date).secondOfMinute();
		return secondPro.setCopy(amount).toDate();
	}

	/**
	 * 取指定日期所在月的第一天的日期
	 * 
	 * @param date
	 *            指定的日期
	 * @return 指定日期所在月的第一天
	 */
	public static Date getFirstDayOfMonth(Date date) {
		Property dayPro = new DateTime(date).dayOfMonth();
		Integer min = dayPro.getMinimumValue();
		return dayPro.setCopy(min).toDate();
	}

	/**
	 * 取指定日期所在月的最后一天的日期
	 * 
	 * @param date
	 *            指定的日期
	 * @return 指定日期所在月的最后一天
	 */
	public static Date getLastDayOfMonth(Date date) {
		Property dayPro = new DateTime(date).dayOfMonth();
		Integer max = dayPro.getMaximumValue();
		return dayPro.setCopy(max).toDate();
	}

	/**
	 * 转换 EEE MMM dd yyyy hh:mm:ss z 格式字符串为时间字符串
	 * 
	 * @param str
	 *            时间字符串
	 * @return yyyy-MM-dd HH:mm:ss 格式时间字符串
	 */
	public static String getDateOfGMT(String str) {
		SimpleDateFormat sf = new SimpleDateFormat(SECOND_GMT, Locale.ENGLISH);
		String format = "";
		try {
			StringBuffer s = new StringBuffer(str);
			str = s.insert(s.indexOf("+") + 3, ":")
					.substring(0, s.length() - 6);
			format = DateUtils.format(sf.parse(str), SECOND);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return format;
	}

	public static String getZDateString(String str) {
		return DateUtils.getTDateString(str.substring(0, str.length() - 5));
	}

	public static String getTDateString(String str) {
		return DateUtils.format(DateUtils.parse(str.replace("T", " "), SECOND),
				SECOND);
	}

	public static String formatCST(String str) {
		SimpleDateFormat sdf1 = new SimpleDateFormat(DateUtils.SECOND_CST,
				Locale.ENGLISH);
		String format = "";
		try {
			format = DateUtils.format(sdf1.parse(str), DateUtils.SECOND);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return format;
	}

	public static String convertMiniToString(int time, boolean mil) {
		String first = "小时";
		String second = "分钟";
		if (mil) {
			first = "分钟";
			second = "秒";
		}
		int hour = time / 60;
		int remain = time % 60;
		if (time / 60 == 0) {
			return remain + second;
		} else {
			return hour + first + remain + second;
		}
	}



	/**
	 * 获取同比日期
	 * @param time
	 * @return
	 */
	public static  String getLastYearSameDate(String time ){
		Date now=parse(time, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		calendar.add(Calendar.YEAR, -1);
		Date result=calendar.getTime();
		if(result!=null){
			return format(result, DAY);
		}
		return null;
	}



	/**
	 * 获取日环比（上一周同一个星期）
	 * @param yesterday
	 * @return
	 */
	public static String getDayOnDay(String yesterday){
		Date now=parse(yesterday, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		calendar.add(Calendar.DAY_OF_WEEK, -6);
		Date result=calendar.getTime();
		if(result!=null){
			return format(result, DAY);
		}
		return null;
	}

	/**
	 * 获取当前日期所在的百果园周
	 * 百果园周，每周四到下周三为一个百果园周
	 * @param yesterday
	 * @return
	 */
	public static Map<String,String> getPagodaBusiWeek(String yesterday){
		Map<String,String> resultMap=new HashMap<>();
		Date now=parse(yesterday, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		int dayOfWeek=calendar.get(Calendar.DAY_OF_WEEK);

		Date result=calendar.getTime();
		if(1<dayOfWeek&&dayOfWeek<=4){
			if(dayOfWeek==4&&result!=null){//星期三
				 resultMap.put(PAGODA_WEEK_END,format(result, DAY));

			}else if(dayOfWeek<4) {
				calendar.add(Calendar.DAY_OF_WEEK, 4 - dayOfWeek);
				result = calendar.getTime();
				if (result != null) {
					resultMap.put(PAGODA_WEEK_END, format(result, DAY));
				}
			}
			calendar.add(Calendar.DAY_OF_WEEK, -6);
			result=calendar.getTime();
			if(result!=null){
				resultMap.put(PAGODA_WEEK_BEGIN,format(result, DAY));
			}
		}else {
			if(dayOfWeek==5){//星期四
				resultMap.put(PAGODA_WEEK_BEGIN,format(result, DAY));
			}else{
				if(dayOfWeek==1){
					calendar.add(Calendar.DAY_OF_WEEK, -3);
				}else{
					calendar.add(Calendar.DAY_OF_WEEK, 5-dayOfWeek);
				}

				result = calendar.getTime();
				if (result != null) {
					resultMap.put(PAGODA_WEEK_BEGIN, format(result, DAY));
				}
			}
			calendar.add(Calendar.DAY_OF_WEEK, +6);
			result=calendar.getTime();
			if(result!=null){
				resultMap.put(PAGODA_WEEK_END,format(result, DAY));
			}
		}


		return resultMap;
	}

	/***
	 * 百果园周同比
	 * 获取历史同一个时期的百果园周-同比数据
	 * --------------去年相同日期所在的业务周
	 * @param currentYesterday
	 * @return
	 */
	public static Map<String,String>  getLastYearSamePagodaWeek(String currentYesterday){
		String lastYesterday=getLastYearSameDate(currentYesterday);

		return getPagodaBusiWeek(lastYesterday);
	}

	/***
	 * 百果园周环比比
	 * 获取历史同一个时期的百果园周-同比数据
	 * 例如今天周二，统计周期为上周四-周二，和上一个周四-周二对比
	 * @param time
	 * @return
	 */
	public static Map<String,String>  getWeekOnWeekPagodaWeek(String time){
		/**
		Map<String,String> nowWeekMap= getPagodaBusiWeek(time);
		String beginTime=nowWeekMap.get(PAGODA_WEEK_BEGIN);
		Date beginTimeDt=parse(beginTime, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(beginTimeDt);
		calendar.add(Calendar.DAY_OF_YEAR, -1);
		String resultEndTime=format(calendar.getTime(), DAY);
		Map<String,String> resutlMap=getPagodaBusiWeek(resultEndTime);
		return resutlMap;
		 **/
		Date now=parse(time, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		calendar.add(Calendar.DAY_OF_WEEK, -7);
		String endTime=format(calendar.getTime(), DAY);
		Map<String,String> map=getPagodaBusiWeek(endTime);
		map.put(PAGODA_WEEK_REAL_END,map.get(PAGODA_WEEK_END));
		map.put(PAGODA_WEEK_END, endTime);

		return map;
	}

	/**
	 * 获取当前日期所在的月份的第一天和最后一天
	 * @param yesterday
	 * @return
	 */
	public static Map<String,String> getMonthBeginEndTime(String yesterday){
		Date dt=parse(yesterday, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(dt);
		calendar.set(Calendar.DATE, 1);
		Map<String,String> map=new HashMap<>();
		map.put(PAGODA_WEEK_BEGIN, format(calendar.getTime(), DAY));
		calendar.roll(Calendar.DATE,-1 );
		map.put(PAGODA_WEEK_END, format(calendar.getTime(), DAY));
		return map;
	}

	/**
	 * 月份同比
	 * 获取当前日期的去年同一个日期所在的月份范围
	 * @param yesterday
	 * @return
	 */
	public static Map<String,String> getLastMonthBeginEndTime(String yesterday){
		String lastYesterday=getLastYearSameDate(yesterday);
		return getMonthBeginEndTime(lastYesterday);
	}

	/**
	 * 获取月份环比
	 * @param yesterday
	 * @return
	 */
	public static Map<String,String> getMonthOnMonthBeginEndTIme(String yesterday){
		 Date now=parse(yesterday,DAY);
		 Calendar calendar=Calendar.getInstance();
		 calendar.setTime(now);
		 calendar.add(Calendar.MONTH, -1);
		 String endTime=format(calendar.getTime(),DAY);

		 Map<String,String> map=getMonthBeginEndTime(endTime);
		 map.put(PAGODA_WEEK_REAL_END, map.get(PAGODA_WEEK_END));
		 map.put(PAGODA_WEEK_END,endTime);
		 return map;
	}

	/**
	 * 获取当前时间所在年份的第一天
	 * @param time
	 * @return
	 */
	public  static  String getYearFirstDay(String time){
		Date now=parse(time, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		int year=calendar.get(Calendar.YEAR);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);

		String beginTime=format(calendar.getTime(), DAY);
		return beginTime;
	}

	/**
	 * 获取当前时间所在年份的最后一天
	 * @param time
	 * @return
	 */
	public static String getYearLastDay(String time){
		Date now=parse(time, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		int year=calendar.get(Calendar.YEAR);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);
		calendar.roll(Calendar.DAY_OF_YEAR, -1);
		return format(calendar.getTime(), DAY);
	}
	/**
	 * 获取当前时间所在年的第一天和最后一天
	 * @param time
	 * @return
	 */
	public static  Map<String,String> getYearBeginEndTime(String time){
		Date now=parse(time, DAY);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(now);
		int year=calendar.get(Calendar.YEAR);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);

		String beginTime=format(calendar.getTime(), DAY);
		Map<String,String> map=new HashMap<>();
		map.put(PAGODA_WEEK_BEGIN, beginTime);
		calendar.roll(Calendar.DAY_OF_YEAR, -1);
		map.put(PAGODA_WEEK_END, format(calendar.getTime(), DAY));
		return map;
	}







}
