package com.hust;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStart = Calendar.getInstance();
        Calendar calendarEnd = Calendar.getInstance();
        Calendar calendarStamp = Calendar.getInstance();

        String start = String.format("%04d", 2019) + "-" + String.format("%02d", 12) + "-" + String.format("%02d", 29);
        calendarStamp.setTime(simpleDateFormat.parse(start));
        calendarStamp.add(Calendar.DAY_OF_YEAR, 1);
        System.out.println(simpleDateFormat.format(calendarStamp.getTime()).compareTo("2019-12-31"));
        if (simpleDateFormat.format(calendarStamp.getTime()).compareTo("2019-12-31") > 0)
            System.out.println(simpleDateFormat.format(calendarStamp.getTime()));
    }
}