package com.hust;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.hadoop.hbase.util.Hash;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static enum ValueType {
        BDGD,
        BDQD;
    }

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(simpleDateFormat.parse("2008-01-01"));
        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(simpleDateFormat.parse("2016-12-31"));
        long count = 0;
        while (!calendarStart.equals(calendarEnd)) {
            count++;
            System.out.println("Step: " + count);
            calendarStart.add(Calendar.DAY_OF_YEAR, 1);
        }
    }
}