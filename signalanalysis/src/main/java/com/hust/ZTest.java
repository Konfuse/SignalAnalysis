package com.hust;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStamp = Calendar.getInstance();

        Map<String, Double> map = new HashMap<>();
        map.put("1", 1.1);
        map.put("2", 2.2);
        System.out.println(map.toString());
        JSONObject jsonObject = new JSONObject();
    }
}