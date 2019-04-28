package com.hust.PredictionTable;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: Konfuse
 * @Date: 2019/4/22 17:59
 */
public class PredictionTableQuery {
    private static String tableName = "prediction";
    public static enum PredictionType {
        BDGD,
        BDQD
    }

    public String predit(PredictionType predictionType, int year, int month, int day, int lon, int lat) {
        JSONObject jsonObject = new JSONObject();
        String type, startTime, date, row;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStamp = Calendar.getInstance();
        Calendar calendarEnd = Calendar.getInstance();
        Connection connection = null;

        //check year
        if (year != 2019) {
            return null;
        }

        //check type of value
        if (predictionType == PredictionType.BDGD) {
            type = "bdgd";
        } else {
            type = "bdqd";
        }

        //find start time and end time
        try {
            startTime = String.format("%04d", year) + "-" + String.format("%02d", month) + "-" + String.format("%02d", day);
            calendarStamp.setTime(simpleDateFormat.parse(startTime));
            calendarEnd.setTime(simpleDateFormat.parse("2020-01-01"));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            connection = HBaseUtil.init();
            //travel prediction result
            for (int i = 0; i < 7 && !calendarStamp.equals(calendarEnd); i++) {
                date = simpleDateFormat.format(calendarStamp.getTime());
                row = date + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
                jsonObject.put(date, HBaseUtil.getCellData(connection, tableName, row, "wave", type));
                calendarStamp.add(Calendar.DAY_OF_YEAR, 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                HBaseUtil.closeAll(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return jsonObject.toJSONString();
    }
}
