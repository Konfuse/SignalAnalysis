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

    public String predict(PredictionType predictionType, int year, int month, int day, int lon, int lat) {
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

    public double predictTheDay(PredictionType predictionType, int lon, int lat) {
        Connection connection = null;
        String type, row, date;
        double value = -1.0;

        //check type of value
        if (predictionType == PredictionType.BDGD) {
            type = "bdgd";
        } else {
            type = "bdqd";
        }

        // get current time
        // if MM-dd equals 02-29, replace with 02-28
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM-dd");
        date = simpleDateFormat.format(new Date());
        if ("02-29".equals(date)) {
            date = "02-28";
        }

        try {
            connection = HBaseUtil.init();
            row = "2019-" + date + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
            String valueStr = HBaseUtil.getCellData(connection, tableName, row, "wave", type);
            if (valueStr != null) {
                value = Double.parseDouble(valueStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    public String predictAllPosition(PredictionType predictionType, int year, int month, int day) {
        JSONObject jsonObject = new JSONObject();
        String type, regex, row = null, position, value = null;
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

        regex = String.format("%04d", year)
                + "-"
                + String.format("%02d", month)
                + "-"
                + String.format("%02d", day)
                + ".*";

        List<Result> resultList = HBaseUtil.getDataByRegex(tableName, regex);
        //travel result sets, convert value to json string, and push into a list
        for (Result result : resultList) {
            for (Cell cell : result.listCells()) {
                //if find the correct type, break and use the value
                row = Bytes.toString(CellUtil.cloneRow(cell));
                if ((Bytes.toString(CellUtil.cloneQualifier(cell))).equals(type)) {
                    value = Bytes.toString(CellUtil.cloneValue(cell));
                    break;
                }
            }
            if (row == null)
                continue;
            //Calculation
            position = row.substring(row.indexOf(":") + 1);
            jsonObject.put(position, Double.parseDouble(value));
        }
        return jsonObject.toJSONString();
    }
}
