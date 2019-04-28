package com.hust.PredictionTable;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

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
        String type, row = null, value = null, startTime = null, endTime = null;
        String start, end;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStamp = Calendar.getInstance();

        //check year
        if (year != 2019) {
            return null;
        }

        //judge type of value
        if (predictionType == PredictionType.BDGD) {
            type = "bdgd";
        } else {
            type = "bdqd";
        }

        //find start time and end time
        try {
            startTime = String.format("%04d", year) + "-" + String.format("%02d", month) + "-" + String.format("%02d", day);
            calendarStamp.setTime(simpleDateFormat.parse(startTime));
            calendarStamp.add(Calendar.DAY_OF_YEAR, 6);
            if (simpleDateFormat.format(calendarStamp.getTime()).compareTo("2019-12-31") > 0) {
                endTime = "2019-12-31";
            } else {
                endTime = simpleDateFormat.format(calendarStamp.getTime());
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //create regex for querying
        String regex = "[\\d]{4}"
                + "-"
                + "[\\d]{2}"
                + "-"
                + "[\\d]{2}:"
                + String.format("%03d", lon)
                + ","
                + String.format("%03d", lat);
        start = startTime + ":"
                + String.format("%03d", lon)
                + ","
                + String.format("%03d", lat);
        end = endTime + ":"
                + String.format("%03d", lon)
                + ","
                + String.format("%03d", lat);
        List<Result> resultList = HBaseUtil.getDataByFilter(tableName, start, end, regex);

        for (Result result : resultList) {
            for (Cell cell : result.listCells()) {
                //if find the correct type, break and use the value
                if ((Bytes.toString(CellUtil.cloneQualifier(cell))).equals(type)) {
                    row = Bytes.toString(CellUtil.cloneRow(cell));
                    value = Bytes.toString(CellUtil.cloneValue(cell));
                    break;
                }
            }
            if (row == null)
                continue;
            jsonObject.put(row.substring(0, row.indexOf(":")), Double.parseDouble(value));
        }

        return jsonObject.toJSONString();
    }
}
