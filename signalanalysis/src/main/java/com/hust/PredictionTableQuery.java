package com.hust;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

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

    public String preditByDate(PredictionType predictionType, int year, int month, int day, int lon, int lat) {
        JSONObject jsonObject = new JSONObject();
        String type, row = null, value = null;
        String[] date;

        //judge type of value
        if (predictionType == PredictionType.BDGD) {
            type = "bdgd";
        } else {
            type = "bdqd";
        }

        //create regex for querying
        String regex = String.format("%04d", year)
                + String.format("%02d", month)
                + String.format("%2d", day)
                + "-[\\d]{2}:"
                + String.format("%03d", lon)
                + ","
                + String.format("%03d", lat);
        List<Result> resultList = HBaseUtil.getDataByRegex(tableName, regex);

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
            jsonObject.put("prediction", Double.parseDouble(value));
        }

        return jsonObject.toJSONString();
    }
}
