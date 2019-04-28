package com.hust.LowAltitudeWaveTable;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 10:49
 */
public class LowAltitudeWaveTableQuery {
    private static String tableName = "lowaltitude";
    public static enum DuctType {
        SURFACE,
        SUSPENDED
    }
    public static enum DataType {
        SURFACE_GD,
        SURFACE_QD,
        SUSPENDED_DINGGAO,
        SUSPENDED_DIGAO,
        SUSPENDED_HD,
        SUSPENDED_QD
    }


    public String queryProbablyPerMonth(String siteID, DuctType ductType) {
        JSONObject jsonObject = new JSONObject();
        String regex, row = null, type, month;
        double dataCount, ductCount;

        if (ductType == DuctType.SURFACE) {
            type = "01";
        } else {
            type = "02";
        }

        regex = siteID + ":[\\d]{2}-[\\d]{2}:" + type;
        List<Result> resultList = HBaseUtil.getDataByRegex(tableName, regex);

        //travel result sets
        for (Result result : resultList) {
            dataCount = 0.0;
            ductCount = 0.0;
            for (Cell cell : result.listCells()) {
                //if find the correct type, break and use the value
                row = Bytes.toString(CellUtil.cloneRow(cell));
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("dataCount")) {
                    dataCount = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ductCount")) {
                    ductCount = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            if (row == null || dataCount == 0.0 || ductCount == 0.0)
                continue;
            //compute prob
            month = row.substring(0, row.indexOf("-")).split(":")[1];
            jsonObject.put(month, ductCount / dataCount);
        }

        return jsonObject.toJSONString();
    }

    public String queryMonthAverage(String siteID, DuctType ductType, DataType dataTpye) {
        JSONObject jsonObject = new JSONObject();
        String regex, row = null, type = null, column = null, month;
        double value = 0.0;
        HashMap<String, Double> mapSum = new HashMap<>();
        HashMap<String, Integer> mapNum = new HashMap<>();

        if (ductType == DuctType.SURFACE) {
            type = "01";
            switch (dataTpye) {
                case SURFACE_GD:
                    column = "layerdigaoAvg";
                    break;
                case SURFACE_QD:
                    column = "layerhouduAvg";
                    break;
                default:
                    return null;
            }
        } else if (ductType == DuctType.SUSPENDED) {
            type = "02";
            switch (dataTpye) {
                case SUSPENDED_DINGGAO:
                    column = "dinggaoAvg";
                    break;
                case SUSPENDED_DIGAO:
                    column = "digaoAvg";
                case SUSPENDED_HD:
                    column = "houduAvg";
                    break;
                case SUSPENDED_QD:
                    column = "qiangduAvg";
                    break;
                default:
                    return null;
            }
        }

        regex = siteID + ":[\\d]{2}-[\\d]{2}:" + type;
        List<Result> resultList = HBaseUtil.getDataByRegex(tableName, regex);

        //travel result sets
        for (Result result : resultList) {
            for (Cell cell : result.listCells()) {
                //if find the correct type, break and use the value
                if ((Bytes.toString(CellUtil.cloneQualifier(cell))).equals(column)) {
                    row = Bytes.toString(CellUtil.cloneRow(cell));
                    value = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                    break;
                }
            }
            if (row == null)
                continue;
            //compute average
            month = row.substring(0, row.indexOf("-")).split(":")[1];
            double finalValue = value;
            mapSum.compute(month, (k, v) -> {
                if (v == null)
                    return finalValue;
                return v + finalValue;
            });
            mapNum.compute(month, (k, v) -> {
                if (v == null)
                    return 1;
                return v + 1;
            });
        }

        for (String key : mapSum.keySet()) {
            jsonObject.put(key, mapSum.get(key) / mapNum.get(key));
        }

        return jsonObject.toJSONString();
    }
}
