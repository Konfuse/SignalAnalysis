package com.hust;

import com.hust.Util.HBaseUtil;

/**
 * @Author: Konfuse
 * @Date: 2019/4/24 18:19
 */
public class ResultTableQuery {
    private static String tableName = "result_of_evaporation";
    public static enum ValueType {
        BDGD,
        BDQD
    }

    public String getAllYearHeatMap(ValueType valueType) {
        String row = "heat_year_average";
        String column = "all";
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getAllYearHeatMap(ValueType valueType, int hour) {
        String row = "heat_year_average";
        String column = "all-" + String.format("%02d", hour);
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getYearHeatMap(ValueType valueType, int year, int hour) {
        String row = "heat_year_average";
        String column = String.format("%04d", year) + "-" + String.format("%02d", hour);
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getYearHeatMap(ValueType valueType, int year) {
        String row = "heat_year_average";
        String column = String.format("%04d", year);
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getMonthHeatMap(ValueType valueType, int month, int hour) {
        String row = "heat_month_average";
        String column = String.format("%02d", month) + "-" + String.format("%02d", hour);
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getMonthHeatMap(ValueType valueType, int month) {
        String row = "heat_month_average";
        String column = String.format("%02d", month);
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getEveryYearAverage(ValueType valueType, int lon, int lat) {
        String row = "every_year_average:" + String.format("%03d", lon) + "," + String.format("%03d", lat);
        String column = "value";
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getEveryMonthAverage(ValueType valueType, int lon, int lat) {
        String row = "every_month_average:" + String.format("%03d", lon) + "," + String.format("%03d", lat);
        String column = "value";
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getYearProbability(ValueType valueType, int year, int lon, int lat) {
        String row = "year_probability:" + String.format("%04d", year) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
        String column = "value";
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }

    public String getMonthProbability(ValueType valueType, int month, int lon, int lat) {
        String row = "month_probability:" + String.format("%02d", month) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
        String column = "value";
        return HBaseUtil.getCellData(tableName, row, valueType.toString().toLowerCase(), column);
    }
}
