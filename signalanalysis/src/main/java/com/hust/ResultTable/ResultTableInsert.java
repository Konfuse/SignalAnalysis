package com.hust.ResultTable;

import com.hust.EvaporationWaveTable.EvaporationWaveTableQuery;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 2019/4/24 15:55
 */
public class ResultTableInsert {
    public static void main(String[] args) {
        Connection conn = null;
        String row, result;
        String[] types = {"bdgd", "bdqd"};
        int[] hours = {0, 6, 12, 18};
        EvaporationWaveTableQuery query = new EvaporationWaveTableQuery();

        try {
            System.out.println("start transfer table...");
            conn = HBaseUtil.init();
            row = "heat_year_average";
            //insert year heat map query with hour
            //row.format(heat_year_average)
            //column.format(YYYY-hh)
            System.out.println("process year heat map with hour...");
            for (int year = 2008; year < 2018; year++) {
                for (String type : types) {
                    for (int hour : hours) {
                        result = query.queryHeatMapByDate(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.YEAR, year, hour);
                        HBaseUtil.insertData(conn, "result_of_evaporation", row, type, String.format("%04d", year) + "-" + String.format("%02d", hour), result);
                    }
                }
            }
            //insert year heat map without hour
            //row.format(heat_year_average)
            //column.format(YYYY)
            System.out.println("process year heat map without hour...");
            for (int year = 2008; year < 2018; year++) {
                for (String type : types) {
                    result = query.queryHeatMapByDate(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.YEAR, year);
                    HBaseUtil.insertData(conn, "result_of_evaporation", row, type, String.format("%04d", year), result);
                }
            }

            row = "heat_month_average";
            //insert month heat map with hour
            //row.format(heat_month_average)
            //column.format(mm-hh)
            System.out.println("process month heat map with hour...");
            for (int month = 1; month < 13; month++) {
                for (String type : types) {
                    for (int hour : hours) {
                        result = query.queryHeatMapByDate(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.MONTH, month, hour);
                        HBaseUtil.insertData(conn, "result_of_evaporation", row, type, String.format("%02d", month) + "-" + String.format("%02d", hour), result);
                    }
                }
            }
            //insert month heat map without hour
            //row.format(heat_month_average)
            //column.format(mm)
            System.out.println("process month heat map without hour...");
            for (int month = 1; month < 13; month++) {
                for (String type : types) {
                    result = query.queryHeatMapByDate(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.MONTH, month);
                    HBaseUtil.insertData(conn, "result_of_evaporation", row, type, String.format("%02d", month), result);
                }
            }

            row = "every_year_average";
            //insert every year average
            //row.format(every_year_average:xxx,yyy)
            //column.format(value)
            System.out.println("process every year average...");
            for (String type : types) {
                for (int lon = 100; lon <= 150; lon++) {
                    for (int lat = 0; lat <= 50; lat++) {
                        result = query.queryEveryYearAverage(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), lon, lat);
                        HBaseUtil.insertData(conn, "result_of_evaporation", row + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat), type, "value", result);
                    }
                }
            }


            row = "every_month_average";
            //insert every month average
            //row.format(every_month_average:xxx,yyy)
            //column.format(value)
            System.out.println("process every month average...");
            for (String type : types) {
                for (int lon = 100; lon <= 150; lon++) {
                    for (int lat = 0; lat <= 50; lat++) {
                        result = query.queryEveryMonthAverage(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), lon, lat);
                        HBaseUtil.insertData(conn, "result_of_evaporation", row + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat), type, "value", result);
                    }
                }
            }

            row = "year_probability";
            //insert year probability
            //row.format(year_probability:YYYY:xxx,yyy)
            //column.format(value)
            System.out.println("process year year probability...");
            for (int year = 2008; year < 2018; year++) {
                for (String type : types) {
                    for (int lon = 100; lon <= 150; lon++) {
                        for (int lat = 0; lat <= 50; lat++) {
                            result = query.queryProbability(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.YEAR, year, lon, lat);
                            HBaseUtil.insertData(conn, "result_of_evaporation", row + ":" + String.format("%04d", year) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat), type, "value", result);
                        }
                    }
                }
            }

            row = "month_probability";
            //insert month probability
            //row.format(month_probability:mm:xxx,yyy)
            //column.format(value)
            System.out.println("process month probability...");
            for (int month = 1; month < 13; month++) {
                for (String type : types) {
                    for (int lon = 100; lon <= 150; lon++) {
                        for (int lat = 0; lat <= 50; lat++) {
                            result = query.queryProbability(EvaporationWaveTableQuery.ValueType.valueOf(type.toUpperCase()), EvaporationWaveTableQuery.DateType.MONTH, month, lon, lat);
                            HBaseUtil.insertData(conn, "result_of_evaporation", row + ":" + String.format("%02d", month) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat), type, "value", result);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                HBaseUtil.closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
