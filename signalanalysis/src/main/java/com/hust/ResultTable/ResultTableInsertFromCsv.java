package com.hust.ResultTable;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Konfuse
 * @Date: 2019/4/26 9:25
 */
public class ResultTableInsertFromCsv {
    public static String tableName = "result_of_evaporation";
    public static void main(String[] args) {
        String path = "/home/test/Documents/data/EvaporationWaveFake.csv";
        Connection conn = null;
        BufferedReader reader;
        String[] item, columns;
        String line, row;
        int lon, lat, year, month, day, hour, count = 0;
        double bdgd, bdqd;
        Map<String, Map<String, Double>> heatYearMap_BDGD = new HashMap<>(), heatMonthMap_BDGD = new HashMap<>();
        Map<String, Map<String, Double>> heatYearMap_BDQD = new HashMap<>(), heatMonthMap_BDQD = new HashMap<>();
        Map<String, Map<String, Long>> heatYearMap_Num = new HashMap<>(), heatMonthMap_Num = new HashMap<>();
        Map<String, Map<String, Double>> yearAvgMap_BDGD = new HashMap<>(), monthAvgMap_BDGD = new HashMap<>();
        Map<String, Map<String, Double>> yearAvgMap_BDQD = new HashMap<>(), monthAvgMap_BDQD = new HashMap<>();
        Map<String, Map<String, Long>> yearAvgMap_Num = new HashMap<>(), monthAvgMap_Num = new HashMap<>();
        Map<String, Map<Integer, Double>> yearProbMap_BDGD = new HashMap<>(), monthProbMap_BDGD = new HashMap<>();
        Map<String, Map<Integer, Double>> yearProbMap_BDQD = new HashMap<>(), monthProbMap_BDQD = new HashMap<>();
        Map<String, Long> yearProbMap_Num = new HashMap<>(), monthProbMap_Num = new HashMap<>();
        JSONObject json_BDGD = new JSONObject(); JSONObject json_BDQD = new JSONObject();

        try {
            System.out.println("start import data from csv...");
            reader = new BufferedReader(new FileReader(path));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                item = line.split(",");
                if (item.length != 8)
                    continue;
                lon = Integer.parseInt(item[0]); lat = Integer.parseInt(item[1]);
                year = Integer.parseInt(item[2]); month = Integer.parseInt(item[3]);
                day = Integer.parseInt(item[4]); hour = Integer.parseInt(item[5]);
                bdgd = Double.parseDouble(item[6]); bdqd = Double.parseDouble(item[7]);
                //heat year map
                columns = new String[] {
                        String.format("%04d", year) + "-" + String.format("%02d", hour),
                        String.format("%04d", year),
                        "all",
                        "all-" + String.format("%02d", hour)
                };
                for (String column : columns) {
                    String finalJsonKey = String.format("%03d", lon) + "," + String.format("%03d", lat);
                    double finalBdgd = bdgd;
                    double finalBdqd = bdqd;
                    heatYearMap_BDGD.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Double> map = new HashMap<>();
                            map.put(finalJsonKey, finalBdgd);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return finalBdgd;
                            return value + finalBdgd;
                        });
                        return v;
                    });
                    heatYearMap_BDQD.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Double> map = new HashMap<>();
                            map.put(finalJsonKey, finalBdqd);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return finalBdqd;
                            return value + finalBdqd;
                        });
                        return v;
                    });
                    heatYearMap_Num.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Long> map = new HashMap<>();
                            map.put(finalJsonKey, 1L);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return 1L;
                            return value + 1L;
                        });
                        return v;
                    });
                }
                //heat month map
                columns = new String[] {
                        String.format("%02d", month) + "-" + String.format("%02d", hour),
                        String.format("%02d", month)
                };
                for (String column : columns) {
                    String finalJsonKey = String.format("%03d", lon) + "," + String.format("%03d", lat);
                    double finalBdgd = bdgd;
                    double finalBdqd = bdqd;
                    heatMonthMap_BDGD.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Double> map = new HashMap<>();
                            map.put(finalJsonKey, finalBdgd);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return finalBdgd;
                            return value + finalBdgd;
                        });
                        return v;
                    });
                    heatMonthMap_BDQD.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Double> map = new HashMap<>();
                            map.put(finalJsonKey, finalBdqd);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return finalBdqd;
                            return value + finalBdqd;
                        });
                        return v;
                    });
                    heatMonthMap_Num.compute(column, (k, v) -> {
                        if (v == null) {
                            Map<String, Long> map = new HashMap<>();
                            map.put(finalJsonKey, 1L);
                            return map;
                        }
                        v.compute(finalJsonKey, (key, value)->{
                            if (value == null) return 1L;
                            return value + 1L;
                        });
                        return v;
                    });
                }
                //every year average
                row = "every_year_average:" + String.format("%03d", lon) + "," + String.format("%03d", lat);
                String finalYearJsonKey = String.format("%04d", year);
                double finalBdgd = bdgd;
                double finalBdqd = bdqd;
                yearAvgMap_BDGD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Double> map = new HashMap<>();
                        map.put(finalYearJsonKey, finalBdgd);
                        return map;
                    }
                    v.compute(finalYearJsonKey, (key, value)->{
                        if (value == null) return finalBdgd;
                        return value + finalBdgd;
                    });
                    return v;
                });
                yearAvgMap_BDQD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Double> map = new HashMap<>();
                        map.put(finalYearJsonKey, finalBdqd);
                        return map;
                    }
                    v.compute(finalYearJsonKey, (key, value)->{
                        if (value == null) return finalBdqd;
                        return value + finalBdqd;
                    });
                    return v;
                });
                yearAvgMap_Num.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Long> map = new HashMap<>();
                        map.put(finalYearJsonKey, 1L);
                        return map;
                    }
                    v.compute(finalYearJsonKey, (key, value)->{
                        if (value == null) return 1L;
                        return value + 1L;
                    });
                    return v;
                });
                //every month average
                row = "every_month_average:" + String.format("%03d", lon) + "," + String.format("%03d", lat);
                String finalMonthJsonKey = String.format("%02d", month);
                monthAvgMap_BDGD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Double> map = new HashMap<>();
                        map.put(finalMonthJsonKey, finalBdgd);
                        return map;
                    }
                    v.compute(finalMonthJsonKey, (key, value)->{
                        if (value == null) return finalBdgd;
                        return value + finalBdgd;
                    });
                    return v;
                });
                monthAvgMap_BDQD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Double> map = new HashMap<>();
                        map.put(finalMonthJsonKey, finalBdqd);
                        return map;
                    }
                    v.compute(finalMonthJsonKey, (key, value)->{
                        if (value == null) return finalBdqd;
                        return value + finalBdqd;
                    });
                    return v;
                });
                monthAvgMap_Num.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<String, Long> map = new HashMap<>();
                        map.put(finalMonthJsonKey, 1L);
                        return map;
                    }
                    v.compute(finalMonthJsonKey, (key, value)->{
                        if (value == null) return 1L;
                        return value + 1L;
                    });
                    return v;
                });
                //year probability
                row = "year_probability:" + String.format("%04d", year) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
                yearProbMap_BDGD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<Integer, Double> map = new HashMap<>();
                        map.put(((int)finalBdgd) / 5, 1.0);
                        return map;
                    }
                    v.compute(((int)finalBdgd) / 5, (key, value)->{
                        if (value == null) return 1.0;
                        return value + 1;
                    });
                    return v;
                });
                yearProbMap_BDQD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<Integer, Double> map = new HashMap<>();
                        map.put(((int)finalBdqd) / 5, 1.0);
                        return map;
                    }
                    v.compute(((int)finalBdqd) / 5, (key, value)->{
                        if (value == null) return 1.0;
                        return value + 1;
                    });
                    return v;
                });
                yearProbMap_Num.compute(row, (k, v) -> {
                   if (v == null) return 1L;
                   return v + 1;
                });
                //month probability
                row = "month_probability:" + String.format("%02d", month) + ":" + String.format("%03d", lon) + "," + String.format("%03d", lat);
                monthProbMap_BDGD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<Integer, Double> map = new HashMap<>();
                        map.put(((int)finalBdgd) / 5, 1.0);
                        return map;
                    }
                    v.compute(((int)finalBdgd) / 5, (key, value)->{
                        if (value == null) return 1.0;
                        return value + 1;
                    });
                    return v;
                });
                monthProbMap_BDQD.compute(row, (k, v) -> {
                    if (v == null) {
                        Map<Integer, Double> map = new HashMap<>();
                        map.put(((int)finalBdqd) / 5, 1.0);
                        return map;
                    }
                    v.compute(((int)finalBdqd) / 5, (key, value)->{
                        if (value == null) return 1.0;
                        return value + 1;
                    });
                    return v;
                });
                monthProbMap_Num.compute(row, (k, v) -> {
                    if (v == null) return 1L;
                    return v + 1;
                });
                System.out.println("data " + (++count) + "has been processed..");
            }
            System.out.println("process all data.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            conn = HBaseUtil.init();
            //travel heat year map
            for (String key : heatYearMap_BDGD.keySet()) {
                for (String s : heatYearMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(s, heatYearMap_BDGD.get(key).get(s) / heatYearMap_Num.get(key).get(s));
                    json_BDQD.put(s, heatYearMap_BDQD.get(key).get(s) / heatYearMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, "heat_year_average", "bdgd", key, json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, "heat_year_average", "bdqd", key, json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("heat year map done...");

            //travel heat month map
            for (String key : heatMonthMap_BDGD.keySet()) {
                for (String s : heatMonthMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(s, heatMonthMap_BDGD.get(key).get(s) / heatMonthMap_Num.get(key).get(s));
                    json_BDQD.put(s, heatMonthMap_BDQD.get(key).get(s) / heatMonthMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, "heat_month_average", "bdgd", key, json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, "heat_month_average", "bdqd", key, json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("heat month map done...");

            //travel every year average
            for (String key : yearAvgMap_BDGD.keySet()) {
                for (String s : yearAvgMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(s, yearAvgMap_BDGD.get(key).get(s) / yearAvgMap_Num.get(key).get(s));
                    json_BDQD.put(s, yearAvgMap_BDQD.get(key).get(s) / yearAvgMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("every year average done...");

            //travel every month average
            for (String key : monthAvgMap_BDGD.keySet()) {
                for (String s : monthAvgMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(s, monthAvgMap_BDGD.get(key).get(s) / monthAvgMap_Num.get(key).get(s));
                    json_BDQD.put(s, monthAvgMap_BDQD.get(key).get(s) / monthAvgMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("every month average done...");

            //travel year probability
            for (String key : yearProbMap_BDGD.keySet()) {
                for (int s : yearProbMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(String.valueOf(s), yearProbMap_BDGD.get(key).get(s) / yearProbMap_Num.get(key));
                }
                for (int s : yearProbMap_BDQD.get(key).keySet()) {
                    json_BDQD.put(String.valueOf(s), yearProbMap_BDQD.get(key).get(s) / yearProbMap_Num.get(key));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("year probability done...");

            //travel month probability
            for (String key : monthProbMap_BDGD.keySet()) {
                for (int s : monthProbMap_BDGD.get(key).keySet()) {
                    json_BDGD.put(String.valueOf(s), monthProbMap_BDGD.get(key).get(s) / monthProbMap_Num.get(key));
                }
                for (int s : monthProbMap_BDQD.get(key).keySet()) {
                    json_BDQD.put(String.valueOf(s), monthProbMap_BDQD.get(key).get(s) / monthProbMap_Num.get(key));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", json_BDGD.toJSONString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", json_BDQD.toJSONString());
                json_BDGD.clear(); json_BDQD.clear();
            }
            System.out.println("month probability done...\nAll done.");
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
