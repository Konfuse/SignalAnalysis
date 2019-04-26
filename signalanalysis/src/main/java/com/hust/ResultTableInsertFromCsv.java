package com.hust;

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
    public static String tableName = "";
    public static void main(String[] args) {
        String path = "/home/test/Documents/data/EvaporationWaveFake.csv";
        Connection conn = null;
        BufferedReader reader;
        String[] item, columns;
        String line, row;
        int lon, lat, year, month, day, hour;
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

        try {
            reader = new BufferedReader(new FileReader(path));
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
                        String.format("%04d", year)
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
                row = "every_year_average" + String.format("%03d", lon) + "," + String.format("%03d", lat);
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
                row = "every_month_average" + String.format("%03d", lon) + "," + String.format("%03d", lat);
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
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            conn = HBaseUtil.init();
            //travel heat year map
            for (String key : heatYearMap_BDGD.keySet()) {
                for (String s : heatYearMap_BDGD.get(key).keySet()) {
                    heatYearMap_BDGD.get(key).replace(s, heatYearMap_BDGD.get(key).get(s) / heatYearMap_Num.get(key).get(s));
                    heatYearMap_BDQD.get(key).replace(s, heatYearMap_BDQD.get(key).get(s) / heatYearMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, "heat_year_average", "bdgd", key, heatYearMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, "heat_year_average", "bdqd", key, heatYearMap_BDQD.get(key).toString());
            }
            //travel heat month map
            for (String key : heatMonthMap_BDGD.keySet()) {
                for (String s : heatMonthMap_BDGD.get(key).keySet()) {
                    heatMonthMap_BDGD.get(key).replace(s, heatMonthMap_BDGD.get(key).get(s) / heatMonthMap_Num.get(key).get(s));
                    heatMonthMap_BDQD.get(key).replace(s, heatMonthMap_BDQD.get(key).get(s) / heatMonthMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, "heat_month_average", "bdgd", key, heatMonthMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, "heat_month_average", "bdqd", key, heatMonthMap_BDQD.get(key).toString());
            }
            //travel every year average
            for (String key : yearAvgMap_BDGD.keySet()) {
                for (String s : yearAvgMap_BDGD.get(key).keySet()) {
                    yearAvgMap_BDGD.get(key).replace(s, yearAvgMap_BDGD.get(key).get(s) / yearAvgMap_Num.get(key).get(s));
                    yearAvgMap_BDQD.get(key).replace(s, yearAvgMap_BDQD.get(key).get(s) / yearAvgMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", yearAvgMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", yearAvgMap_BDQD.get(key).toString());
            }
            //travel every month average
            for (String key : monthAvgMap_BDGD.keySet()) {
                for (String s : monthAvgMap_BDGD.get(key).keySet()) {
                    monthAvgMap_BDGD.get(key).replace(s, monthAvgMap_BDGD.get(key).get(s) / monthAvgMap_Num.get(key).get(s));
                    monthAvgMap_BDQD.get(key).replace(s, monthAvgMap_BDQD.get(key).get(s) / monthAvgMap_Num.get(key).get(s));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", monthAvgMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", monthAvgMap_BDQD.get(key).toString());
            }
            //travel year probability
            for (String key : yearProbMap_BDGD.keySet()) {
                for (int s : yearProbMap_BDGD.get(key).keySet()) {
                    yearProbMap_BDGD.get(key).replace(s, yearProbMap_BDGD.get(key).get(s) / yearProbMap_Num.get(key));
                    yearProbMap_BDQD.get(key).replace(s, yearProbMap_BDQD.get(key).get(s) / yearProbMap_Num.get(key));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", yearProbMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", yearProbMap_BDQD.get(key).toString());
            }
            //travel month probability
            for (String key : monthProbMap_BDGD.keySet()) {
                for (int s : monthProbMap_BDGD.get(key).keySet()) {
                    monthProbMap_BDGD.get(key).replace(s, monthProbMap_BDGD.get(key).get(s) / monthProbMap_Num.get(key));
                    monthProbMap_BDQD.get(key).replace(s, monthProbMap_BDQD.get(key).get(s) / monthProbMap_Num.get(key));
                }
                HBaseUtil.insertData(conn, tableName, key, "bdgd", "value", monthProbMap_BDGD.get(key).toString());
                HBaseUtil.insertData(conn, tableName, key, "bdqd", "value", monthProbMap_BDQD.get(key).toString());
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
