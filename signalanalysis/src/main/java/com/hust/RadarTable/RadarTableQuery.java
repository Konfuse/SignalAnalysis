package com.hust.RadarTable;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Konfuse
 * @Date: 2019/4/30 15:31
 */
public class RadarTableQuery {
    private static final String path = "C:/Users/Konfuse/Desktop/radar.csv";

    private static final String JDBC_DRIVER = "com.ddtek.jdbc.greenplum.GreenplumDriver";
    //IP段:端口号
    private static final String DB_URL = "jdbc:pivotal:greenplum://168.1.0.90:5432";

    //用户名和密码
    private static final String USER = "gpadmin";
    private static final String PASS = "";


    public String queryDistance(int lon1, int lat1, int lon2, int lat2, int type) {
        JSONObject jsonObject = new JSONObject();
        //if query file, set getDistanceMapPerHour on csv
        //else if query database, set getDistanceMapPerHour
        Map<String, Double> distanceMap = getDistanceMap(getDistanceMapPerHourOnCsv(lon1, lat1, lon2, lat2, type));

        //put into json object and return
        for (String s : distanceMap.keySet()) {
            jsonObject.put(s, distanceMap.get(s));
        }
        return jsonObject.toJSONString();
    }

    public String queryProbability(int lon1, int lat1, int lon2, int lat2, int type) {
        JSONObject jsonObject = new JSONObject();
        //if query file, set getDistanceMapPerHour on csv
        //else if query database, set getDistanceMapPerHour
        Map<String, Double> distanceMap = getDistanceMap(getDistanceMapPerHourOnCsv(lon1, lat1, lon2, lat2, type));

        Map<Integer, Double> distributionMap = new HashMap<>();
        long sum = 0;

        for (Double value : distanceMap.values()) {
            double finalValue = value;
            distributionMap.compute(((int)finalValue) / 50, (k, v) -> {
                if (v == null) return 1.0;
                return v + 1;
            });
            sum++;
        }

        for (Integer integer : distributionMap.keySet()) {
            jsonObject.put(String.valueOf(integer), distributionMap.get(integer) / sum);
        }

        return jsonObject.toJSONString();
    }

    public Map<String, Double> getDistanceMap(Map<String, Double> distancePerHour) {
        if (distancePerHour.size() <= 800) {
            //put into json object and return
            return distancePerHour;
        } else {
            //put into a day map
            Map<String, Double> distancePerDay = new HashMap<>();
            for (String s : distancePerHour.keySet()) {
                double distance = distancePerHour.get(s);
                s = s.substring(0, s.lastIndexOf("-"));
                distancePerDay.compute(s, (k, v) -> {
                    if (v == null) return distance;
                    else if (v < distance) return distance;
                    else return v;
                });
            }
            return distancePerDay;
        }
    }

    public double getMaxDistance(Map<String, Double> distancePerHour) {
        double maxDistance = 0.0;
        for (Double value : distancePerHour.values()) {
            maxDistance = value > maxDistance ? value : maxDistance;
        }
        return maxDistance;
    }

    public double getAvgDistance(Map<String, Double> distancePerHour) {
        int num = distancePerHour.size();
        double sum = 0.0;
        for (Double value : distancePerHour.values()) {
            sum += value;
        }
        if (num == 0)
            return 0;
        else return sum / num;
    }

    public Map<String, Double> getDistanceMapPerHour(int lon1, int lat1, int lon2, int lat2, int type) {
        Connection connection = null;
        Statement statement = null;
        String sql = "select track_origin.timeMark, track_origin.distance"
                + " from track_navi, track_origin"
                + " WHERE track_navi.longitude >= " + lon1
                + " and track_navi.longitude < " + lon2
                + " and track_navi.latitude >= " + lat1
                + " and track_navi.latitude < " + lat2
                + " and track_origin.timeMark = track_navi.timeMark"
                + " and track_origin.ship_no = track_navi.ship_no"
                + " and track_origin.source like '%" + type + "雷达%'"
                + " and track_origin.type like '%海上目标%'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
        Map<String, Double> distancePerHour = new HashMap<>();
        Long timestamp;
        String date;
        double distance;

        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Connecting to HAWQ database...");
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                timestamp = resultSet.getLong(1);
                distance = resultSet.getDouble(2);
                date = simpleDateFormat.format(new Date(timestamp * 1000));
                double finalDistance = distance;
                distancePerHour.compute(date, (k, v)->{
                    if (v == null) return finalDistance;
                    else if (v < finalDistance) return finalDistance;
                    else return v;
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return distancePerHour;
    }

    public Map<String, Double> getDistanceMapPerHourOnCsv(int lon1, int lat1, int lon2, int lat2, int type) {
        String[] items, position;
        int source;
        String line, date, time;
        double distance, lon, lat;
        Map<String, Double> distancePerHour = new HashMap<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(path));
            System.out.println(reader.readLine());
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                source = Integer.parseInt(items[0]);
                date = items[1];
                time = String.format("%04d", Integer.parseInt(items[2]));
                date = date.substring(0, 4)
                        + "-"
                        + date.substring(4, 6)
                        + "-"
                        + date.substring(6, 8)
                        + "-"
                        + time.substring(0, 2);

                distance = Double.parseDouble(items[3]);

                position = items[4].split("-");
                lon = Double.parseDouble(position[0]);
                lat = Double.parseDouble(position[1]);

//                System.out.println(source + ", " + date + ", " + time + ", " + distance + ", " + lon + ", " + lat);

                if (lon1 <= lon && lon2 >= lon && lat1 <= lat && lat2 >= lat && source == type) {
                    double finalDistance = distance;
                    distancePerHour.compute(date, (k, v)->{
                        if (v == null) return finalDistance;
                        else if (v < finalDistance) return finalDistance;
                        else return v;
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return distancePerHour;
    }
}
