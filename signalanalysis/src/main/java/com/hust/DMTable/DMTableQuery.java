package com.hust.DMTable;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.DMBaseUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @Author: Konfuse
 * @Date: 2019/5/1 21:50
 */
public class DMTableQuery {
    private static String absPath = "C:/Users/Konfuse/Desktop/DM/";

//    public static void main(String[] args) {
//        DMTableQuery query = new DMTableQuery();
//        query.getRadarParamOnCsv("平台1", "366");
//        query.getQxtkDataOnCsv("平台1");
//        query.getSwqxDataOnCsv("平台1");
//        query.getZslDataOnCsv("平台1");
//    }

    public void start() {
        querySystemParamOnCsv();
        queryTargetParamOnCsv();
        queryJamParamOnCsv();
    }

    public String querySystemParamOnCsv() {
        String path = absPath + "DUCT_PTCS.csv";
        BufferedReader reader;
        String[] items;
        String line, sys_name;
        double lon, lat;
        JSONObject jsonObject;
        List<JSONObject> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
//            System.out.println(reader.readLine());
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                jsonObject = new JSONObject();
                items = line.split(",");
                sys_name = items[1];
                lon = Double.parseDouble(items[12]);
                lat = Double.parseDouble(items[13]);
                jsonObject.put("name", sys_name);
                jsonObject.put("position", lon + "-" + lat);
                jsonObject.put("radars", getRadarNameOnCsv(sys_name));
                list.add(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(list.toString());
        return list.toString();
    }

    public String querySystemParam() {
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject;
        String sys_name;
        double lon, lat;
        String sql = "SELECT t1.PTXH, t1.LON, t1.LAT FROM DUCT_PTCS t1 ORDER BY t1.PTXH";
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                jsonObject = new JSONObject();
                sys_name = resultSet.getString(1);
                lon = resultSet.getDouble(2);
                lat = resultSet.getDouble(3);
                jsonObject.put("name", sys_name);
                jsonObject.put("position", lon + "-" + lat);
                jsonObject.put("radars", getRadarName(sys_name));
                list.add(jsonObject);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(list.toString());
        return list.toString();
    }

    public String queryTargetParamOnCsv() {
        String path = absPath + "DUCT_TARGET_PARA.csv";
        BufferedReader reader;
        String[] items;
        String line, target_name;
        double rcs, height;
        JSONObject jsonObject;
        List<JSONObject> list = new ArrayList<>();
        try{
            reader = new BufferedReader(new FileReader(path));
//            System.out.println(reader.readLine());
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                jsonObject = new JSONObject();
                items = line.split(",");
                target_name = items[0];
                rcs = Double.parseDouble(items[1]);
                height = Double.parseDouble(items[2]);
                jsonObject.put("name", target_name);
                jsonObject.put("rcs", rcs);
                jsonObject.put("height", height);
                list.add(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(list.toString());
        return list.toString();
    }

    public String queryTargetParam() {
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject;
        String target_name;
        double rcs, height;
        String sql = "SELECT t1.TARGET_TYPE, t1.TARGET_RCS, t1.TARGET_HEIGHT "
                + "FROM DUCT_TARGET_PARA t1 ";
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                jsonObject = new JSONObject();
                target_name = resultSet.getString(1);
                rcs = resultSet.getDouble(2);
                height = resultSet.getDouble(3);
                jsonObject.put("name", target_name);
                jsonObject.put("rcs", rcs);
                jsonObject.put("height", height);
                list.add(jsonObject);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(list.toString());
        return list.toString();
    }

    public String queryJamParamOnCsv() {
        String path = absPath + "DUCT_JAM_PARA.csv";
        BufferedReader reader;
        String[] items;
        String line, jam_name;
        double lon, lat, power, gain, height, loss;
        JSONObject jsonObject;
        List<JSONObject> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(path));
//            System.out.println(reader.readLine());
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                jsonObject = new JSONObject();
                items = line.split(",");
                jam_name = items[0];
                power = Double.parseDouble(items[1]);
                gain = Double.parseDouble(items[2]);
                height = Double.parseDouble(items[3]);
                loss = Double.parseDouble(items[4]);
                lon = Double.parseDouble(items[5]);
                lat = Double.parseDouble(items[6]);
                jsonObject.put("name", jam_name);
                jsonObject.put("power", power);
                jsonObject.put("gain", gain);
                jsonObject.put("loss", loss);
                jsonObject.put("height", height);
                jsonObject.put("position", lon + "-" + lat);
                list.add(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(list.toString());
        return list.toString();
    }

    public String queryJamParam() {
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject;
        String jam_name;
        double lon, lat, power, gain, height, loss;
        String sql = "SELECT t1.JAM_TYPE, t1.JAM_PT, t1.JAM_ANTGAIN, t1.JAM_ANTHEIGHT, t1.SYSLOSS, t1.LON, t1.LAT "
                + "FROM DUCT_JAM_PARA t1 ";
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                jsonObject = new JSONObject();
                jam_name = resultSet.getString(1);
                power = resultSet.getDouble(2);
                gain = resultSet.getDouble(3);
                height = resultSet.getDouble(4);
                loss = resultSet.getDouble(5);
                lon = resultSet.getDouble(6);
                lat = resultSet.getDouble(7);
                jsonObject.put("name", jam_name);
                jsonObject.put("power", power);
                jsonObject.put("gain", gain);
                jsonObject.put("loss", loss);
                jsonObject.put("height", height);
                jsonObject.put("position", lon + "-" + lat);
                list.add(jsonObject);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(list.toString());
        return list.toString();
    }

    public double[] getSystemPositionOnCsv(String sys_name) {
        String path = absPath + "DUCT_PTCS.csv";
        BufferedReader reader;
        String[] items;
        String line;
        double[] position = {-999.0, -999.0};
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
//            System.out.println(reader.readLine());
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[1].equals(sys_name)) {
                    position[0] = Double.parseDouble(items[12]);
                    position[1] = Double.parseDouble(items[13]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(Arrays.toString(position));
        return position;
    }

    public double[] getSystemPosition(String sys_name) {
        double[] position = new double[2];
        String sql = "SELECT t1.LON, t1.LAT FROM DUCT_PTCS t1 WHERE t1.PTXH = '" + sys_name + "'";
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                position[0] = resultSet.getDouble(2);
                position[1] = resultSet.getDouble(3);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(Arrays.toString(position));
        return position;
    }

    public List<String> getRadarNameOnCsv(String sys_name) {
        String path = absPath + "DUCT_RADAR_PARA.csv";
        BufferedReader reader;
        String[] items;
        String line;
        List<String> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
//            System.out.println(reader.readLine());
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[0].equals(sys_name)) {
                    list.add(items[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(list.toString());
        return list;
    }

    public List<String> getRadarName(String sys_name) {
        List<String> list = new ArrayList<>();
        String sql = "SELECT t1.RADAR_NAME "
                + "FROM DUCT_RADAR_PARA t1 "
                + "WHERE t1.SYS_NAME = '" + sys_name + "'";
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(list.toString());
        return list;
    }

    public String[] getRadarParamOnCsv(String sys_name, String radar_name) {
        String path = absPath + "DUCT_RADAR_PARA.csv";
        String[] items, params;
        BufferedReader reader;
        String line;
        int rowCount = 0;
        List<String> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[0].equals(sys_name) && items[1].equals(radar_name)) {
                    rowCount++;
                    list.addAll(Arrays.asList(items));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getRadarParam(String sys_name, String radar_name) {
        String[] params;
        List<String> list = new ArrayList<>();
        int rowCount = 0;
        String sql = "SELECT t1.SYS_NAME, t1.RADAR_NAME, t1.FREQ, t1.PT, t1.PULSE, t1.SYSLOSS, t1.FN, t1.ANT_BEAM, "
                + "t1.ANT_GAIN, t1.ANT_ELEV, t1.ANT_HEIGHT, t1.ANT_TYPE, t1.POLAR_MODE, t1.IPULSE, t1.XJGL, t1.PD "
                + "FROM DUCT_RADAR_PARA t1 "
                + "WHERE t1.SYS_NAME='" + sys_name + "' AND t1.RADAR_NAME='" + radar_name + "'";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                rowCount++;
                for (int i = 1; i <= 16; i++) {
                    list.add(resultSet.getString(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getQxtkDataOnCsv(String sys_name) {
        String path = absPath + "DUCT_QXTK_DATA.csv";
        BufferedReader reader;
        String[] items, params;
        String line;
        int rowCount = 0;
        List<String> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[9].equals(sys_name)) {
                    rowCount++;
                    items[6] = items[6].replace(";", ",");
                    items[7] = items[7].replace(";", ",");
                    items[8] = items[8].replace(";", ",");
                    list.add(items[9]);
                    for (int i = 0; i < 9; i++) {
                        list.add(items[i]);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getQxtkData(String sys_name) {
        String[] params;
        List<String> list = new ArrayList<>();
        int rowCount = 0;
        String sql = "SELECT t1.DATA_DT, t1.SITEHEIGHT, t1.SITELON, t1.SITELAT, t1.WINDSPEED, "
                + "t1.LAYER_NUM, t1.PRESS_LAYERS, t1.TEMP_LAYERS, t1.HUM_LAYERS, t1.PTXH "
                + "FROM DUCT_QXTK_DATA t1 "
                + "WHERE t1.PTXH='" + sys_name + "'";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                rowCount++;
                list.add(resultSet.getString(10));
                for (int i = 1; i <= 9; i++) {
                    list.add(resultSet.getString(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getSwqxDataOnCsv(String sys_name) {
        String path = absPath + "DUCT_SWQX_DATA.csv";
        BufferedReader reader;
        String[] items, params;
        String line;
        int rowCount = 0;
        List<String> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[8].equals(sys_name)) {
                    rowCount++;
                    list.add(items[8]);
                    for (int i = 0; i < 8; i++) {
                        list.add(items[i]);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getSwqxData(String sys_name) {
        String[] params;
        List<String> list = new ArrayList<>();
        int rowCount = 0;
        String sql = "SELECT t1.DATA_DT, t1.HEIGHT, t1.TEMP, t1.SEATEMP, "
                + "t1.HUM, t1.PRESS, t1.WINDSPEED, t1.WINDDIR, t1.PTXH "
                + "FROM DUCT_SWQX_DATA t1 "
                + "WHERE t1.PTXH='" + sys_name + "'";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                rowCount++;
                list.add(resultSet.getString(9));
                for (int i = 1; i <= 8; i++) {
                    list.add(resultSet.getString(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getZslDataOnCsv(String sys_name) {
        String path = absPath + "DUCT_ZSL_DATA.csv";
        BufferedReader reader;
        String[] items, params;
        String line;
        int rowCount = 0;
        List<String> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "GBK"));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                if (items[5].equals(sys_name)) {
                    rowCount++;
                    items[3] = items[3].replace(";", ",");
                    items[4] = items[4].replace(";", ",");
                    list.add(items[5]);
                    for (int i = 0; i < 5; i++) {
                        list.add(items[i]);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }

    public String[] getZslData(String sys_name) {
        String[] params;
        List<String> list = new ArrayList<>();
        int rowCount = 0;
        String sql = "SELECT t1.DATA_DT, t1.WINDSPEED, t1.LAYER_NUM, "
                + "t1.HEIGHT_LAYERS, t1.MVALUE_LAYERS, t1.PTXH "
                + "FROM DUCT_ZSL_DATA t1 "
                + "WHERE t1.PTXH='" + sys_name + "'";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DMBaseUtil.init();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                rowCount++;
                list.add(resultSet.getString(6));
                for (int i = 1; i <= 5; i++) {
                    list.add(resultSet.getString(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DMBaseUtil.closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (rowCount == 1) {
            params = list.toArray(new String[0]);
            System.out.println(Arrays.toString(params));
            return params;
        } else {
            return new String[0];
        }
    }
}
