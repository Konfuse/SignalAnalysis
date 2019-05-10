package com.hust;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.DMBaseUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    private static final String JDBC_DRIVER = "dm.jdbc.driver.DmDriver";
    private static final String DB_URL = "jdbc:dm://ypt.dameng.com:30033";
    private static final String USER = "U_13109539713";
    private static final String PASS = "8drVKOqWNq";

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject;
        String target_name;
        double rcs, height;
        String sql = "SELECT t1.TARGET_TYPE, t1.TARGET_RCS, t1.TARGET_HEIGHT "
                + "FROM DUCT_TARGET_PARA t1 ";
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Connecting to DM database...");
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
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
    }
}