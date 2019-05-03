package com.hust.Util;

import java.sql.*;

/**
 * @Author: Konfuse
 * @Date: 2019/5/1 20:11
 */
public class DMBaseUtil {
    private static final String JDBC_DRIVER = "dm.jdbc.driver.DmDriver";
    private static final String DB_URL = "jdbc:dm://168.1.0.185:5236";
    private static final String USER = "DUCT";
    private static final String PASS = "123456789";

    public static Connection init() throws Exception {
        Class.forName(JDBC_DRIVER);
        System.out.println("Connecting to DM database...");
        Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println("Connected database successfully...");
        return connection;
    }

    public static void closeAll(Connection connection, Statement statement) throws SQLException {
        if (connection != null)
            connection.close();

        if (statement != null)
            statement.close();
    }

    public static ResultSet query(String sql) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = init();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(connection, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultSet;
    }
}
