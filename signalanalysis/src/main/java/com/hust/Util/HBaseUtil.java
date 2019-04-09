package com.hust.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 19-04-09 下午04:17
 */
public class HBaseUtil {
    public static Connection init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

    public static void closeAll(Admin admin, Connection conn) throws IOException {
        if (admin != null) {
            admin.close();
        }

        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public static void closeAll(Connection conn) throws IOException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public static void createTable(String tableName, String[] columnFamilies) {
        TableName table = TableName.valueOf(tableName);
        Admin admin = null;
        Connection conn = null;
        try {
            conn = init();
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                System.out.println("table exists!");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
                for (String columnFamily: columnFamilies) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(admin, conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertData(String tableName, String row, String columnFamily, String column, String data) {
        Put put = new Put(row.getBytes());
        put.add(columnFamily.getBytes(), column.getBytes(), data.getBytes());
        Connection conn = null;
        try {
            conn = init();
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertData(Connection conn, String tableName, String row, String columnFamily, String column, String data) throws IOException {
        Put put = new Put(row.getBytes());
        put.add(columnFamily.getBytes(), column.getBytes(), data.getBytes());
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(put);
    }

    public static void getUnDealData(String tableName) {
        Connection conn = null;
        try {
            conn = init();
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result: resultScanner) {
                System.out.println("scan: " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static Result getDataByRow(String tableName, String row) {
        Connection conn = null;
        Result result = null;
        try {
            conn = init();
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(row.getBytes());
            if (!get.isCheckExistenceOnly()) {
                result = table.get(get);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public static String getCellData(String tableName, String row, String columnFamily, String column) {
        Connection conn = null;
        try {
            conn = init();
            Table table = conn.getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(row.getBytes());
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                result = Bytes.toString(resByte);
                return result;
            } else {
                return "the result doesn't exist!";
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "error!";
    }

    public static String getCellData(Connection conn, String tableName, String row, String columnFamily, String column) throws IOException {
        String result = null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(row.getBytes());
        if (get.isCheckExistenceOnly()) {
            return "the result doesn't exist!";
        } else {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            Result res = table.get(get);
            byte[] resByte = res.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            result = Bytes.toString(resByte);
            return result;
        }
    }

    public static void deleteByRow(String tableName, String row) {
        Connection conn = null;
        try {
            conn = init();
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(row));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void deleteTable(String tableName) {
        Connection conn = null;
        Admin admin = null;
        try {
            TableName table = TableName.valueOf(tableName);
            conn = init();
            admin = conn.getAdmin();
            admin.disableTable(table);
            admin.deleteTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                closeAll(admin, conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
