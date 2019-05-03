package com.hust.PredictionTable;

import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 2019/4/22 18:08
 */
public class PredictionTableInsert {
    public static void main(String[] args) {
        String[] paths = {
                "BDGDpredictions.csv",
                "BDQDpredictions.csv"
        };

        Connection conn = null;
        BufferedReader reader;
        String[] items, temp, message;
        String line, name, type;
        String row, lon, lat, year, month, day, value;

        try {
            conn = HBaseUtil.init();
            for (String path : paths) {
                reader = new BufferedReader(new FileReader(path));

                name = path.substring(path.lastIndexOf("/") + 1);
                if ("BDGDpredictions.csv".equals(name)) type = "bdgd";
                else type = "bdqd";

                System.out.println(reader.readLine());
                while ((line = reader.readLine()) != null) {
                    items = line.split(",");
                    message = items[0].split("-");
                    value = items[1];

                    year = message[0];
                    month = message[1];
                    day = message[2];
                    lon = message[3];
                    lat = message[4];
                    //row key format(yyyy-mm-dd-hh:xxx,yyy)
                    row = String.format("%04d", Integer.parseInt(year))
                            + "-"
                            + String.format("%02d", Integer.parseInt(month))
                            + "-"
                            + String.format("%02d", Integer.parseInt(day))
                            + ":"
                            + String.format("%03d", Integer.parseInt(lon))
                            + ","
                            + String.format("%03d", Integer.parseInt(lat));
                    System.out.println(row + ":" + value);
                    HBaseUtil.insertData(conn, "prediction", row, "wave", type, value);
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
