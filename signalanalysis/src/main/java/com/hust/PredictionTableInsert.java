package com.hust;

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
        String path = "/home/test/Documents/data/PredictionWaveFake.csv";

        Connection conn = null;
        BufferedReader reader;
        String[] item;
        String line;
        String row, lon, lat, year, month, day, bdgd, bdqd;

        try {
            conn = HBaseUtil.init();
            reader = new BufferedReader(new FileReader(path));
            System.out.println(reader.readLine());
            while ((line = reader.readLine()) != null) {
                item = line.split(",");
                if (item.length != 8)
                    continue;
                lon = item[0];
                lat = item[1];
                year = item[2];
                month = item[3];
                day = item[4];
                bdgd = item[5];
                bdqd = item[6];
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
                System.out.println(row + ": " + bdgd + ", " + bdqd);
                HBaseUtil.insertData(conn, "prediction", row, "wave", "bdgd", bdgd);
                HBaseUtil.insertData(conn, "prediction", row, "wave", "bdqd", bdqd);
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
