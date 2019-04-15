package com.hust;

import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 10:13
 */
public class LowAltitudeWaveTableInsert {
    public static void main(String[] args) {
        String path = "/home/test/Documents/LowAltitude.csv";

        Connection connection = null;
        BufferedReader reader;
        String[] items;
        String line, row, family, siteId, month, hour, ductType, dataCount, ductCount, dinggaoAvg, layerdigaoAvg, digaoAvg, layerhouduAvg, houduAvg, qiangduAvg;

        try {
            connection = HBaseUtil.init();
            reader = new BufferedReader(new FileReader(path));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                items = line.split(",");
                siteId = items[0];
                month = items[1];
                hour = items[2];
                ductType = items[3];
                dataCount = items[4];
                ductCount = items[5];
                dinggaoAvg = items[6];
                layerdigaoAvg = items[7];
                digaoAvg = items[8];
                layerhouduAvg = items[9];
                houduAvg = items[10];
                qiangduAvg = items[11];

                //row format(siteId:mm-dd)
                row = siteId
                        + ":"
                        + String.format("%02d", Integer.parseInt(month))
                        + "-"
                        + String.format("%02d", Integer.parseInt(hour));
                if ("1".equals(ductType)) {
                    family = "surface";
                } else {
                    family = "suspended";
                }
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "dataCount", dataCount);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "ductCount", ductCount);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "dinggaoAvg", dinggaoAvg);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "layerdigaoAvg", layerdigaoAvg);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "digaoAvg", digaoAvg);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "layerhouduAvg", layerhouduAvg);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "houduAvg", houduAvg);
                HBaseUtil.insertData(connection, "lowaltitude", row, family, "qiangduAvg", qiangduAvg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                HBaseUtil.closeAll(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
