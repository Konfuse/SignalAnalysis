package com.hust;

import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class Test {
    public static void main(String[] args) {
        String path = "/home/konfuse/Documents/WorkDir/EvaporationWave.csv";

        BufferedReader reader;
        double lon;
        double lat;
        int count;
        String item[];
        String line;

        try {
            count = 0;
            reader = new BufferedReader(new FileReader(path));
            System.out.println(reader.readLine());
            while ((line = reader.readLine()) != null) {
                item = line.split(",");
//                for (int i = 0; i < item.length; i++) {
//                    System.out.print(item[i] + ";");
//                }
//                System.out.println();
                lon = Double.parseDouble(item[0]);
                lat = Double.parseDouble(item[1]);
                count ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
