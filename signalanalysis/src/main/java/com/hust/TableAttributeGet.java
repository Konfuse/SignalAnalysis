package com.hust;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 2019/4/16 14:11
 */
public class TableAttributeGet {
    public static void main(String[] args) {
        String path = "C:/Users/Konfuse/Desktop/BigDataProject/EvaporationWave.csv";
        BufferedReader reader = null;
        String line;
        String[] items;
        double maxBDGD = 0.0, maxBDQD = 0.0, minBDGD = 100.0, minBDQD = 100.0, count = 0.0;
        double valueBDGD = 0.0, valueBDQD = 0.0, sumBDGD = 0.0, sumBDQD = 0.0;

        try {
            reader = new BufferedReader(new FileReader(path));
            System.out.println(reader.readLine());
            while ((line = reader.readLine()) != null) {
                count++;
                items = line.split(",");
                valueBDGD = Double.parseDouble(items[6]);
                sumBDGD += valueBDGD;
                valueBDQD = Double.parseDouble(items[7]);
                sumBDQD += valueBDQD;
                if (valueBDGD > maxBDGD) maxBDGD = valueBDGD;
                if (valueBDGD < minBDGD) minBDGD = valueBDGD;
                if (valueBDQD > maxBDQD) maxBDQD = valueBDQD;
                if (valueBDQD < minBDQD) minBDQD = valueBDQD;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("BDGD: ");
        System.out.println("Average: " + sumBDGD / count + "\tMax: " + maxBDGD + "\tMin: " + minBDGD);
        System.out.println("BDQD: ");
        System.out.println("Average: " + sumBDQD / count + "\tMax: " + maxBDQD + "\tMin: " + minBDQD);
    }
}
