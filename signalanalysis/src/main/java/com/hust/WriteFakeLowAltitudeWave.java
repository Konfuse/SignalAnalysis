package com.hust;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @Author: Konfuse
 * @Date: 2019/4/16 15:08
 */
public class WriteFakeLowAltitudeWave {
    public static void main(String[] args) {
        String path = "C:/Users/Konfuse/Desktop/BigDataProject/LowAltitudeWaveFake.csv";
        BufferedWriter writer = null;
        String row;
        int[] months = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        int[] hours = {8, 20};
        int[] ductTypes = {1, 2};

        try {
            writer = new BufferedWriter(new FileWriter(path));
            writer.write("SiteNum,Month,Hour,DuctType,DataCount,DuctCount,DingGao_Average,LayerDiGao_Average,DiGao_Average,LayerHouDu_Average,HouDu_Average,QiangDu_Average");
            for (int site = 47103; site < 47132; site++) {
                for (int month : months) {
                    for (int hour : hours) {
                        for (int ductType : ductTypes) {
                            row = site + "," + month + "," + hour + "," + ductType + "," + (int)(Math.random() * 60 + 210) + "," + (int)(Math.random() * 95 + 23) + "," + (Math.random() * 1760 + 20) + "," + (Math.random() * 1670) + "," + (Math.random() * 1574) + "," + (Math.random() * 133 + 11) + "," + (Math.random() * 187 + 20) + "," + (Math.random() * 0.66 + 1);
                            System.out.println(row);
                            writer.newLine();
                            writer.write(row);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
