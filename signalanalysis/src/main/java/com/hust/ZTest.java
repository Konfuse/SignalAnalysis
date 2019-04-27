package com.hust;

import java.io.*;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static void main(String[] args) {
        BufferedReader reader;
        BufferedWriter writer = null;
        String fakePath = "/home/test/Documents/data/EvaporationWaveFake.csv";
        String path = "/home/test/Documents/data/EvaporationWave.csv";
        String line;
        try {
            reader = new BufferedReader(new FileReader(path));
            writer = new BufferedWriter(new FileWriter(fakePath, true));
            while ((line = reader.readLine()) != null) {
                writer.newLine();
                writer.write(line);
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