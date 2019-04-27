package com.hust;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author: Konfuse
 * @Date: 2019/4/25 15:00
 */
public class WriteFakePredictionWave {
    public static void main(String[] args) throws ParseException {
        String path = "/home/test/Documents/data/PredictionWaveFake.csv";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(simpleDateFormat.parse("2019-01-01"));
        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(simpleDateFormat.parse("2020-01-01"));
        int lon, lat;
        String row, line;
        String[] date;

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(path));
            writer.write("LON,LAT,YEAR,MONTH,HOUR,BDGD,BDQD");
            for (lon = 100; lon <= 150; lon++) {
                for (lat = 0; lat <= 50; lat++) {
                    calendarStart.setTime(simpleDateFormat.parse("2019-01-01"));
                    while (!calendarStart.equals(calendarEnd)) {
                        date = simpleDateFormat.format(calendarStart.getTime()).split("-");
                        row = lon + "," + lat + "," + date[0] + "," + date[1] + "," + date[2] + "," + (Math.random() * 40) + "," + (Math.random() * 203);
                        System.out.println(row);
                        writer.newLine();
                        writer.write(row);
                        calendarStart.add(Calendar.DAY_OF_YEAR, 1);
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
