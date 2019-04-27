package com.hust;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author: Konfuse
 * @Date: 2019/4/16 13:31
 */
public class WriteFakeEvaporationWave {
    public static void main(String[] args) throws ParseException {
        String fakePath = "/home/test/Documents/data/EvaporationWaveFake.csv";
        String path = "/home/test/Documents/data/EvaporationWave.csv";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(simpleDateFormat.parse("2008-01-01"));
        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(simpleDateFormat.parse("2017-01-01"));
        int[] hours = {0, 6, 12, 18};
        int lon, lat;
        String row, line;
        String[] date;

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(fakePath));
            writer.write("LON,LAT,YEAR,MONTH,DAY,HOUR,BDGD,BDQD");
            for (lon = 100; lon <= 150; lon++) {
                for (lat = 0; lat <= 50; lat++) {
                    calendarStart.setTime(simpleDateFormat.parse("2008-01-01"));
                    while (!calendarStart.equals(calendarEnd)) {
                        for (int hour : hours) {
                            date = simpleDateFormat.format(calendarStart.getTime()).split("-");
                            row = lon + "," + lat + "," + date[0] + "," + date[1] + "," + date[2] + "," + hour + "," + (Math.random() * 40) + "," + (Math.random() * 203);
                            System.out.println(row);
                            writer.newLine();
                            writer.write(row);
                        }
                        calendarStart.add(Calendar.DAY_OF_YEAR, 1);
                    }
                }
            }

            BufferedReader reader = new BufferedReader(new FileReader(path));
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
