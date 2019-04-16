package com.hust;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.hadoop.hbase.util.Hash;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static enum ValueType {
        BDGD,
        BDQD;
    }

    public static void main(String[] args) {
        String path = "C:/Users/Konfuse/Desktop/BigDataProject/LowAltitudeWaveFake.csv";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
            System.out.println(reader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}