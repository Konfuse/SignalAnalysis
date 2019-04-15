package com.hust;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class Test {
    public static enum ValueType {
        BDGD,
        BDQD;
    }

    public static void main(String[] args) {
        String row = "43251:06-20";
        System.out.println(row.substring(row.indexOf(":") + 1).split("-")[0]);
        String content = "2017-01-02-06:111,111";
        String regex = "[\\d]{4}-"
                + String.format("%02d", 1)
                + "-[\\d]{2}-[\\d]{2}:"
                + String.format("%03d", 111)
                + ","
                + String.format("%03d", 111);
        System.out.println(Pattern.matches(regex, content));
    }
}