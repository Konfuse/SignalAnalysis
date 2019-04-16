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
        String row = "43251:06-20:01";
        String regex = "43251" + ":[\\d]{2}-[\\d]{2}:" + "02";
        System.out.println(row.substring(0, row.indexOf("-")).split(":")[1]);
    }
}