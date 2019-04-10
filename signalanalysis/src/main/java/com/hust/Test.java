package com.hust;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
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
        HashMap<String, Double> mapSum = new HashMap<>();
        for (int i = 1; i <= 12; i++) {
            mapSum.put(String.format("%02d", i), 0.0);
        }
        String some = "12";
        mapSum.compute("02", (k, v) -> {
            if (v == null) return 0.0;
            return v + Double.parseDouble(some);
        });
        System.out.println(mapSum.toString());
    }
}
