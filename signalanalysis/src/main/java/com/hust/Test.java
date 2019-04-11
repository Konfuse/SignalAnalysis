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
        String content = "2017010312:110,110";
        String pattern = "[\\d]{4}01[\\d]+:110,110";
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int j = 0; j < 11; j++) {
            map.put(j, 0);
        }
        map.compute(1, (k, v)->{
            if (v == null) return 1;
            return v+1;
        });
        System.out.println(map);
    }
}
