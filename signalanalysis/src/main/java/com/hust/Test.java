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
        String pattern = ".*:110,111";
        System.out.println(Pattern.matches(pattern, content));
    }
}
