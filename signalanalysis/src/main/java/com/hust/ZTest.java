package com.hust;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @Author: Konfuse
 * @Date: 19-4-9 下午9:00
 */
public class ZTest {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM-dd");

        String result1 = simpleDateFormat.format(new Date());
        System.out.println(result1);

        int[][] arrays = new int[2][2];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                arrays[i][j] = 1;
            }
        }
        System.out.println(Arrays.deepToString(arrays));

        List<List<Integer>> lists = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            List<Integer> list = new ArrayList<>();
            for (int j = 0; j < 2; j++) {
                list.add(1);
            }
            lists.add(list);
        }
        System.out.println(lists);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("1", arrays);
        jsonObject.put("2", lists);
        System.out.println(jsonObject.toJSONString());

        double[] position = new double[0];
        System.out.println(Arrays.toString(position));
    }
}