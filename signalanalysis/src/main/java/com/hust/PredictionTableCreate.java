package com.hust;

import com.hust.Util.HBaseUtil;

/**
 * @Author: Konfuse
 * @Date: 2019/4/22 15:57
 */
public class PredictionTableCreate {
    public static void main(String[] args) {
        String[] columnFamilies = {"wave"};
        HBaseUtil.createTable("prediction", columnFamilies);
    }
}
