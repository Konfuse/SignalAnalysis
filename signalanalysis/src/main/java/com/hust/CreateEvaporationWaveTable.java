package com.hust;

import com.hust.Util.HBaseUtil;

public class CreateEvaporationWaveTable {
    public static void main(String[] args) {
        String[] columnFamilies = {"evaporation"};
        HBaseUtil.createTable("points", columnFamilies);
    }
}
