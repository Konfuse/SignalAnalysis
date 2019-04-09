package com.hust;

import com.hust.Util.HBaseUtil;

public class EvaporationWaveTableCreate {
    public static void main(String[] args) {
        String[] columnFamilies = {"position", "date", "value"};
        HBaseUtil.createTable("evaporation", columnFamilies);
    }
}
