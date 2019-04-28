package com.hust.EvaporationWaveTable;

import com.hust.Util.HBaseUtil;

/**
 * @Author: Konfuse
 * @Date: 19-04-09 下午04:17
 */
public class EvaporationWaveTableCreate {
    public static void main(String[] args) {
        String[] columnFamilies = {"wave"};
        HBaseUtil.createTable("evaporation", columnFamilies);
    }
}
