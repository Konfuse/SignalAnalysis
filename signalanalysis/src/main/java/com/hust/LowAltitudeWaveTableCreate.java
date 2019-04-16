package com.hust;

import com.hust.Util.HBaseUtil;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 10:03
 */
public class LowAltitudeWaveTableCreate {
    public static void main(String[] args) {
        String[] columnFamilies = {"wave"};
        HBaseUtil.createTable("lowaltitude", columnFamilies);
    }
}
