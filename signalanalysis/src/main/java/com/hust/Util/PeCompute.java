package com.hust.Util;

/**
 * @Author: Konfuse
 * @Date: 2019/5/6 9:34
 */
public class PeCompute {
    private static String absPath = "/home/test/Desktop/";
    public native int[] ComputePE(String strEnviron, String[] swqx_data, String[] qxtk_data, String[] zsl_data,
                                  String ybDT, float ycEvaHeight, int ybAZI,
                                  double radarLon, double radarLat, float maxHeight, float maxRange,
                                  String[] radar_para, String[] target_para, String[] jam_para,
                                  float aveDis, float maxDis
    );

    public native String ComputePEstr(String strEnviron, String[] swqx_data, String[] qxtk_data, String[] zsl_data,
                                  String ybDT, float ycEvaHeight, int ybAZI,
                                  double radarLon, double radarLat, float maxHeight, float maxRange,
                                  String[] radar_para, String[] target_para, String[] jam_para,
                                  float aveDis, float maxDis
    );

    static {
        System.load(absPath + "libPeComputev3.so");
    }


}
