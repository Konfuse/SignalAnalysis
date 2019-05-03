package com.hust.CallCompute;

import com.alibaba.fastjson.JSONObject;
import com.hust.DMTable.DMTableQuery;
import com.hust.PredictionTable.PredictionTableQuery;
import com.hust.RadarTable.RadarTableQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: Konfuse
 * @Date: 2019/5/3 14:29
 */
public class Compute {
    private static String absPath = "/home/test/Desktop/";
    public native int[] ComputePE(String strEnviron, String[] swqx_data, String[] qxtk_data, String[] zsl_data,
                                  String ybDT, double ycEvaHeight, int ybAZI, double maxHeight, double maxRange,
                                  double radarLon, double radarLat, String[] radar_para,
                                  String[] target_para, String[] jam_para, double aveDis, double maxDis
    );

    public String compute(String sys_name, String radar_name, String target_name, double target_rcs, double target_height, String env_name, String jam_name, double jam_power, double jam_gain, double jam_loss, double jam_height, double jam_lon, double jam_lat, double max_height, double max_distance) {
        JSONObject jsonObject = new JSONObject();
        System.load(absPath + "libPeCompute.so");
        DMTableQuery dmTableQuery = new DMTableQuery();
        RadarTableQuery radarTableQuery = new RadarTableQuery();
        PredictionTableQuery predictionTableQuery = new PredictionTableQuery();

        //load data as params
        String strEnviron = env_name;
        String[] swqx_data = null, qxtk_data = null, zsl_data = null;
        if ("swqx".equals(env_name)) swqx_data = dmTableQuery.getSwqxDataOnCsv(sys_name);
        else if ("qxtk".equals(env_name)) qxtk_data = dmTableQuery.getQxtkDataOnCsv(sys_name);
        else if ("zsl".equals(env_name)) zsl_data = dmTableQuery.getZslDataOnCsv(sys_name);

        double[] position = dmTableQuery.getSystemPositionOnCsv(sys_name);
        double radarLon = position[0], radarLat = position[1];

        String ybDT = null;
        double ycEvaHeight = predictionTableQuery.predictTheDay(PredictionTableQuery.PredictionType.BDGD, (int)radarLon, (int)radarLat);
        int ybAZI = 0;
        double maxHeight = max_height, maxRange = max_distance * 1.852;

        List<String> paras = new ArrayList<>();
        String[] radar_para = null, target_para = null, jam_para = null;
        //build radar_para
        radar_para = dmTableQuery.getRadarParamOnCsv(sys_name, radar_name);
        //build target_para
        paras.add(target_name); paras.add(String.valueOf(target_rcs)); paras.add(String.valueOf(target_height));
        target_para = paras.toArray(new String[0]);
        //build jam_para
        paras.clear();
        paras.add(jam_name); paras.add(String.valueOf(jam_power)); paras.add(String.valueOf(jam_gain));
        paras.add(String.valueOf(jam_loss)); paras.add(String.valueOf(jam_height));
        paras.add(String.valueOf(jam_lon)); paras.add(String.valueOf(jam_lat));
        jam_para = paras.toArray(new String[0]);

        double aveDis, maxDis;
        Map<String, Double> distancePerHour = radarTableQuery.getDistanceMapPerHourOnCsv((int)radarLon -1 , (int)radarLat - 1, (int)radarLon + 1, (int)radarLat + 1, Integer.parseInt(radar_name));
        aveDis = radarTableQuery.getAvgDistance(distancePerHour);
        maxDis = radarTableQuery.getMaxDistance(distancePerHour);

        int[] data = ComputePE(strEnviron, swqx_data, qxtk_data, zsl_data, ybDT, ycEvaHeight, ybAZI, maxHeight, maxRange, radarLon, radarLat, radar_para, target_para, jam_para, aveDis, maxDis);

        if (data.length != 720001)
            return jsonObject.toJSONString();

        List<List<Integer>> probability = new ArrayList<>();
        List<List<Integer>> distance = new ArrayList<>();
        int flag = data[0];
        int column, index = 1, i;

        for (column = 1; column <= 600; column++) {
            List<Integer> list = new ArrayList<>();
            for (i = 0; i < 600; i++) {
                list.add(data[index]);
                index++;
            }
            probability.add(list);
        }

        for (column = 1; column <= 600; column++) {
            List<Integer> list = new ArrayList<>();
            for (i = 0; i < 600; i++) {
                list.add(data[index]);
                index++;
            }
            distance.add(list);
        }

        jsonObject.put("probability", probability);
        jsonObject.put("distance", distance);
        jsonObject.put("flag", flag);

        return jsonObject.toJSONString();
    }
}
