package com.hust;

import com.alibaba.fastjson.JSONObject;
import com.hust.Util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;

import java.util.HashMap;
import java.util.List;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 10:49
 */
public class LowAltitudeWaveTableQuery {
    private static String tableName = "lowaltitude";
    public static enum DuctType {
        SURFACE,
        SUSPENDED;
    }
    public static enum DataTpye {
        SURFACE_GD,
        SURFACE_QD,
        SUSPENDED_DINGGAO,
        SUSPENDED_DIGAO,
        SUSPENDED_HD,
        SUSPENDED_QD;
    }


    public String getProbablyPerMonth(String site, DuctType ductType) {
        JSONObject jsonObject = new JSONObject();
        String regex, row = null, family, month;
        double dataCount, ductCount;

        if (ductType == DuctType.SURFACE) {
            family = "surface";
        } else {
            family = "suspended";
        }

        regex = site
                + ":.*";
        List<Result> resultList = HBaseUtil.getDataByRegex(tableName, regex);

        //travel result sets
        for (Result result : resultList) {
            dataCount = 0.0;
            ductCount = 0.0;
            for (Cell cell : result.listCells()) {
                //if find the correct type, break and use the value
                row = Bytes.toString(CellUtil.cloneRow(cell));
                if (Bytes.toString(CellUtil.cloneFamily(cell)).equals(family) && Bytes.toString(CellUtil.cloneQualifier(cell)).equals("dataCount")) {
                    dataCount = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                } else if (Bytes.toString(CellUtil.cloneFamily(cell)).equals(family) && Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ductCount")) {
                    ductCount = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            if (row == null || dataCount == 0.0 || ductCount == 0.0)
                continue;
            //compute prob
            month = row.substring(row.indexOf(":") + 1).split("-")[0];
            jsonObject.put(month, ductCount / dataCount);
        }

        return jsonObject.toJSONString();
    }
}
