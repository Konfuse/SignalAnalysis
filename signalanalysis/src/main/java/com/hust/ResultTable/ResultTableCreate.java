package com.hust.ResultTable;

import com.hust.Util.HBaseUtil;

/**
 * @Author: Konfuse
 * @Date: 2019/4/24 15:25
 */
public class ResultTableCreate {
    public static void main(String[] args) {
        HBaseUtil.deleteTable("result_of_evaporation");
        String[] columnFamilies = {"bdgd", "bdqd"};
        HBaseUtil.createTable("result_of_evaporation", columnFamilies);
    }
}
