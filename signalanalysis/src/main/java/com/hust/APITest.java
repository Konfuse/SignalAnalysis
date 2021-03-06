package com.hust;

import com.hust.LowAltitudeWaveTable.LowAltitudeWaveTableQuery;
import com.hust.PredictionTable.PredictionTableQuery;
import com.hust.ResultTable.ResultTableQuery;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 14:07
 */
public class APITest {
    public static void main(String[] args) {
        ResultTableQuery resultTableQuery = new ResultTableQuery();
        System.out.println(resultTableQuery.getAllYearHeatMap(ResultTableQuery.ValueType.BDQD, 6));
        System.out.println(resultTableQuery.getAllYearHeatMap(ResultTableQuery.ValueType.BDQD));
        System.out.println(resultTableQuery.getYearHeatMap(ResultTableQuery.ValueType.BDQD, 2017, 6));
        System.out.println(resultTableQuery.getYearHeatMap(ResultTableQuery.ValueType.BDQD, 2017));
        System.out.println(resultTableQuery.getMonthHeatMap(ResultTableQuery.ValueType.BDQD, 2, 6));
        System.out.println(resultTableQuery.getMonthHeatMap(ResultTableQuery.ValueType.BDQD, 2));
        System.out.println(resultTableQuery.getEveryYearAverage(ResultTableQuery.ValueType.BDQD, 100, 3));
        System.out.println(resultTableQuery.getEveryMonthAverage(ResultTableQuery.ValueType.BDQD, 100, 3));
        System.out.println(resultTableQuery.getYearProbability(ResultTableQuery.ValueType.BDQD, 2017, 100, 3));
        System.out.println(resultTableQuery.getMonthProbability(ResultTableQuery.ValueType.BDQD, 2, 100, 3));

        LowAltitudeWaveTableQuery lowAltitudeWaveTableQuery = new LowAltitudeWaveTableQuery();
        System.out.println(lowAltitudeWaveTableQuery.queryProbablyPerMonth("47122", LowAltitudeWaveTableQuery.DuctType.SURFACE));
        System.out.println(lowAltitudeWaveTableQuery.queryMonthAverage("47122", LowAltitudeWaveTableQuery.DuctType.SURFACE, LowAltitudeWaveTableQuery.DataType.SURFACE_GD));

        PredictionTableQuery predictionTableQuery = new PredictionTableQuery();
        System.out.println(predictionTableQuery.predict(PredictionTableQuery.PredictionType.BDQD, 2019, 3, 21, 100, 3));
    }
}
