package com.hust;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 14:07
 */
public class APITest {
    public static void main(String[] args) {
        EvaporationWaveTableQuery evaporationWaveTableQuery = new EvaporationWaveTableQuery();
        System.out.println(evaporationWaveTableQuery.queryByDate(EvaporationWaveTableQuery.ValueType.BDGD, 2017, 1, 3, 12));
        System.out.println(evaporationWaveTableQuery.queryMonthAverage(EvaporationWaveTableQuery.ValueType.BDGD, 110, 3));
        System.out.println(evaporationWaveTableQuery.queryYearAverage(EvaporationWaveTableQuery.ValueType.BDGD, 110, 3));
        System.out.println(evaporationWaveTableQuery.queryProbabilityOnMonth(EvaporationWaveTableQuery.ValueType.BDGD, 1, 110, 3));
        LowAltitudeWaveTableQuery lowAltitudeWaveTableQuery = new LowAltitudeWaveTableQuery();
        System.out.println(lowAltitudeWaveTableQuery.queryProbablyPerMonth("47122", LowAltitudeWaveTableQuery.DuctType.SURFACE));
        System.out.println(lowAltitudeWaveTableQuery.queryMonthAverage("47122", LowAltitudeWaveTableQuery.DuctType.SURFACE, LowAltitudeWaveTableQuery.DataType.SURFACE_GD));
    }
}
