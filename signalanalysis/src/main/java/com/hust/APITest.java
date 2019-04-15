package com.hust;

/**
 * @Author: Konfuse
 * @Date: 2019/4/15 14:07
 */
public class APITest {
    public static void main(String[] args) {
        EvaporationWaveTableQuery query = new EvaporationWaveTableQuery();
        System.out.println(query.queryByDate(EvaporationWaveTableQuery.ValueType.BDGD, 2017, 1, 3, 12));
        System.out.println(query.queryMonthAverage(EvaporationWaveTableQuery.ValueType.BDGD, 110, 3));
        System.out.println(query.queryYearAverage(EvaporationWaveTableQuery.ValueType.BDGD, 2017, 110));
        System.out.println(query.queryProbabilityOnMonth(EvaporationWaveTableQuery.ValueType.BDGD, 1, 110, 3));
    }
}
