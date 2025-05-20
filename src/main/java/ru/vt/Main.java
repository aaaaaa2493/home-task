package ru.vt;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ParquetUtil.readRideData("data/yellow_tripdata_2020-01.parquet");
    }
}