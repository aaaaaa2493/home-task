package ru.vt;

import ru.vt.avgdist.InMemoryAvgDistancesCalculator;

import java.io.IOException;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws IOException {
        try (var calc = new InMemoryAvgDistancesCalculator()) {
            //calc.init(Path.of("data"));
        }

        //calculateStats("data/yellow_tripdata_2020-01.parquet");
        generateData();
    }

    public static void calculateStats(String path) throws IOException {
        var data = ParquetUtil.readRideData(path);

        int increasePickup = 0;
        int equalPickup = 0;
        int decreasePickup = 0;
        int increaseDropoff = 0;
        int equalDropoff = 0;
        int decreaseDropoff = 0;

        long minPickup = Long.MAX_VALUE;
        long maxPickup = Long.MIN_VALUE;
        long minDropoff = Long.MAX_VALUE;
        long maxDropoff = Long.MIN_VALUE;

        long maxNegativePickup = 0;
        long currNegativePickup = 0;

        for (var i = 1; i < data.rowCount(); i++) {
            var prevP = data.pickupMicros()[i - 1];
            var currP = data.pickupMicros()[i];

            var diffP = (currP - prevP) / 1_000_000;
            if (diffP > 0) {
                increasePickup++;
            } else if (diffP < 0) {
                decreasePickup++;
            } else {
                equalPickup++;
            }

            if (diffP < 0) {
                currNegativePickup -= diffP;
                if (currNegativePickup > maxNegativePickup) {
                    maxNegativePickup = currNegativePickup;
                }
            } else {
                currNegativePickup = 0;
            }

            var prevD = data.dropoffMicros()[i - 1];
            var currD = data.dropoffMicros()[i];

            var diffD = (currD - prevD) / 1_000_000;
            if (diffD > 0) {
                increaseDropoff++;
            } else if (diffD < 0) {
                decreaseDropoff++;
            } else {
                equalDropoff++;
            }

            if (currP < minPickup) {
                minPickup = currP;
            }
            if (currP > maxPickup) {
                maxPickup = currP;
            }
            if (currD < minDropoff) {
                minDropoff = currD;
            }
            if (currD > maxDropoff) {
                maxDropoff = currD;
            }

            if (Math.abs(diffP) > 5000) {
                System.out.println(i + " " + currP / 1000000 + " " + currD / 1000000 + " " + diffP + " " + diffD);
            }
        }


        System.out.println();
        System.out.println("INCREASE P " + increasePickup);
        System.out.println("EQUAL    P " + equalPickup);
        System.out.println("DECREASE P " + decreasePickup);
        System.out.println();
        System.out.println("INCREASE D " + increaseDropoff);
        System.out.println("EQUAL    D " + equalDropoff);
        System.out.println("DECREASE D " + decreaseDropoff);
        System.out.println();
        System.out.println("MIN PICKUP " + minPickup);
        System.out.println("MAX PICKUP " + maxPickup);
        System.out.println("MIN DROPOFF " + minDropoff);
        System.out.println("MAX DROPOFF " + maxDropoff);
        System.out.println("PICKUP  DIFF " + (maxPickup - minPickup) / 100000);
        System.out.println("DROPOFF DIFF " + (maxDropoff - minDropoff) / 100000);
        System.out.println();
        System.out.println("maxNegativePickup " + maxNegativePickup);
    }

    public static void generateData() throws IOException {
        var start = 1577836800;
        var end = 1609459200;
        
        // Generate 10000 random points between start and end
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            // Generate a random value between start and end
            long randomEnd = start + (long)(random.nextDouble() * (end - start));
            long randomStart = start + (long)(random.nextDouble() * (randomEnd - start));

            System.out.print(randomStart + " " + randomEnd + " ");
        }
    }
}