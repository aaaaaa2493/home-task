package ru.vt.avgdist;

import ru.vt.ParquetUtil;
import ru.vt.RideData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryAvgDistancesCalculator implements AverageDistances {

    private List<RideData> data = null;

    @Override
    public void init(Path dataDir) {
        data = new ArrayList<>();
        try {
            List<Path> parquetFiles = Files.list(dataDir)
                    .filter(path -> path.toString().endsWith(".parquet"))
                    .toList();

            System.out.println("Found " + parquetFiles.size() + " parquet files");

            for (Path file : parquetFiles) {
                System.out.println("Processing file: " + file);
                RideData rideData = ParquetUtil.readRideData(file.toString());
                data.add(rideData);
            }

        } catch (IOException e) {
            System.err.println("IOException in directory " + dataDir + ": " + e.getMessage());
        }
    }

    @Override
    public Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("start must be before end");
        }
        return dumbCalc(start, end);
    }

    @Override
    public void close() throws IOException {
        data = null;
    }


    /// Different implementations of `getAverageDistances`


    private Map<Integer, Double> dumbCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        var start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        var end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        Map<Integer, Double> totalDistance = new HashMap<>();
        Map<Integer, Integer> totalTravels = new HashMap<>();

        for (var rideData : data) {
            for (int i = 0; i < rideData.rowCount(); i++) {
                var pickup = rideData.pickupMicros()[i];
                var dropoff = rideData.dropoffMicros()[i];

                if (pickup >= start && dropoff <= end) {
                    var tripDistance = rideData.tripDistances()[i];
                    var passengerCount = rideData.passengerCounts()[i];

                    totalDistance.merge(passengerCount, tripDistance, Double::sum);
                    totalTravels.merge(passengerCount, 1, Integer::sum);
                }
            }
        }

        Map<Integer, Double> averageByPassengerCount = new HashMap<>();
        for (var entry : totalDistance.entrySet()) {
            int passengerCount = entry.getKey();
            double sum = entry.getValue();
            int count = totalTravels.get(passengerCount);
            averageByPassengerCount.put(passengerCount, sum / count);
        }

        return averageByPassengerCount;
    }

}
