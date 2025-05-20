package ru.vt.avgdist;

import ru.vt.ParquetUtil;
import ru.vt.ParquetUtil.StreamWithSize;
import ru.vt.RideData;
import ru.vt.RideItem;

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
    private Map<Long, RideData> perMonthMap = null;

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

            List<StreamWithSize<RideItem>> streams = new ArrayList<>();
            for (Path file : parquetFiles) {
                streams.add(ParquetUtil.readRideAsStream(file.toString()));
            }
            perMonthMap = createPerMonthMap(streams);

        } catch (IOException e) {
            System.err.println("IOException in directory " + dataDir + ": " + e.getMessage());
        }
    }

    /**
     * @return Per-month map of RideData objects. RideData objects are sorted by pickup time.
     */
    private Map<Long, RideData> createPerMonthMap(List<StreamWithSize<RideItem>> streams) {
        System.out.println("Creating per-month map");

        Map<Long, List<RideItem>> perMonthMapUnsorted = new HashMap<>();

        for (var stream : streams) {
            stream.stream().forEach(item -> {
                long pickupMicros = item.pickupMicros();
                long startOfMonth = AvgDistUtil.getStartOfMonthTimestamp(pickupMicros);
                perMonthMapUnsorted.computeIfAbsent(startOfMonth, k -> new ArrayList<>()).add(item);
            });
            stream.stream().close();
        }

        Map<Long, RideData> perMonthMapSorted = new HashMap<>();

        // Sort by small amounts to preserve memory, since sorting
        // in my implementation (AvgDistUtil.sortByPickupTime) requires creating a temporary array
        for (var entry : perMonthMapUnsorted.entrySet()) {
            var monthTimestamp = entry.getKey();
            var monthItems = entry.getValue();

            long[] pickupMicros = new long[monthItems.size()];
            long[] dropoffMicros = new long[monthItems.size()];
            int[] passengerCounts = new int[monthItems.size()];
            double[] tripDistances = new double[monthItems.size()];

            int i = 0;
            for (var item : monthItems) {
                pickupMicros[i] = item.pickupMicros();
                dropoffMicros[i] = item.dropoffMicros();
                passengerCounts[i] = item.passengerCounts();
                tripDistances[i] = item.tripDistances();
                i++;
            }

            AvgDistUtil.sortByPickupTime(pickupMicros, dropoffMicros, passengerCounts, tripDistances);

            var monthRideData = new RideData(pickupMicros, dropoffMicros, passengerCounts, tripDistances);
            perMonthMapSorted.put(monthTimestamp, monthRideData);
        }

        return perMonthMapSorted;
    }

    @Override
    public Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("start must be before end");
        }
        return fastCalc(start, end);
    }

    @Override
    public void close() throws IOException {
        data = null;
        perMonthMap = null;
    }


    /// Different implementations of `getAverageDistances`


    protected Map<Integer, Double> dumbCalc(LocalDateTime startDate, LocalDateTime endDate) {
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



    protected Map<Integer, Double> fastCalc(LocalDateTime startDate, LocalDateTime endDate) {
        return null;
    }

}
