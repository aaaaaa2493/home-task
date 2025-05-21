package ru.vt.avgdist;

import ru.vt.ParquetUtil;
import ru.vt.ParquetUtil.RideItemStream;
import ru.vt.RideData;
import ru.vt.RideItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class InMemoryAvgDistancesCalculator implements AverageDistances {

    private Map<Long, RideData> perMonthMap = null;

    @Override
    public void init(Path dataDir) {
        try {
            List<Path> parquetFiles = Files.list(dataDir)
                .filter(path -> path.toString().endsWith(".parquet"))
                .toList();

            System.out.println("Found " + parquetFiles.size() + " parquet files");

            List<RideItemStream<RideItem>> streams = new ArrayList<>();
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
    private Map<Long, RideData> createPerMonthMap(List<RideItemStream<RideItem>> streams) {
        System.out.println("Creating per-month map");

        Map<Long, List<RideItem>> perMonthMapUnsorted = new ConcurrentHashMap<>();

        streams.parallelStream().forEach(stream -> {
            System.out.println("Processing file: " + stream.filePath());
            stream.stream().forEach(item -> {
                long pickupMicros = item.pickupMicros();
                long dropoffMicros = item.dropoffMicros();

                // some entries have invalid data
                if (pickupMicros <= dropoffMicros) {
                    long startOfMonth = AvgDistUtil.getStartOfMonthTimestamp(pickupMicros);
                    perMonthMapUnsorted.computeIfAbsent(startOfMonth, k -> Collections.synchronizedList(new ArrayList<>())).add(item);
                }
            });
            stream.stream().close();
        });

        Map<Long, RideData> perMonthMapSorted = new HashMap<>();

        // Sort by small amounts to preserve memory, since sorting
        // in my implementation (AvgDistUtil.sortByPickupTime) requires creating temporary arrays
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
        return getAverageDistances(this::fastCalc, start, end);
    }

    protected Map<Integer, Double> getAverageDistances(BiFunction<LocalDateTime, LocalDateTime, RideStat> func,
                                                       LocalDateTime start, LocalDateTime end) {
        var stat = func.apply(start, end);
        return AvgDistUtil.calculateAverage(stat.totalDistance(), stat.totalTravels());
    }

    @Override
    public void close() {
        perMonthMap = null;
    }

    private static final boolean COLLECT_ITEMS = false;

    protected record RideStat(Map<Integer, Double> totalDistance,
                              Map<Integer, Integer> totalTravels,
                              List<RideItem> items) {};


    /// Different implementations of `getAverageDistances`


    protected RideStat dumbCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        var start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        var end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        Map<Integer, Double> totalDistance = new HashMap<>();
        Map<Integer, Integer> totalTravels = new HashMap<>();
        List<RideItem> items = COLLECT_ITEMS ? new ArrayList<>() : null;

        for (var rideData : perMonthMap.values()) {
            for (int i = 0; i < rideData.rowCount(); i++) {
                var pickup = rideData.pickupMicros()[i];
                var dropoff = rideData.dropoffMicros()[i];

                if (pickup >= start && dropoff <= end) {
                    var tripDistance = rideData.tripDistances()[i];
                    var passengerCount = rideData.passengerCounts()[i];

                    totalDistance.merge(passengerCount, tripDistance, Double::sum);
                    totalTravels.merge(passengerCount, 1, Integer::sum);

                    if (COLLECT_ITEMS) {
                        items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
                    }
                }
            }
        }

        return new RideStat(totalDistance, totalTravels, items);
    }


    protected RideStat fastCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        long start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        long end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        Map<Integer, Double> totalDistance = new HashMap<>();
        Map<Integer, Integer> totalTravels = new HashMap<>();
        List<RideItem> items = COLLECT_ITEMS ? new ArrayList<>() : null;

        long startMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(start);
        long endMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(end);

        long monthTimestamp = startMonthTimestamp;
        while (monthTimestamp <= endMonthTimestamp) {
            RideData monthData = perMonthMap.get(monthTimestamp);
            if (monthData == null) {
                continue;
            }

            for (int i = 0; i < monthData.rowCount(); i++) {
                long pickup = monthData.pickupMicros()[i];
                long dropoff = monthData.dropoffMicros()[i];

                if (pickup >= start && dropoff <= end) {
                    double tripDistance = monthData.tripDistances()[i];
                    int passengerCount = monthData.passengerCounts()[i];

                    totalDistance.merge(passengerCount, tripDistance, Double::sum);
                    totalTravels.merge(passengerCount, 1, Integer::sum);

                    if (COLLECT_ITEMS) {
                        items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
                    }
                }
            }

            monthTimestamp = AvgDistUtil.getNextMonthTimestamp(monthTimestamp);
        }

        return new RideStat(totalDistance, totalTravels, items);
    }

}
