package ru.vt.avgdist;

import ru.vt.Util;
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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;

public class InMemoryAvgDistancesCalculator implements AverageDistances {

    private Map<Long, RideData> perMonthMap = null;
    private Map<Long, RideStat> cachedMonthResults = null;
    private Map<Long, RideStat> cachedBetweenMonthResults = null;

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
            cachedMonthResults = new HashMap<>();
            cachedBetweenMonthResults = new HashMap<>();
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

        Map<Long, List<RideItem>> perMonthMapUnsorted = new ConcurrentSkipListMap<>();

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

            var nextMonthTimestamp = AvgDistUtil.getNextMonthTimestamp(monthTimestamp);
            Map<Integer, Double> monthTotalDistance = new HashMap<>();
            Map<Integer, Integer> monthTotalTravels = new HashMap<>();
            Map<Integer, Double> betweenMonthTotalDistance = new HashMap<>();
            Map<Integer, Integer> betweenMonthTotalTravels = new HashMap<>();
            List<RideItem> monthRideItems = COLLECT_ITEMS ? new ArrayList<>() : null;
            List<RideItem> partialMonthRideItems = COLLECT_ITEMS ? new ArrayList<>() : null;

            int i = 0;
            for (var item : monthItems) {
                pickupMicros[i] = item.pickupMicros();
                dropoffMicros[i] = item.dropoffMicros();
                passengerCounts[i] = item.passengerCounts();
                tripDistances[i] = item.tripDistances();
                i++;
            }

            AvgDistUtil.sortByPickupTime(pickupMicros, dropoffMicros, passengerCounts, tripDistances);

            var earliestPickupBetweenMonths = -1;
            for (i = 0; i < pickupMicros.length; i++) {
                if (earliestPickupBetweenMonths < 0 && dropoffMicros[i] >= nextMonthTimestamp) {
                    earliestPickupBetweenMonths = i;
                }

                var item = new RideItem(pickupMicros[i], dropoffMicros[i], passengerCounts[i], tripDistances[i]);
                if (item.pickupMicros() >= monthTimestamp) {
                    if (item.dropoffMicros() < nextMonthTimestamp) {
                        monthTotalDistance.merge(item.passengerCounts(), item.tripDistances(), Double::sum);
                        monthTotalTravels.merge(item.passengerCounts(), 1, Integer::sum);
                        if (COLLECT_ITEMS) {
                            monthRideItems.add(item);
                        }
                    } else {
                        betweenMonthTotalDistance.merge(item.passengerCounts(), item.tripDistances(), Double::sum);
                        betweenMonthTotalTravels.merge(item.passengerCounts(), 1, Integer::sum);
                        if (COLLECT_ITEMS) {
                            partialMonthRideItems.add(item);
                        }
                    }
                }
            }

            var monthStat = new RideStat(monthTotalDistance, monthTotalTravels, monthRideItems);
            cachedMonthResults.put(monthTimestamp, monthStat);

            var betweenMonthStat = new RideStat(betweenMonthTotalDistance, betweenMonthTotalTravels, partialMonthRideItems);
            cachedBetweenMonthResults.put(monthTimestamp, betweenMonthStat);

            var monthRideData = new RideData(pickupMicros, dropoffMicros, passengerCounts, tripDistances, earliestPickupBetweenMonths);
            perMonthMapSorted.put(monthTimestamp, monthRideData);
        }

        return perMonthMapSorted;
    }

    @Override
    public Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
        return getAverageDistances(this::cachedCalc, start, end);
    }

    protected Map<Integer, Double> getAverageDistances(BiFunction<LocalDateTime, LocalDateTime, RideStat> func,
                                                       LocalDateTime start, LocalDateTime end) {
        var stat = func.apply(start, end);
        return AvgDistUtil.calculateAverage(stat.totalDistance(), stat.totalTravels());
    }

    @Override
    public void close() {
        perMonthMap = null;
        cachedMonthResults = null;
        cachedBetweenMonthResults = null;
    }

    // used for debugging, if =false Java will eliminate dead code, so no performance hit
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


    protected RideStat cachedCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        long start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        long end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        Map<Integer, Double> totalDistance = new HashMap<>();
        Map<Integer, Integer> totalTravels = new HashMap<>();
        List<RideItem> items = COLLECT_ITEMS ? new ArrayList<>() : null;

        long startMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(start);
        long endMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(end);

        // dates within the same month
        if (startMonthTimestamp == endMonthTimestamp) {
            RideData monthData = perMonthMap.get(startMonthTimestamp);
            if (monthData != null) {
                processPartialMonth(monthData, start, end, totalDistance, totalTravels, items);
            }
            return new RideStat(totalDistance, totalTravels, items);
        }

        // 1. partial calculation for the starting month
        RideData startMonthData = perMonthMap.get(startMonthTimestamp);
        if (startMonthData != null) {
            processPartialMonth(startMonthData, start, end, totalDistance, totalTravels, items);
        }

        long currentMonthTimestamp = AvgDistUtil.getNextMonthTimestamp(startMonthTimestamp);
        long lastFullMonthTimestamp = -1;

        // 2. add cached data for full months + for between full months
        while (currentMonthTimestamp < endMonthTimestamp) {

            RideStat cachedResult = cachedMonthResults.get(currentMonthTimestamp);
            if (cachedResult != null) {
                mergeWithCached(totalDistance, totalTravels, items, cachedResult);

                if (lastFullMonthTimestamp != -1) {
                    RideStat betweenResult = cachedBetweenMonthResults.get(lastFullMonthTimestamp);
                    if (betweenResult != null) {
                        mergeWithCached(totalDistance, totalTravels, items, betweenResult);
                    }
                }
            }

            lastFullMonthTimestamp = currentMonthTimestamp;
            currentMonthTimestamp = AvgDistUtil.getNextMonthTimestamp(currentMonthTimestamp);
        }

        // 3. process `between month` entries of last month manually (can't use cache)
        if (lastFullMonthTimestamp != -1) {
            RideData lastMonthData = perMonthMap.get(lastFullMonthTimestamp);
            if (lastMonthData != null && lastMonthData.earliestPickupBetweenMonths() >= 0) {
                long nextMonthTimestamp = AvgDistUtil.getNextMonthTimestamp(lastFullMonthTimestamp);
                for (int i = lastMonthData.earliestPickupBetweenMonths(); i < lastMonthData.rowCount(); i++) {
                    long pickup = lastMonthData.pickupMicros()[i];
                    long dropoff = lastMonthData.dropoffMicros()[i];

                    if (pickup >= lastFullMonthTimestamp && dropoff >= nextMonthTimestamp && dropoff <= end) {
                        int passengerCount = lastMonthData.passengerCounts()[i];
                        double tripDistance = lastMonthData.tripDistances()[i];

                        totalDistance.merge(passengerCount, tripDistance, Double::sum);
                        totalTravels.merge(passengerCount, 1, Integer::sum);

                        if (COLLECT_ITEMS) {
                            items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
                        }
                    }
                }
            }
        }

        // 4. partial calculation for the ending month
        RideData endMonthData = perMonthMap.get(endMonthTimestamp);
        if (endMonthData != null) {
            processPartialMonth(endMonthData, endMonthTimestamp, end, totalDistance, totalTravels, items);
        }

        return new RideStat(totalDistance, totalTravels, items);
    }

    private void processPartialMonth(RideData monthData, long startTime, long endTime,
                                     Map<Integer, Double> totalDistance, Map<Integer, Integer> totalTravels, List<RideItem> items) {

        int i = 0;
        if (monthData.pickupMicros()[0] < startTime) {
            i = Util.findFirstIndexBinarySearch(monthData.pickupMicros(), startTime);
        }

        for (; i < monthData.rowCount(); i++) {
            long pickup = monthData.pickupMicros()[i];
            long dropoff = monthData.dropoffMicros()[i];

            if (pickup > endTime) {
                break;
            }

            if (pickup >= startTime && dropoff <= endTime) {
                int passengerCount = monthData.passengerCounts()[i];
                double tripDistance = monthData.tripDistances()[i];

                totalDistance.merge(passengerCount, tripDistance, Double::sum);
                totalTravels.merge(passengerCount, 1, Integer::sum);

                if (COLLECT_ITEMS) {
                    items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
                }
            }
        }
    }

    private void mergeWithCached(Map<Integer, Double> totalDistance,  Map<Integer, Integer> totalTravels,
                                 List<RideItem> items, RideStat cacheStat) {
        for (var entry : cacheStat.totalDistance.entrySet()) {
            totalDistance.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        for (var entry : cacheStat.totalTravels.entrySet()) {
            totalTravels.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
        if (COLLECT_ITEMS && cacheStat.items != null) {
            items.addAll(cacheStat.items);
        }
    }

}
