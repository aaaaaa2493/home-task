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

import static ru.vt.ParquetUtil.NULL_PASSENGER_COUNT;

public class InMemoryAvgDistancesCalculator implements AverageDistances {

    private static final int MAX_PASSENGERS = 15;
    public static final int STATS_ARRAY_SIZE = MAX_PASSENGERS + 1;
    public static final int NULL_PASSENGERS_STATS_SLOT = STATS_ARRAY_SIZE - 1; // for NULL use last place

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

            double[] monthTotalDistance = new double[STATS_ARRAY_SIZE];
            int[] monthTotalTravels = new int[STATS_ARRAY_SIZE];
            double[] betweenMonthTotalDistance = new double[STATS_ARRAY_SIZE];
            int[] betweenMonthTotalTravels = new int[STATS_ARRAY_SIZE];
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
                        addStats(monthTotalDistance, monthTotalTravels, item.passengerCounts(), item.tripDistances());
                        if (COLLECT_ITEMS) {
                            monthRideItems.add(item);
                        }
                    } else {
                        addStats(betweenMonthTotalDistance, betweenMonthTotalTravels, item.passengerCounts(), item.tripDistances());
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

    protected record RideStat(double[] totalDistance, int[] totalTravels, List<RideItem> items) {};


    /// Different implementations of `getAverageDistances`


    protected RideStat dumbCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        var start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        var end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        double[] totalDistance = new double[STATS_ARRAY_SIZE];
        int[] totalTravels = new int[STATS_ARRAY_SIZE];
        List<RideItem> items = COLLECT_ITEMS ? new ArrayList<>() : null;

        for (var rideData : perMonthMap.values()) {
            for (int i = 0; i < rideData.rowCount(); i++) {
                var pickup = rideData.pickupMicros()[i];
                var dropoff = rideData.dropoffMicros()[i];

                if (pickup >= start && dropoff <= end) {
                    var tripDistance = rideData.tripDistances()[i];
                    var passengerCount = rideData.passengerCounts()[i];

                    addStats(totalDistance, totalTravels, passengerCount, tripDistance);

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

        double[] totalDistance = new double[STATS_ARRAY_SIZE];
        int[] totalTravels = new int[STATS_ARRAY_SIZE];
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

                    addStats(totalDistance, totalTravels, passengerCount, tripDistance);

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

        double[] totalDistance = new double[STATS_ARRAY_SIZE];
        int[] totalTravels = new int[STATS_ARRAY_SIZE];
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

                        addStats(totalDistance, totalTravels, passengerCount, tripDistance);

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
                                     double[] totalDistance, int[] totalTravels, List<RideItem> items) {

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

                addStats(totalDistance, totalTravels, passengerCount, tripDistance);

                if (COLLECT_ITEMS) {
                    items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
                }
            }
        }
    }

    private void addStats(double[] totalDistance, int[] totalTravels, int passengerCounts, double tripDistances) {
        if (passengerCounts == NULL_PASSENGER_COUNT) {
            passengerCounts = NULL_PASSENGERS_STATS_SLOT;
        }
        totalDistance[passengerCounts] += tripDistances;
        totalTravels[passengerCounts]++;
    }

    private void mergeWithCached(double[] totalDistance, int[] totalTravels,
                                 List<RideItem> items, RideStat cacheStat) {
        for (var i = 0; i < STATS_ARRAY_SIZE; i++) {
            totalDistance[i] += cacheStat.totalDistance()[i];
            totalTravels[i] += cacheStat.totalTravels()[i];
        }
        if (COLLECT_ITEMS && cacheStat.items != null) {
            items.addAll(cacheStat.items);
        }
    }

}
