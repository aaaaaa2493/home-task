package ru.vt.avgdist;

import ru.vt.ParquetUtil;
import ru.vt.ParquetUtil.RideItemStream;
import ru.vt.RideData;
import ru.vt.RideItem;
import ru.vt.Util;

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

    // used for debugging, if =false Java will eliminate dead code, so no performance hit
    private static final boolean DEBUG_COLLECT_ITEMS = false;

    protected record RideStat(double[] totalDistance, int[] totalTravels, List<RideItem> items) {
        static RideStat emptyStats() {
            return new RideStat(
                new double[STATS_ARRAY_SIZE],
                new int[STATS_ARRAY_SIZE],
                DEBUG_COLLECT_ITEMS ? new ArrayList<>() : null
            );
        }
    };

    protected record DayRideStat(double[] totalDistance, int[] totalTravels, List<RideItem> items,
                                 int startIndex, int endIndex, int earliestPickupBetweenDays,
                                 int[] threePlusDaysRideItems) { };

    private static final int MAX_PASSENGERS = 15;
    public static final int STATS_ARRAY_SIZE = MAX_PASSENGERS + 2;
    public static final int NULL_PASSENGERS_STATS_SLOT = STATS_ARRAY_SIZE - 1; // for NULL use last place

    private Map<Long, RideData> perMonthMap = null;
    private Map<Long, RideStat> cachedMonthResults = null;
    private Map<Long, RideStat> cachedBetweenMonthResults = null;
    private Map<Long, DayRideStat> cachedDayResults = null;
    private Map<Long, RideStat> cachedBetweenDayResults = null;

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
            cachedDayResults = new HashMap<>();
            cachedBetweenDayResults = new HashMap<>();
            perMonthMap = new HashMap<>();
            processRideData(streams);


        } catch (IOException e) {
            System.err.println("IOException in directory " + dataDir + ": " + e.getMessage());
        }
    }

    private void processRideData(List<RideItemStream<RideItem>> streams) {
        System.out.println("Processing ride data");

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

            populateCaches(monthTimestamp, pickupMicros, dropoffMicros, passengerCounts, tripDistances);
        }
    }

    private void populateCaches(Long monthTimestamp, long[] pickupMicros, long[] dropoffMicros,
                                int[] passengerCounts, double[] tripDistances) {

        long startOfDayTimestamp = -1;
        long nextDayTimestamp = -1;
        int earliestPickupBetweenDays = -1;
        int startDayIndex = -1;

        var nextMonthTimestamp = AvgDistUtil.getNextMonthTimestamp(monthTimestamp);

        var monthStats = RideStat.emptyStats();
        var betweenMonthStats = RideStat.emptyStats();
        var dayStats = RideStat.emptyStats();
        var betweenDayStats = RideStat.emptyStats();

        List<Integer> threePlusDaysRideItems = new ArrayList<>();


        var earliestPickupBetweenMonths = -1;
        for (int i = 0; i < pickupMicros.length; i++) {
            if (earliestPickupBetweenMonths < 0 && dropoffMicros[i] >= nextMonthTimestamp) {
                earliestPickupBetweenMonths = i;
            }

            if (pickupMicros[i] >= monthTimestamp) {
                if (dropoffMicros[i] < nextMonthTimestamp) {
                    addStats(monthStats, passengerCounts[i], tripDistances[i]);
                    if (DEBUG_COLLECT_ITEMS) {
                        var item = new RideItem(pickupMicros[i], dropoffMicros[i], passengerCounts[i], tripDistances[i]);
                        monthStats.items.add(item);
                    }
                } else {
                    addStats(betweenMonthStats, passengerCounts[i], tripDistances[i]);
                    if (DEBUG_COLLECT_ITEMS) {
                        var item = new RideItem(pickupMicros[i], dropoffMicros[i], passengerCounts[i], tripDistances[i]);
                        betweenMonthStats.items.add(item);
                    }
                }
            }


            if (startOfDayTimestamp == -1) {
                startDayIndex = i;
                earliestPickupBetweenDays = -1;
                startOfDayTimestamp = AvgDistUtil.getStartOfDayTimestamp(pickupMicros[i]);
                nextDayTimestamp = AvgDistUtil.getNextDayTimestamp(startOfDayTimestamp);
            }

            if (pickupMicros[i] >= nextDayTimestamp) {
                var dayStat = new DayRideStat(dayStats.totalDistance, dayStats.totalTravels, dayStats.items,
                    startDayIndex, i - 1, earliestPickupBetweenDays,
                    Util.toIntArray(threePlusDaysRideItems));
                cachedDayResults.put(startOfDayTimestamp, dayStat);
                cachedBetweenDayResults.put(startOfDayTimestamp, betweenDayStats);

                startDayIndex = i;
                earliestPickupBetweenDays = -1;
                startOfDayTimestamp = AvgDistUtil.getStartOfDayTimestamp(pickupMicros[i]);
                nextDayTimestamp = AvgDistUtil.getNextDayTimestamp(startOfDayTimestamp);

                dayStats = RideStat.emptyStats();
                betweenDayStats = RideStat.emptyStats();
                threePlusDaysRideItems = new ArrayList<>();
            }

            if (dropoffMicros[i] < nextDayTimestamp) {
                addStats(dayStats, passengerCounts[i], tripDistances[i]);
                if (DEBUG_COLLECT_ITEMS) {
                    var item = new RideItem(pickupMicros[i], dropoffMicros[i], passengerCounts[i], tripDistances[i]);
                    dayStats.items.add(item);
                }

            } else {
                if (earliestPickupBetweenDays < 0) {
                    earliestPickupBetweenDays = i;
                }

                var nextNextDayTimestamp = AvgDistUtil.getNextDayTimestamp(nextDayTimestamp);

                if (dropoffMicros[i] < nextNextDayTimestamp) {
                    addStats(betweenDayStats, passengerCounts[i], tripDistances[i]);
                    if (DEBUG_COLLECT_ITEMS) {
                        var item = new RideItem(pickupMicros[i], dropoffMicros[i], passengerCounts[i], tripDistances[i]);
                        betweenDayStats.items.add(item);
                    }

                } else {
                    threePlusDaysRideItems.add(i);
                }
            }
        }

        if (startOfDayTimestamp != -1) {
            var dayStat = new DayRideStat(dayStats.totalDistance, dayStats.totalTravels, dayStats.items,
                startDayIndex, pickupMicros.length - 1, earliestPickupBetweenDays,
                Util.toIntArray(threePlusDaysRideItems));
            cachedDayResults.put(startOfDayTimestamp, dayStat);
            cachedBetweenDayResults.put(startOfDayTimestamp, betweenDayStats);
        }

        cachedMonthResults.put(monthTimestamp, monthStats);
        cachedBetweenMonthResults.put(monthTimestamp, betweenMonthStats);

        var monthRideData = new RideData(pickupMicros, dropoffMicros, passengerCounts, tripDistances, earliestPickupBetweenMonths);
        perMonthMap.put(monthTimestamp, monthRideData);
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
        cachedDayResults = null;
        cachedBetweenDayResults = null;
    }

    /// Different implementations of `getAverageDistances`


    protected RideStat dumbCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        var start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        var end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        var stats = RideStat.emptyStats();

        for (var data : perMonthMap.values()) {
            for (int i = 0; i < data.rowCount(); i++) {
                processItem(data, i, stats, start, end);
            }
        }

        return stats;
    }


    protected RideStat fastCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        long start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        long end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        var stats = RideStat.emptyStats();

        long startMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(start);
        long endMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(end);

        long monthTimestamp = startMonthTimestamp;
        while (monthTimestamp <= endMonthTimestamp) {
            RideData monthData = perMonthMap.get(monthTimestamp);
            if (monthData == null) {
                continue;
            }
            processData(monthData, start, end, end, stats);
            monthTimestamp = AvgDistUtil.getNextMonthTimestamp(monthTimestamp);
        }

        return stats;
    }


    protected RideStat cachedCalc(LocalDateTime startDate, LocalDateTime endDate) {
        // assuming UTC
        long start = startDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
        long end = endDate.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;

        var stats = RideStat.emptyStats();

        long startMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(start);
        long endMonthTimestamp = AvgDistUtil.getStartOfMonthTimestamp(end);

        boolean withinSameMonth = startMonthTimestamp == endMonthTimestamp;
        if (withinSameMonth) {
            RideData monthData = perMonthMap.get(startMonthTimestamp);
            processMonth(monthData, start, end, end, stats);
            return stats;
        }

        // 1. manual calculation for the starting month
        if (start > startMonthTimestamp) {
            var nextMonth = AvgDistUtil.getNextMonthTimestamp(startMonthTimestamp);
            RideData startMonthData = perMonthMap.get(startMonthTimestamp);
            processMonth(startMonthData, start, end, nextMonth - 1, stats);
            startMonthTimestamp = nextMonth;
        }

        long currentMonthTimestamp = startMonthTimestamp;
        long lastFullMonthTimestamp = -1;

        // 2. process full months
        while (currentMonthTimestamp < endMonthTimestamp) {

            RideStat cachedResult = cachedMonthResults.get(currentMonthTimestamp);
            if (cachedResult != null) {
                mergeWithCached(stats, cachedResult);

                if (lastFullMonthTimestamp != -1) {
                    RideStat betweenResult = cachedBetweenMonthResults.get(lastFullMonthTimestamp);
                    if (betweenResult != null) {
                        mergeWithCached(stats, betweenResult);
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
                processDataDropoffAfter(lastMonthData, start, end, nextMonthTimestamp, stats,
                    lastMonthData.earliestPickupBetweenMonths(), lastMonthData.rowCount() - 1);
            }
        }

        // 4. manual calculation for the ending month
        RideData endMonthData = perMonthMap.get(endMonthTimestamp);
        processMonth(endMonthData, endMonthTimestamp, end, end, stats);

        return stats;
    }

    private void processMonth(RideData monthData, long start, long end, long endTimeForIteration, RideStat stats) {
        if (monthData == null) {
            return;
        }

        long startDayTimestamp = AvgDistUtil.getStartOfDayTimestamp(start);
        long endDayTimestamp = AvgDistUtil.getStartOfDayTimestamp(endTimeForIteration);

        boolean withinSameDay = startDayTimestamp == endDayTimestamp;
        if (withinSameDay) {
            processData(monthData, start, end, endTimeForIteration, stats);
            return;
        }

        // 1. manual calculation for the starting day
        if (start > startDayTimestamp) {
            long nextDay = AvgDistUtil.getNextDayTimestamp(startDayTimestamp);
            processData(monthData, start, end, nextDay - 1, stats);
            startDayTimestamp = nextDay;
        }

        long currentDayTimestamp = startDayTimestamp;
        long lastFullDayTimestamp = -1;

        // 2. process full days
        while (currentDayTimestamp < endDayTimestamp) {

            DayRideStat cachedDayResult = cachedDayResults.get(currentDayTimestamp);
            if (cachedDayResult != null) {
                mergeWithCached(stats, cachedDayResult);

                if (lastFullDayTimestamp != -1) {
                    RideStat betweenDayResult = cachedBetweenDayResults.get(lastFullDayTimestamp);
                    if (betweenDayResult != null) {
                        mergeWithCached(stats, betweenDayResult);
                    }
                }

                for (var i : cachedDayResult.threePlusDaysRideItems()) {
                    processItem(monthData, i, stats, start, end);
                }
            }

            lastFullDayTimestamp = currentDayTimestamp;
            currentDayTimestamp = AvgDistUtil.getNextDayTimestamp(currentDayTimestamp);
        }

        // 3. process `between day` entries of last day manually (can't use cache)
        if (lastFullDayTimestamp != -1) {
            DayRideStat lastDayStat = cachedDayResults.get(lastFullDayTimestamp);

            if (lastDayStat != null && lastDayStat.earliestPickupBetweenDays() >= 0) {
                long nextDayTimestamp = AvgDistUtil.getNextDayTimestamp(lastFullDayTimestamp);
                processDataDropoffAfter(monthData, start, end, nextDayTimestamp, stats,
                    lastDayStat.earliestPickupBetweenDays(), lastDayStat.endIndex());
            }
        }

        // 4. manual calculation for the ending day
        if (end > endDayTimestamp) {
            processData(monthData, endDayTimestamp, end, end, stats);
        }
    }

    private void processData(RideData monthData, long start, long end, long endTimeForIteration, RideStat stats) {
        int i = Util.findFirstIndexBinarySearch(monthData.pickupMicros(), start);
        for (; i < monthData.rowCount(); i++) {
            long pickup = monthData.pickupMicros()[i];
            if (pickup > endTimeForIteration) {
                break;
            }
            processItem(monthData, i, stats, start, end);
        }
    }

    private void processDataDropoffAfter(RideData data, long start, long end, long dropoffAfter, RideStat stats,
                                         int startIndex, int endIndex) {
        for (int i = startIndex; i <= endIndex; i++) {
            long dropoff = data.dropoffMicros()[i];
            if (dropoff >= dropoffAfter) {
                processItem(data, i, stats, start, end);
            }
        }
    }

    private void addStats(RideStat stats, int passengerCounts, double tripDistances) {
        if (passengerCounts == NULL_PASSENGER_COUNT) {
            passengerCounts = NULL_PASSENGERS_STATS_SLOT;
        }
        stats.totalDistance[passengerCounts] += tripDistances;
        stats.totalTravels[passengerCounts]++;
    }

    private void mergeWithCached(RideStat stats, RideStat cacheStat) {
        mergeWithCached(stats.totalDistance, stats.totalTravels, stats.items,
            cacheStat.totalDistance, cacheStat.totalTravels, cacheStat.items);
    }

    private void mergeWithCached(RideStat stats, DayRideStat cacheStat) {
        mergeWithCached(stats.totalDistance, stats.totalTravels, stats.items,
            cacheStat.totalDistance, cacheStat.totalTravels, cacheStat.items);
    }

    private void mergeWithCached(double[] totalDistance, int[] totalTravels, List<RideItem> items,
                                 double[] cacheDistance, int[] cacheTravels, List<RideItem> cacheItems) {
        for (var i = 0; i < STATS_ARRAY_SIZE; i++) {
            totalDistance[i] += cacheDistance[i];
            totalTravels[i] += cacheTravels[i];
        }
        if (DEBUG_COLLECT_ITEMS && cacheItems != null) {
            items.addAll(cacheItems);
        }
    }

    private void processItem(RideData data, int index, RideStat stats, long start, long end) {
        processItem(
            data.pickupMicros()[index], data.dropoffMicros()[index],
            data.passengerCounts()[index], data.tripDistances()[index],
            stats, start, end
        );
    }

    private void processItem(long pickup, long dropoff, int passengerCount, double tripDistance,
                             RideStat stats, long start, long end) {
        if (pickup >= start && dropoff <= end) {
            addStats(stats, passengerCount, tripDistance);
            if (DEBUG_COLLECT_ITEMS) {
                stats.items.add(new RideItem(pickup, dropoff, passengerCount, tripDistance));
            }
        }
    }

}
