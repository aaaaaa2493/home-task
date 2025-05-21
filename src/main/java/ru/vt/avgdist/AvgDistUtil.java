package ru.vt.avgdist;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class AvgDistUtil {

    public static void sortByPickupTime(long[] pickupMicros, long[] dropoffMicros,
                                        int[] passengerCounts, double[] tripDistances) {

        Integer[] indices = new Integer[pickupMicros.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }

        Arrays.sort(indices, Comparator.comparingLong(i -> pickupMicros[i]));

        long[] sortedPickups = new long[pickupMicros.length];
        long[] sortedDropoffs = new long[dropoffMicros.length];
        int[] sortedPassengers = new int[passengerCounts.length];
        double[] sortedDistances = new double[tripDistances.length];

        for (int i = 0; i < indices.length; i++) {
            int originalIndex = indices[i];
            sortedPickups[i] = pickupMicros[originalIndex];
            sortedDropoffs[i] = dropoffMicros[originalIndex];
            sortedPassengers[i] = passengerCounts[originalIndex];
            sortedDistances[i] = tripDistances[originalIndex];
        }

        System.arraycopy(sortedPickups, 0, pickupMicros, 0, pickupMicros.length);
        System.arraycopy(sortedDropoffs, 0, dropoffMicros, 0, dropoffMicros.length);
        System.arraycopy(sortedPassengers, 0, passengerCounts, 0, passengerCounts.length);
        System.arraycopy(sortedDistances, 0, tripDistances, 0, tripDistances.length);
    }

    public static long getStartOfMonthTimestamp(long timestampMicros) {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
            timestampMicros / 1_000_000, 0,
            ZoneOffset.UTC
        );

        LocalDateTime firstDayOfMonth = dateTime
            .withDayOfMonth(1)
            .withHour(0)
            .withMinute(0)
            .withSecond(0)
            .withNano(0);

        return firstDayOfMonth.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
    }

    public static long getNextMonthTimestamp(long currentMonthTimestamp) {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
            currentMonthTimestamp / 1_000_000, 0,
            ZoneOffset.UTC
        );

        LocalDateTime nextMonth = dateTime.plusMonths(1);

        return nextMonth.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
    }

    public static Map<Integer, Double> calculateAverage(Map<Integer, Double> totalDistance,
                                                        Map<Integer, Integer> totalTravels) {
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
