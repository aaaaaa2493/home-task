package ru.vt;

public record RideData(
    long[] pickupMicros,
    long[] dropoffMicros,
    int[] passengerCounts,
    double[] tripDistances,
    int earliestPickupBetweenMonths
) {
    public int rowCount() {
        return pickupMicros.length;
    }
}
