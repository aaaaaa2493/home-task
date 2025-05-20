package ru.vt;

public record RideItem (
    long pickupMicros,
    long dropoffMicros,
    int passengerCounts,
    double tripDistances
) {}
