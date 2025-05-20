package ru.vt;

import java.io.Closeable;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Map;

interface AverageDistances extends Closeable {

    /**

     * Initializes the instance.
     * @param dataDir Path to the data directory that contains the files
     *                with Parquet data files.
     */
    void init(Path dataDir);


    /**
     * Calculates an average of all trip_distance fields for each distinct
     * passenger_count value for trips with tpep_pickup_datetime >= start
     * and tpep_dropoff_datetime <= end.
     * @return A map where key is passenger count and value is the average
     *         trip distance for this passenger count.
     */
    Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end);

}
