package ru.vt;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class ParquetUtil {

    public static final int NULL_PASSENGER_COUNT = -1;

    private ParquetUtil() {
    }

    public static RideData readRideData(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path parquetPath = new Path(filePath);

        var inputFile = HadoopInputFile.fromPath(parquetPath, conf);

        List<BlockMetaData> blocks;
        try (ParquetFileReader metaReader = ParquetFileReader.open(inputFile)) {
            blocks = metaReader.getFooter().getBlocks();
        }
        long totalRowsLong = blocks.stream().mapToLong(BlockMetaData::getRowCount).sum();
        int totalRows = Math.toIntExact(totalRowsLong);

        long[] pickupMicros = new long[totalRows];
        long[] dropoffMicros = new long[totalRows];
        int[] passengerCounts = new int[totalRows];
        double[] tripDistances = new double[totalRows];

        try (
            var reader =
                AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build()
        ) {
            GenericRecord item;
            int row = 0;
            while ((item = reader.read()) != null) {
                pickupMicros[row] = (Long) item.get("tpep_pickup_datetime");
                dropoffMicros[row] = (Long) item.get("tpep_dropoff_datetime");
                tripDistances[row] = (Double) item.get("trip_distance");

                var passCount = (Double) item.get("passenger_count");
                passengerCounts[row] = passCount == null ? NULL_PASSENGER_COUNT : passCount.intValue();

                row++;
            }
        }

        return new RideData(
            pickupMicros,
            dropoffMicros,
            passengerCounts,
            tripDistances,
            -1
        );
    }


    public record RideItemStream<T>(int size, Stream<T> stream, String filePath) {}

    public static RideItemStream<RideItem> readRideAsStream(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path parquetPath = new Path(filePath);
        var inputFile = HadoopInputFile.fromPath(parquetPath, conf);

        List<BlockMetaData> blocks;
        try (ParquetFileReader metaReader = ParquetFileReader.open(inputFile)) {
            blocks = metaReader.getFooter().getBlocks();
        }
        long totalRowsLong = blocks.stream().mapToLong(BlockMetaData::getRowCount).sum();
        int totalRows = Math.toIntExact(totalRowsLong);

        var reader = AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build();
        Stream<GenericRecord> stream = Stream.generate(() -> {
                try {
                    return reader.read();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .takeWhile(Objects::nonNull)
            .onClose(() -> {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        var result = stream.map(item -> {
            long pickupMicros = (Long) item.get("tpep_pickup_datetime");
            long dropoffMicros = (Long) item.get("tpep_dropoff_datetime");
            double tripDistance = (Double) item.get("trip_distance");
            var passCount = (Double) item.get("passenger_count");
            int passengerCount = passCount == null ? NULL_PASSENGER_COUNT : passCount.intValue();

            return new RideItem(pickupMicros, dropoffMicros, passengerCount, tripDistance);
        });

        return new RideItemStream<>(totalRows, result, filePath);
    }


}
