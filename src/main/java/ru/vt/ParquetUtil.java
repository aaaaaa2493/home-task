package ru.vt;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class ParquetUtil {

    private ParquetUtil() {}

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
                passengerCounts[row] = passCount == null ? -1 : passCount.intValue();

                row++;
            }
        }

        return new RideData(
            pickupMicros,
            dropoffMicros,
            passengerCounts,
            tripDistances
        );
    }

}
