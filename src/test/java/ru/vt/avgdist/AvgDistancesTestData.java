package ru.vt.avgdist;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AvgDistancesTestData {

    record TimeRange(LocalDateTime start, LocalDateTime end) {}

    static List<TimeRange> getTestRanges() {
        long[] testRanges = getTestRangesArray();

        var result = new ArrayList<TimeRange>();
        for (var i = 0; i < testRanges.length; i+=2) {
            var start = Instant.ofEpochSecond(testRanges[i]).atZone(ZoneOffset.UTC).toLocalDateTime();
            var end = Instant.ofEpochSecond(testRanges[i+1]).atZone(ZoneOffset.UTC).toLocalDateTime();
            result.add(new TimeRange(start, end));
        }

        return result;
    }

    private static long[] getTestRangesArray() {
        try (var inputStream = AvgDistancesTestData.class.getClassLoader().getResourceAsStream("test-ranges.txt")) {
            if (inputStream == null) {
                throw new RuntimeException("Could not find test-ranges.txt in resources");
            }

            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            String[] parts = content.trim().split("\\s+");
            return Arrays.stream(parts)
                .mapToLong(Long::parseLong)
                .toArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read test-ranges.txt", e);
        }
    }

}
