package ru.vt.avgdist;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.vt.avgdist.AvgDistancesTestData.TimeRange;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class AverageDistancesTest {

    private static List<TimeRange> testTimeRanges;
    private static InMemoryAvgDistancesCalculator calculator;

    @BeforeAll
    static void setUp() {
        testTimeRanges = AvgDistancesTestData.getTestRanges();
        calculator = new InMemoryAvgDistancesCalculator();
        calculator.init(Path.of("data"));
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (calculator != null) {
            calculator.close();
        }
        testTimeRanges = null;
    }

    // 1401 472442100 ns
    @Test
    void testCalculator() {
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(range.start(), range.end());
            System.out.println(result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }
}
