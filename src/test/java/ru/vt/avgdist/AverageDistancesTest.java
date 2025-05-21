package ru.vt.avgdist;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.vt.ListDiffUtil;
import ru.vt.avgdist.AvgDistancesTestData.TimeRange;
import ru.vt.avgdist.InMemoryAvgDistancesCalculator.RideStat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private void checkMaps(RideStat correct, RideStat tested) {
        checkMaps(
            AvgDistUtil.calculateAverage(correct.totalDistance(), correct.totalTravels()),
            AvgDistUtil.calculateAverage(tested.totalDistance(), tested.totalTravels())
        );
    }

    private void checkMaps(Map<Integer, Double> correct, Map<Integer, Double> tested) {
        assertEquals(correct.size(), tested.size(), "Maps should have the same number of entries");
        assertEquals(correct.keySet(), tested.keySet(), "Maps should have the same keys");

        double EPSILON = 1e-10;
        for (Integer key : correct.keySet()) {
            double expectedValue = correct.get(key);
            double actualValue = tested.get(key);
            assertEquals(expectedValue, actualValue, EPSILON,
                "Values for key " + key + " should be equal within margin of error. "
            );
        }
    }

    @Test
    void testSameResults() {
        int i = 0;
        for (var range : testTimeRanges) {
            var result1 = calculator.dumbCalc(range.start(), range.end());
            var result2 = calculator.fastCalc(range.start(), range.end());

            try {
                checkMaps(result1, result2);
            } catch (AssertionError e) {
                var items1 = result1.items();
                var items2 = result2.items();

                var difference = ListDiffUtil.diff(items1, items2);

                System.out.println("Range: " + range.start().toEpochSecond(ZoneOffset.UTC) + " " + range.end().toEpochSecond(ZoneOffset.UTC));

                System.out.println("Items only in first result: " + difference.onlyInFirst().size());
                difference.onlyInFirst().forEach(System.out::println);

                System.out.println("Items only in second result: " + difference.onlyInSecond().size());
                difference.onlyInSecond().forEach(System.out::println);

                throw e;
            }
            System.out.println((++i) + " " + AvgDistUtil.calculateAverage(result1.totalDistance(), result1.totalTravels()));
        }
    }

    // 1401 472442100 ns (23 minutes)
    @Test
    void testDumbCalculator() {
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.dumbCalc(range.start(), range.end());
            System.out.println(result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }

    // 1147 807806400 ns (19 minutes)
    @Test
    void testFastCalculator() {
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.fastCalc(range.start(), range.end());
            System.out.println(result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }
}
