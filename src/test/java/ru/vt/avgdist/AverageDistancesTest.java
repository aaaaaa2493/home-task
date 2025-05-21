package ru.vt.avgdist;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.vt.Util;
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
            var result2 = calculator.cachedCalc(range.start(), range.end());

            try {
                checkMaps(result1, result2);
            } catch (AssertionError e) {
                var items1 = result1.items();
                var items2 = result2.items();

                System.out.println("Range: " + range.start().toEpochSecond(ZoneOffset.UTC) + " " + range.end().toEpochSecond(ZoneOffset.UTC));

                if (items1 != null && items2 != null) {
                    var difference = Util.diff(items1, items2);

                    System.out.println(result1.totalDistance());
                    System.out.println(result1.totalTravels());
                    System.out.println(result2.totalDistance());
                    System.out.println(result2.totalTravels());

                    System.out.println("Items only in first result: " + difference.onlyInFirst().size());
                    difference.onlyInFirst().stream().limit(100).forEach(System.out::println);

                    System.out.println("Items only in second result: " + difference.onlyInSecond().size());
                    difference.onlyInSecond().stream().limit(100).forEach(System.out::println);

                    if (difference.onlyInFirst().isEmpty() && difference.onlyInSecond().isEmpty()) {
                        var duplicated1 = Util.duplicatedEntries(items1);
                        var duplicated2 = Util.duplicatedEntries(items2);

                        System.out.println("Duplicated entries in first result: " + duplicated1.size());
                        duplicated1.stream().limit(100).forEach(System.out::println);
                        System.out.println("Duplicated entries in second result: " + duplicated2.size());
                        duplicated2.stream().limit(100).forEach(System.out::println);
                    }
                }

                throw e;
            }
            System.out.println((++i) + " " + AvgDistUtil.calculateAverage(result1.totalDistance(), result1.totalTravels()));
        }
    }

    // 1401 472442100 ns (23 minutes)
    @Test
    void testDumbCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::dumbCalc, range.start(), range.end());
            System.out.println((++i) + " " + result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }

    // 1147 807806400 ns (19 minutes)
    @Test
    void testFastCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::fastCalc, range.start(), range.end());
            System.out.println((++i) + " " + result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }

    // Attempt 1: 550 661455200 ns (9.1 minutes) - month cache
    // Attempt 2: 502 327595600 ns (8.3 minutes) - binary search inside month
    @Test
    void testCachedCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::cachedCalc, range.start(), range.end());
            System.out.println((++i) + " " + result);
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }
}
