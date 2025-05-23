package ru.vt.avgdist;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.vt.Util;
import ru.vt.avgdist.AvgDistancesTestData.TimeRange;
import ru.vt.avgdist.InMemoryAvgDistancesCalculator.RideStat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageDistancesTest {

    private static List<TimeRange> testTimeRanges;
    private static InMemoryAvgDistancesCalculator calculator;

    @BeforeAll
    static void setUp() {
        testTimeRanges = AvgDistancesTestData.getTestRanges(); // 10 000 entries
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
    void testSameResultsDumbAndFast() {
        compareSolutions(calculator::dumbCalc, calculator::fastCalc);
    }

    @Test
    void testSameResultsDumbAndCached() {
        compareSolutions(calculator::dumbCalc, calculator::cachedCalc);
    }

    private void compareSolutions(BiFunction<LocalDateTime, LocalDateTime, RideStat> firstSolution,
                                  BiFunction<LocalDateTime, LocalDateTime, RideStat> secondSolution) {

        int i = 0;
        for (var range : testTimeRanges) {
            var result1 = firstSolution.apply(range.start(), range.end());
            var result2 = secondSolution.apply(range.start(), range.end());

            try {
                checkMaps(result1, result2);
            } catch (AssertionError e) {
                var items1 = result1.items();
                var items2 = result2.items();

                System.out.println("Range: " + range.start().toEpochSecond(ZoneOffset.UTC) + " " + range.end().toEpochSecond(ZoneOffset.UTC));

                if (items1 != null && items2 != null) {
                    var difference = Util.diff(items1, items2);

                    System.out.println(Arrays.toString(result1.totalDistance()));
                    System.out.println(Arrays.toString(result1.totalTravels()));
                    System.out.println(Arrays.toString(result2.totalDistance()));
                    System.out.println(Arrays.toString(result2.totalTravels()));

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
    // 249 768758300 ns (4.2 minutes) - array-based stats calculation
    @Test
    void testDumbCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::dumbCalc, range.start(), range.end());
            if (++i % 100 == 0) {
                System.out.println(i + " " + result);
            }
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }

    // 1147 807806400 ns (19 minutes)
    // 140 827256200 ns (2.3 minutes) - array-based stats calculation
    // 125 568455900 ns (2.1 minutes) - binary search inside month
    @Test
    void testFastCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::fastCalc, range.start(), range.end());
            if (++i % 100 == 0) {
                System.out.println(i + " " + result);
            }
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }

    // 550 661455200 ns (9.1 minutes) - month cache
    // 502 327595600 ns (8.3 minutes) - binary search inside month
    // 50 754847300 ns (50 seconds)   - array-based stats calculation
    // 7  579402600 ns (7-10 seconds) - day cache
    @Test
    void testCachedCalculator() {
        int i = 0;
        var start = System.nanoTime();
        for (var range : testTimeRanges) {
            var result = calculator.getAverageDistances(calculator::cachedCalc, range.start(), range.end());
            if (++i % 100 == 0) {
                System.out.println(i + " " + result);
            }
        }
        var end = System.nanoTime();
        System.out.println("Time: " + (end - start) + " ns");
    }
}
