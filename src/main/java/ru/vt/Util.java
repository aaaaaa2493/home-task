package ru.vt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for finding differences between lists.
 */
public class Util {

    public static <T> ListDiffResult<T> diff(List<T> first, List<T> second) {
        Set<T> firstSet = new HashSet<>(first);
        Set<T> secondSet = new HashSet<>(second);

        List<T> onlyInFirst = new ArrayList<>();
        for (T item : first) {
            if (!secondSet.contains(item)) {
                onlyInFirst.add(item);
            }
        }

        List<T> onlyInSecond = new ArrayList<>();
        for (T item : second) {
            if (!firstSet.contains(item)) {
                onlyInSecond.add(item);
            }
        }

        return new ListDiffResult<>(onlyInFirst, onlyInSecond);
    }

    public record ListDiffResult<T>(List<T> onlyInFirst, List<T> onlyInSecond) {
    }

    public static <T> List<T> duplicatedEntries(List<T> list) {

        List<T> duplicated = new ArrayList<>();
        Set<T> set = new HashSet<>();
        for (T item : list) {
            if (!set.add(item)) {
                duplicated.add(item);
            }
        }

        return duplicated;
    }

    public static int findFirstIndexBinarySearch(long[] array, long key) {
        int foundIndex = Arrays.binarySearch(array, key);
        if (foundIndex < 0) {
            return -foundIndex - 1;
        } else {
            while (foundIndex > 0 && array[foundIndex - 1] == foundIndex) {
                foundIndex--;
            }
            return foundIndex;
        }
    }
}
