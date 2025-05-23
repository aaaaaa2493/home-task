
### Implementation of `AverageDistances` interface

- Located in package `ru.vt.avgdist`
- Contains 3 implementations: `dumbCalc`, `fastCalc` and `cachedCalc`, the latter is the fastest

#### Details of `cachedCalc` implementation

Given `startDate` and `endDate` the range is divided into the following parts:

1. **Full-month** parts
2. **Full-day** parts in the leftover range
3. Remaining **part-day** parts (up to 2 parts)

Both **1** and **2** ranges are precalculated (named caches), leaving only **3rd** range to actually calculate iteratively.

### Precalculation of ranges

Starting from sorting all entries by **pickup date**, 
both **full-month** and **full-day** caches are precalculated using 3 types of caches each:

1. **Inside-period cache** - cache for data rides strictly inside the period (month or day)
2. **Between-period cache** - cache for data rides that started in this period and finished in the next period
3. **Three-plus-periods cache** - cache mor entries that span more that 2 periods (mostly for day cache)

Caches **1** and **2** keep a precalculated sum and number of distances for each passenger count, 
whereas cache **3** contains indexes of rides, which is analysed manually. Also, Cache **1** contains **index 
of earliest entry** that is not fully inside this period, which would be useful in following calculations.

### Calculation using caches

For each range we define 2 possible *ride-types*

1. That started and finished within this range (end of range excluded)
2. That started in this range and finished not in this range.

Then, upon dividing into **full-month**, **full-day** and **part-day** parts we calculate in this order

1. Starting **part-day** part until the start of the next day
2. **Full-day** parts until the start of the month
3. **Full-month** parts
4. **Full-day** parts until the start of the last **part-day** part
5. Finishing **part-day** part.

For starting and finishing **part-day** parts we consider both *ride-types* since we manually iterate through data 
and check for `startDate` and `endDate` directly.

For **N** consecutive **full-day**, or **full-month** parts:

For **1..N-1** such parts we consider all 3 caches types

- **Inside-period cache** considers *ride-type 1* only
- **Between-period cache** considers *ride-type 2* and we can use precalculated cache since it is guaranteed that the following
  period is included fully (for period **N-1** it is guaranteed that period **N** is also included).
- **Three-plus-periods cache** considers *ride-type 2* and since this cache contains indexes of such entries, 
  we can manually check them (they are a tiny amount of entries)

So, this way we consider both *ride-types* in such **1..N-1** parts.

For **Nth** period within **N** consecutive periods we consider the following:

- **Inside-period cache** considers *ride-type 1* only
- We iterate from **index of earliest entry** that is not fully inside this period to the end of this period
  and manually check only entries that are of *ride-type 2*

So, this way we consider both *ride-types* in **Nth** part within **N** consecutive periods.

Given that we considered all entries of both *ride-types* in all **part-day** 
periods and in both **full-day** and **full-month** periods, we found all rides that are between `startDate` and `endDate`.

### Tests 

Test cases were generated using `Main::generateData` and contain 10000 ranges which (from tough debugging experience) 
contain various edge cases for the given data. Luckily, `DEBUG_COLLECT_ITEMS` being set to true was savior for inspection 
for what actual rides were considered.

Some tests were created to merely test the speed of the solutions (and that the solution is not crashing)

- `testDumbCalculator` calculates speed of plain scanning of all entries.
- `testFastCalculator` calculates speed of a solution that finds starting point using binary search and plainly calculates all
  entries until `pickup` > `endDate`.
- `testCachedCalculator` calculates speed of described `cachedCalc` implementation

Some tests were created to compare faster solutions with correct `dumbCalc` one

- `testSameResultsDumbAndFast` checks `fastCalc` for correctness
- `testSameResultsDumbAndCached` checks `cachedCalc` for correctness


**Massive optimizations during implementing task**:

- introducing month cache
- introducing day cache
- going from `Map<Integer, Double>` to `double[]` and from `Map<Integer, Integer>` to `int[]` for calculating 
  per-passenger count statistics.

**Before going to prod** it is recommended to check `dumbCalc` against synthetic data to guarantee 
that it is indeed working correctly.
