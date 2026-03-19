# Tinybird Data Engineer Test

## Problem

Using NYC Yellow Taxi trip data, return all trips where `trip_distance` is above the 90th percentile (0.9 quantile).

---

## Dataset

NYC TLC Trip Record Data:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The dataset contains trip-level records for NYC taxi rides, including distance traveled, timestamps, fares, and location IDs.

---

## Approach

I selected a subset of NYC Yellow Taxi parquet files and processed them using DuckDB.

DuckDB allows querying Parquet files directly without loading them into memory, which simplifies the workflow and improves performance.

The solution:
1. Reads parquet files directly from disk
2. Filters out invalid records (`trip_distance IS NULL` or `<= 0`)
3. Computes the 90th percentile of `trip_distance`
4. Returns all trips where `trip_distance` is greater than this threshold

---

## Query Logic

```sql
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    total_amount
FROM read_parquet('data/*.parquet')
WHERE trip_distance IS NOT NULL
  AND trip_distance > 0
  AND trip_distance >
    (
      SELECT quantile_cont(trip_distance, 0.9)
      FROM read_parquet('data/*.parquet')
      WHERE trip_distance IS NOT NULL
        AND trip_distance > 0
    )
ORDER BY trip_distance DESC;
```

---

## Assumptions

- The 90th percentile is computed across the selected parquet files
- Invalid values (`NULL` or `<= 0`) are excluded
- A subset of files is used for faster execution, but the solution scales to all files

---

## Why DuckDB

- Native support for Parquet files
- No data ingestion or preprocessing required
- Fast analytical queries using SQL
- Minimal setup, making it easy to reproduce

---

## Project Structure

```
tinybird-data-engineer-test/
│
├── main.py
├── requirements.txt
├── README.md
└── data/
```

---

## How to Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Download 1–2 parquet files from the dataset and place them in the `data/` folder  
   (example: `yellow_tripdata_2025-01.parquet`)

3. Run the script:

```bash
python main.py
```

4. Output will be saved as:

```
output.csv
```

---

## Scalability

The solution uses a wildcard path (`data/*.parquet`) and can process multiple files without modification. It can easily scale to the full dataset if required.

---

## Questions / Clarifications

- Should the percentile be computed per file or across all files?
- Should extreme outliers be handled differently?
- Is there a preferred output format (CSV vs Parquet)?

---

## Possible Improvements

- Parallel processing for large datasets
- Partition-based processing (e.g., by month/year)
- Output results as Parquet for better performance
- Add data validation and logging
- Integrate into a data pipeline or API (e.g., Tinybird)

---

## Summary

This solution focuses on simplicity, correctness, and reproducibility.  
It demonstrates how to efficiently process large parquet datasets using SQL-based analytics without unnecessary complexity.
