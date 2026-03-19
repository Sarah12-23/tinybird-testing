import duckdb
import os

DATA_PATH = "data/*.parquet"
OUTPUT_FILE = "output.csv"

def main():
    query = f"""
    COPY (
        SELECT *
        FROM read_parquet('{DATA_PATH}')
        WHERE trip_distance IS NOT NULL
          AND trip_distance > 0
          AND trip_distance >
              (SELECT quantile_cont(trip_distance, 0.9)
               FROM read_parquet('{DATA_PATH}')
               WHERE trip_distance IS NOT NULL AND trip_distance > 0)
    )
    TO '{OUTPUT_FILE}' (HEADER, DELIMITER ',');
    """

    print("Running query...")
    duckdb.query(query)
    print(f"Done. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()