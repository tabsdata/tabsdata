#
# Copyright 2025 Tabs Data Inc.
#

import csv
import random
from pathlib import Path


# Use this function to regenerate sample file if necessary.
def generate(path: str | Path = "data.csv", rows: int = 1000):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["col_i_f", "col_b_s"])
        for i in range(rows):
            if i < 200:
                col_i_f = i
                col_b_s = i % 2 == 0
            else:
                col_i_f = round(i + random.uniform(0.1, 0.9), 4)
                col_b_s = random.choice(["A", "B"])
            writer.writerow([col_i_f, col_b_s])
    print(f"Written the csv file to '{path}'")


if __name__ == "__main__":
    generate()
