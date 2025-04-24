#
# Copyright 2025 Tabs Data Inc.
#

import json
import random
from pathlib import Path


# Use this function to regenerate sample file if necessary.
def generate(path: str | Path = "data.ndjson", rows: int = 1000):
    with open(path, "w") as f:
        for i in range(rows):
            if i < 200:
                col_i_f = i
                col_b_s = i % 2 == 0
            else:
                col_i_f = round(i + random.uniform(0.1, 0.9), 4)
                col_b_s = random.choice(["A", "B"])
            json.dump({"col_i_f": col_i_f, "col_b_s": col_b_s}, f)
            f.write("\n")
    print(f"Written the ndjson file to '{path}'")


if __name__ == "__main__":
    generate()
