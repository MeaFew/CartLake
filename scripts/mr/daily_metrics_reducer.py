#!/usr/bin/env python3
"""CartLake MapReduce ETL - Reducer (Hadoop Streaming)

输入: <date>_<behavior><TAB>1 (框架已按 key 全排序,同 key 连续到达)
输出: date<TAB>behavior<TAB>count
"""
import sys

cur_key, cur_sum = None, 0
for line in sys.stdin:
    line = line.rstrip("\n")
    try:
        key, one = line.rsplit("\t", 1)
    except ValueError:
        continue
    if key == cur_key:
        cur_sum += int(one)
    else:
        if cur_key is not None:
            day, behavior = cur_key.rsplit("_", 1)
            print(f"{day}\t{behavior}\t{cur_sum}")
        cur_key, cur_sum = key, int(one)
if cur_key is not None:
    day, behavior = cur_key.rsplit("_", 1)
    print(f"{day}\t{behavior}\t{cur_sum}")
