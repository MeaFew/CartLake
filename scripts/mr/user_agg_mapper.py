#!/usr/bin/env python3
"""CartLake MapReduce - 用户行为聚合 Mapper
输出: user_id<TAB>behavior<TAB>1
"""
import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 5:
        continue
    user_id, _, _, behavior, _ = parts
    if behavior in ("pv", "cart", "fav", "buy"):
        print(f"{user_id}\t{behavior}\t1")
