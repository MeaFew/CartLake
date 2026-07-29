#!/usr/bin/env python3
"""CartLake MapReduce - 用户行为聚合 Reducer
输入: user_id<TAB>behavior<TAB>1 (按 user_id 排序, behavior 二次序)
输出: user_id<TAB>pv<TAB>cart<TAB>fav<TAB>buy
"""
import sys

cur_user, agg = None, {"pv": 0, "cart": 0, "fav": 0, "buy": 0}

def flush(u, a):
    print(f"{u}\t{a['pv']}\t{a['cart']}\t{a['fav']}\t{a['buy']}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 3:
        continue
    user, behavior, one = parts
    if user != cur_user:
        if cur_user is not None:
            flush(cur_user, agg)
        cur_user, agg = user, {"pv": 0, "cart": 0, "fav": 0, "buy": 0}
    if behavior in agg:
        agg[behavior] += int(one)
if cur_user is not None:
    flush(cur_user, agg)
