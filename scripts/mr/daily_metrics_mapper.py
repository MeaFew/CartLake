#!/usr/bin/env python3
"""CartLake MapReduce ETL - Mapper (Hadoop Streaming)

输入: UserBehavior.csv 行 (user_id,item_id,category_id,behavior_type,timestamp)
输出: <date>_<behavior><TAB>1   —— 复合键拼进单字段(制表符前即 key,
      框架按整 key 排序,无需任何 -D 参数)
"""
import sys
from datetime import datetime, timezone, timedelta

# 数据集时间戳为北京时间(UTC+8),与 Hive from_unixtime 口径对齐
CST = timezone(timedelta(hours=8))

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 5:
        continue
    _, _, _, behavior, ts = parts
    if behavior not in ("pv", "cart", "fav", "buy"):
        continue
    try:
        day = datetime.fromtimestamp(int(ts), tz=CST).strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        continue
    print(f"{day}_{behavior}\t1")
