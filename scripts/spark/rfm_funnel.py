#!/usr/bin/env python3
"""CartLake Spark 分析作业: 转化漏斗 + RFM 用户分层

用法:
  spark-submit --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:8020 \
    rfm_funnel.py
"""
from pyspark.sql import SparkSession, functions as F, Window

RAW = "hdfs://localhost:8020/cartlake/raw/user_behavior"
OUT = "hdfs://localhost:8020/cartlake/spark_out"

spark = (SparkSession.builder
         .appName("cartlake-rfm-funnel")
         .config("spark.executor.memory", "4g")
         .config("spark.executor.cores", "4")
         .config("spark.sql.shuffle.partitions", "32")
         .getOrCreate())

df = (spark.read
      .option("header", "false")
      .csv(RAW)
      .toDF("user_id", "item_id", "category_id", "behavior", "ts")
      .withColumn("user_id", F.col("user_id").cast("long"))
      .withColumn("ts", F.col("ts").cast("long"))
      .withColumn("dt", F.from_unixtime("ts", "yyyy-MM-dd"))  # 会话时区=系统 Asia/Shanghai
      .filter(F.col("ts").between(1511539200, 1512316799))  # DWD 同款北京 9 天窗口
      .cache())

total = df.count()
print(f"[cartlake] rows={total}")

# ── 漏斗 ──
funnel = (df.groupBy("behavior").agg(F.count("*").alias("cnt"),
                                     F.countDistinct("user_id").alias("users"))
          .orderBy("behavior"))
funnel.show(truncate=False)
funnel.coalesce(1).write.mode("overwrite").json(f"{OUT}/funnel")

# 逐步转化率
pv_users  = df.filter("behavior='pv'").select("user_id").distinct()
cart_users = df.filter("behavior='cart'").select("user_id").distinct()
buy_users = df.filter("behavior='buy'").select("user_id").distinct()
n_pv, n_cart, n_buy = (x.count() for x in (pv_users, cart_users, buy_users))
print(f"[cartlake] users pv={n_pv} cart={n_cart} buy={n_buy} "
      f"pv→buy={n_buy/n_pv:.4%}")

# ── RFM ──
max_ts = df.agg(F.max("ts")).first()[0]
rfm = (df.groupBy("user_id").agg(
          F.datediff(F.from_unixtime(F.lit(max_ts)),
                     F.from_unixtime(F.max("ts"))).alias("recency_d"),
          F.count("*").alias("frequency"),
          F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_cnt")))

w = Window.orderBy("recency_d")
rfm = rfm.withColumn("r_score", F.ntile(5).over(w))
w2 = Window.orderBy(F.col("frequency").desc())
rfm = rfm.withColumn("f_score", F.ntile(5).over(w2))
rfm = rfm.withColumn("segment",
          F.when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "高价值活跃")
           .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4), "流失预警")
           .when(F.col("buy_cnt") > 0, "有购买")
           .otherwise("沉默浏览"))

seg = rfm.groupBy("segment").agg(F.count("*").alias("users"),
                                 F.round(F.avg("frequency"), 1).alias("avg_acts"),
                                 F.round(F.avg("buy_cnt"), 2).alias("avg_buys"))
seg.show(truncate=False)
seg.coalesce(1).write.mode("overwrite").json(f"{OUT}/rfm_segments")

# ── 日指标 ──
daily = (df.groupBy("dt")
          .agg(F.countDistinct("user_id").alias("dau"),
               F.count("*").alias("events"),
               F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buys"))
          .orderBy("dt"))
daily.show(truncate=False)
daily.coalesce(1).write.mode("overwrite").json(f"{OUT}/daily")

spark.stop()
print("[cartlake] done")
