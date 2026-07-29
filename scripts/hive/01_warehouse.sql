-- CartLake Hive 数仓: 原始层 → 明细层 → 集市层
-- 在 hive-server 容器内用 beeline 执行

CREATE DATABASE IF NOT EXISTS cartlake;
USE cartlake;

-- ODS: 原始行为日志(HDFS 上的 CSV)
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_behavior (
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    STRING,
  ts          BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/cartlake/raw/user_behavior'
TBLPROPERTIES ("skip.header.line.count"="0");

-- DWD: Parquet 明细,按日期分区(列存+分区裁剪)
CREATE TABLE IF NOT EXISTS dwd_user_behavior (
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    STRING,
  ts          BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;

-- 数据清洗: MR 层发现 1.23% 时间戳脏数据(窗口外), DWD 层过滤
INSERT OVERWRITE TABLE dwd_user_behavior PARTITION(dt)
SELECT user_id, item_id, category_id, behavior, ts,
       from_unixtime(CAST(ts AS BIGINT) + 28800, 'yyyy-MM-dd')  -- +8h: 对齐北京时间(会话 JVM 为 UTC) AS dt
FROM ods_user_behavior
WHERE ts BETWEEN 1511539200 AND 1512316799;  -- 2017-11-25 ~ 2017-12-03 UTC

-- ADS: 购买转化漏斗(9 天全量)
CREATE TABLE IF NOT EXISTS ads_funnel AS
SELECT behavior, COUNT(1) AS cnt, COUNT(DISTINCT user_id) AS users
FROM dwd_user_behavior
GROUP BY behavior;

-- ADS: 日级大盘
CREATE TABLE IF NOT EXISTS ads_daily AS
SELECT dt,
       COUNT(DISTINCT user_id)                                   AS dau,
       SUM(CASE WHEN behavior='pv'   THEN 1 ELSE 0 END)          AS pv,
       SUM(CASE WHEN behavior='cart' THEN 1 ELSE 0 END)          AS cart,
       SUM(CASE WHEN behavior='fav'  THEN 1 ELSE 0 END)          AS fav,
       SUM(CASE WHEN behavior='buy'  THEN 1 ELSE 0 END)          AS buy
FROM dwd_user_behavior
GROUP BY dt;

-- ADS: 类目 TOP20
CREATE TABLE IF NOT EXISTS ads_category_top AS
SELECT category_id,
       COUNT(1)                                            AS pv,
       SUM(CASE WHEN behavior='buy' THEN 1 ELSE 0 END)     AS buy,
       ROUND(SUM(CASE WHEN behavior='buy' THEN 1 ELSE 0 END) / COUNT(1), 6) AS cvr
FROM dwd_user_behavior
WHERE behavior IN ('pv','buy') OR behavior IS NOT NULL
GROUP BY category_id;
