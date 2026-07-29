# CartLake 🏗️🛒

> 100M 行电商用户行为的 Hadoop 湖仓实战：HDFS + MapReduce + Hive + Spark 全链路，
> 与单机 Python 方案（[ShopLytics](https://github.com/MeaFew/shoplytics)）同数据集对照。

[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-66CCFF?logo=apachehadoop&logoColor=white)]()
[![Hive](https://img.shields.io/badge/Hive-3.1.3-FDEE21?logo=apachehive&logoColor=black)]()
[![Spark](https://img.shields.io/badge/Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)]()
[![Data](https://img.shields.io/badge/数据-1亿行-success)]()

## 为什么有这个仓库

[ShopLytics](https://github.com/MeaFew/shoplytics) 用 pandas/DuckDB 单机分析了阿里云天池
**UserBehavior** 数据集（2017-11-25 ~ 12-03，1 亿条行为日志，3.4GB）。单机方案在
这个规模已经逼近极限——于是有了这个项目：**同一个业务问题，用大数据生态重做一次**，
回答三个问题：

1. 分布式存储与计算（HDFS/MapReduce）如何把 1 亿行 ETL 变成可水平扩展的流水线？
2. Hive 数仓分层（ODS→DWD→ADS）在行为分析场景长什么样？
3. 同一任务在 MapReduce / Hive(MR) / Spark 三种引擎上的工程差异与性能差异？

## 架构

```
UserBehavior.csv (3.4GB, 100,150,807 行)
        │
        ▼  hdfs dfs -put
┌─────────────────────────────────────────────┐
│ HDFS (pseudo-distributed, replication=1)    │
│   /cartlake/raw/user_behavior/   ← ODS 原始层│
└─────────────────────────────────────────────┘
        │
        ├──► MapReduce (Hadoop Streaming, Python)
        │      daily_metrics: 日级行为计数
        │      user_agg: 用户级 pv/cart/fav/buy 聚合
        │
        ├──► Hive 3.1.3 (engine=MR, Parquet)
        │      ODS → DWD(按日期分区) → ADS
        │      漏斗 / 日大盘 / 类目 TOP
        │
        └──► Spark 3.5.1 (PySpark)
               转化漏斗 + RFM 用户分层 + 日指标
```

## 快速复现

> 前置：Linux + Python3。本项目在 24 线程 / 29GB 的开发机上以伪分布式运行，
> 所有组件二进制由国内镜像站下载（见 `docs/SETUP.md`），无需 Docker。

```bash
# 1. 下载并解压 Hadoop/Hive/Spark（脚本内为镜像站地址）
bash scripts/setup_cluster.sh

# 2. 启动 HDFS+YARN 并运行全链路
bash scripts/run_pipeline.sh
```

关键界面（伪分布式默认端口）：
- HDFS NameNode UI: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- Spark UI: http://localhost:18080

## 结果

**端到端校验**（三引擎交叉验证，全部真实运行）：
- 原始层：100,150,807 行 / 987,994 用户 —— 与官方数据卡一致 ✓
- 清洗后（北京 9 天窗口）：三引擎均为 **100,095,231** 行，逐日指标分毫不差 ✓
- 数据质量发现：55,576 条（0.055%）时间戳越界脏数据，散布 338 个异常日期

**转化漏斗**（Hive & Spark 一致）：

| 行为 | 事件数 | 独立用户 | 用户转化 |
|---|---|---|---|
| pv 浏览 | 89,660,688 | 984,105 | — |
| cart 加购 | 5,530,446 | 738,996 | 75.1% |
| fav 收藏 | 2,888,258 | 389,823 | 39.6% |
| buy 购买 | 2,015,839 | 672,404 | **68.3%** |

**RFM 用户分层**（Spark, ntile-5 打分）：

| 分层 | 用户 | 人均行为 | 人均购买 |
|---|---|---|---|
| 高价值活跃 | 160,726 | 182.1 | 2.85 |
| 有购买 | 459,361 | 106.8 | 2.97 |
| 流失预警 | 153,809 | 33.2 | 1.27 |
| 沉默浏览 | 214,095 | 77.8 | 0.00 |

**日大盘**：12-02（周六）为峰值日，DAU 970,401、pv 12,329,644、buy 257,907；
周末效应显著（较工作日 +30% 流量）。

## 基准：三种引擎同任务对比

> 单机伪分布式（24 线程/29GB，YARN 8GB/8vcore 上限），数据 3.4GB CSV → 日级聚合

| 引擎 | 任务 | 耗时 | 备注 |
|---|---|---|---|
| MapReduce (Streaming/Python) | 日级行为计数 | **156s** | 每行过 Python 解释器 |
| MapReduce (Streaming/Python) | 用户级聚合(987,994 用户) | **106s** | reduce 端字典聚合 |
| Hive (engine=MR) | DWD 构建 CSV→Parquet | **228s** | 含列存压缩写入 |
| Hive (engine=MR) | ADS 分析(Parquet 扫描) | **27-69s** | 列存提速 3-8 倍 |
| Spark (local[8]) | 漏斗+RFM+日大盘 全套 | **54s** | 内存计算,一次加载多次复用 |

结论（单机语境，勿外推集群）：Spark 内存计算对迭代式分析有数量级优势；
Hive/Parquet 列存让重复分析远离原始 CSV；MR 的价值在于模型本身——
分片、排序、聚合的显式控制，以及每一行数据流向的可解释性。

## 数据字典

| 列 | 类型 | 说明 |
|---|---|---|
| user_id | int64 | 用户 ID（脱敏） |
| item_id | int64 | 商品 ID |
| category_id | int64 | 类目 ID |
| behavior | string | pv / cart / fav / buy |
| ts | int64 | Unix 秒级时间戳 |

来源：[阿里云天池 - 淘宝用户购物行为数据集](https://tianchi.aliyun.com/dataset/649)

## 仓库结构

```
cartlake/
├── docker/                  # 容器化部署方案(compose,备选)
├── scripts/
│   ├── mr/                  # MapReduce (Hadoop Streaming, Python)
│   ├── hive/                # Hive 数仓 DDL+ADS SQL
│   ├── spark/               # PySpark 漏斗+RFM
│   └── run_pipeline.sh      # 一键全链路
├── docs/SETUP.md            # 环境搭建与踩坑记录
└── results/                 # 真实运行输出
```

## 与 ShopLytics 的关系

| 维度 | ShopLytics | CartLake |
|---|---|---|
| 定位 | 单机分析上限 | 分布式流水线 |
| 栈 | pandas / DuckDB | HDFS / MR / Hive / Spark |
| 数据 | 同一份（天池 UserBehavior） | 同一份 |
| 产出 | 业务洞察报告 | 工程流水线 + 数仓 + 基准 |
