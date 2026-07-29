#!/usr/bin/env bash
# CartLake 全流水线: HDFS 启动 → 入湖 → MapReduce ×2 → Hive 数仓 → Spark 分析
set -uo pipefail
DIST=${CARTLAKE_DIST:-$HOME/cartlake-dist}
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME=$DIST/jdk8
export HADOOP_HOME=$DIST/hadoop-3.3.6
export HIVE_HOME=$DIST/apache-hive-3.1.3-bin
export SPARK_HOME=$DIST/spark-3.5.1-bin-hadoop3
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH
DATA=${1:-$HOME/Projects/shoplytics/data/raw/UserBehavior.csv}

echo "═══ [1/6] HDFS+YARN 启动 ═══"
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver
sleep 8
hdfs dfsadmin -report | head -6

echo "═══ [2/6] 数据入湖 ═══"
hdfs dfs -mkdir -p /cartlake/raw/user_behavior /cartlake/mr_out /cartlake/spark_out
hdfs dfs -put -f "$DATA" /cartlake/raw/user_behavior/
hdfs dfs -du -h /cartlake/raw

echo "═══ [3/6] MapReduce: 日级行为计数 ═══"
time mapred streaming \
  -D mapreduce.job.reduces=4 \
  -files "$REPO/scripts/mr/daily_metrics_mapper.py,$REPO/scripts/mr/daily_metrics_reducer.py" \
  -mapper "python3 daily_metrics_mapper.py" \
  -reducer "python3 daily_metrics_reducer.py" \
  -input /cartlake/raw/user_behavior \
  -output /cartlake/mr_out/daily_metrics
hdfs dfs -cat '/cartlake/mr_out/daily_metrics/part-*' | sort > "$REPO/results/mr_daily_metrics.tsv"

echo "═══ [4/6] MapReduce: 用户聚合 ═══"
time mapred streaming \
  -D mapreduce.job.reduces=8 \
  -files "$REPO/scripts/mr/user_agg_mapper.py,$REPO/scripts/mr/user_agg_reducer.py" \
  -mapper "python3 user_agg_mapper.py" \
  -reducer "python3 user_agg_reducer.py" \
  -input /cartlake/raw/user_behavior \
  -output /cartlake/mr_out/user_agg
hdfs dfs -cat '/cartlake/mr_out/user_agg/part-*' > "$REPO/results/mr_user_agg.tsv"
echo "用户聚合校验: $(wc -l < "$REPO/results/mr_user_agg.tsv") 用户 (应 987,994)"

echo "═══ [5/6] Hive 数仓(ODS→DWD→ADS) ═══"
time hive -f "$REPO/scripts/hive/01_warehouse.sql"

echo "═══ [6/6] Spark 分析(漏斗+RFM) ═══"
time spark-submit --master "local[8]" \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:8020 \
  --conf spark.driver.memory=6g \
  "$REPO/scripts/spark/rfm_funnel.py"

echo "═══ CARTLAKE PIPELINE DONE ═══"
