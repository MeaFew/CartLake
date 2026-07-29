#!/usr/bin/env bash
# CartLake 集群搭建: 下载+解压+配置 Hadoop/Hive/Spark(无 root,无 Docker)
# 用法: bash scripts/setup_cluster.sh [安装目录,默认 ~/cartlake-dist]
set -euo pipefail
DIST=${1:-$HOME/cartlake-dist}
mkdir -p "$DIST" && cd "$DIST"

echo "═══ [1/4] 下载二进制(国内镜像) ═══"
[ -f jdk8.tar.gz ] || curl -L -o jdk8.tar.gz \
  "https://ghfast.top/https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u432-b06/OpenJDK8U-jre_x64_linux_hotspot_8u432b06.tar.gz"
[ -f hadoop.tar.gz ] || curl -L -o hadoop.tar.gz \
  "https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
[ -f hive.tar.gz ] || curl -L -o hive.tar.gz \
  "https://mirrors.huaweicloud.com/apache/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz"
[ -f spark.tgz ] || curl -L -o spark.tgz \
  "https://mirrors.huaweicloud.com/apache/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz"

echo "═══ [2/4] 解压 ═══"
mkdir -p jdk8
tar xzf jdk8.tar.gz -C jdk8 --strip-components=1
tar xzf hadoop.tar.gz
tar xzf hive.tar.gz
tar xzf spark.tgz

echo "═══ [3/4] 注入配置 ═══"
echo "export JAVA_HOME=$DIST/jdk8" >> hadoop-3.3.6/etc/hadoop/hadoop-env.sh
# 仓库内 conf 模板(hdfs/yarn/mapred 参数已按开发机收敛)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp "$SCRIPT_DIR"/../conf/*.xml "$SCRIPT_DIR"/../conf/workers hadoop-3.3.6/etc/hadoop/ 2>/dev/null || true
sed -i "s|/home/baifan/cartlake-dist|$DIST|g" hadoop-3.3.6/etc/hadoop/*.xml

# guava 冲突手术: hive 3.1.3 自带 guava-19 与 hadoop 3.3.6 冲突
rm -f apache-hive-3.1.3-bin/lib/guava-19.0.jar
cp hadoop-3.3.6/share/hadoop/common/lib/guava-27.0-jre.jar apache-hive-3.1.3-bin/lib/
cp "$SCRIPT_DIR"/../conf/hive-site.xml apache-hive-3.1.3-bin/conf/ 2>/dev/null || true
sed -i "s|/home/baifan/cartlake-dist|$DIST|g" apache-hive-3.1.3-bin/conf/hive-site.xml
echo "export HADOOP_HOME=$DIST/hadoop-3.3.6" >> apache-hive-3.1.3-bin/conf/hive-env.sh

echo "═══ [4/4] 初始化 ═══"
export JAVA_HOME=$DIST/jdk8 HADOOP_HOME=$DIST/hadoop-3.3.6
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
hdfs namenode -format -force -nonInteractive
echo "═══ 完成。接下来: bash scripts/run_pipeline.sh ═══"
