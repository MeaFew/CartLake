# CartLake 环境搭建与踩坑记录

> 这台机器：24 线程 / 29GB 内存 / Ubuntu 24.04 无头开发机，全程**无 root、无 Docker**。
> 所有组件为 Apache 官方二进制（国内镜像站下载），单机伪分布式。

## 组件与来源

| 组件 | 版本 | 来源 |
|---|---|---|
| JDK | Temurin 8u432 JRE | adoptium GitHub Release |
| Hadoop | 3.3.6 | 阿里云 Apache 镜像 |
| Hive | 3.1.3 | 华为云 Apache 镜像 |
| Spark | 3.5.1 (hadoop3) | 华为云 Apache 镜像 |
| 元存储 | 内嵌 Derby | Hive 自带 |

## 启动顺序

```bash
hdfs namenode -format -force -nonInteractive   # 仅首次
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver
schematool -dbType derby -initSchema           # 仅首次
```

## 踩坑清单（按出现顺序）

### 1. `MRAppMaster` 找不到主类
提交 MR 作业秒挂，container 报 `找不到或无法加载主类 MRAppMaster`。
**原因**：原生 tarball 的 `mapred-site.xml`/`yarn-site.xml` 没配
`mapreduce.application.classpath` 与 `yarn.application.classpath`。
**修法**：两个 classpath 显式列出 `share/hadoop/{mapreduce,common,hdfs,yarn}/*` 及 `lib/*`，重启 YARN。

### 2. Hadoop Streaming 的 key 截取陷阱
Mapper 输出 `day\tbehavior\t1` 时，框架默认**第一个制表符前才是 key**（即只有 `day`），
`behavior` 不参与排序。Reducer 若假设同 (day,behavior) 连续到达，就会产出几万行碎聚合。
**修法**：复合键拼进单字段 `day_behavior\t1`，零 `-D` 参数，排序天然正确。
（教训 2.5：Reducer 的 `try/except: continue` 会把这类错位**静默吞掉**——调试期去掉防御，让错误喊出来。）

### 3. Hive 3.1.3 + Hadoop 3.3.6 的 guava 冲突
`rm hive/lib/guava-19.0.jar`，用 `hadoop/share/hadoop/common/lib/guava-27.0-jre.jar` 替换。

### 4. Derby 初始化半残
失败的首次启动会创建半个 schema，再次 `initSchema` 报 `NUCLEUS_ASCII already exists`。
**修法**：删掉 metastore_db 目录重新 init。生产环境用 PostgreSQL/MySQL 就没这事。

### 5. `-files` 参数里的 `~` 不展开
bash 的波浪号展开只发生在词首；`-files a.py,b.py` 逗号后必须用 `$HOME` 或绝对路径。

## 数据质量发现

MapReduce 日聚合暴露出天池 UserBehavior 数据集 **0.055%（55,576 条，散布在 338 个脏日期上）
时间戳越界**（窗口 2017-11-25~12-03 之外，散落 2020-2037 年）。DWD 层以
`ts BETWEEN 1511539200 AND 1512316799`（北京 9 天）过滤，这正是"数仓分层"存在的意义：
ODS 保留原貌，DWD 负责清洗口径。

## 时区口径（三引擎对齐的最后一公里）

数据集时间戳约定为**北京时间**。三处坑：Python `datetime.fromtimestamp` 默认 UTC、
Hive `from_unixtime` 跟 JVM 时区（本机 UTC）、Spark `from_unixtime` 跟会话时区（本机 Asia/Shanghai）。
第一次对齐时 MR 用 UTC、Hive 用 UTC、Spark 用北京，三套日表两两对不上。
**修法**：MR mapper 显式 `timezone(timedelta(hours=8))`；Hive 用 `from_unixtime(ts + 28800)`；
对齐后三引擎逐日数字完全一致（如 11-25 buy=201,145，三引擎分毫不差）。

端到端校验：原始层 MR 用户聚合事件总数 **100,150,807**（官方行数一致）、用户数 **987,994**
（官方一致）；清洗后三引擎均为 **100,095,231** 行。
