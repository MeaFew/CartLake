"""CartLake MR streaming 契约测试（无 Hadoop 依赖）。

Hadoop Streaming 的 mapper/reducer 就是 stdin→stdout 程序，
用管道喂样本数据即可在 CI 里验证处理逻辑与时区口径。
"""
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts" / "mr"

# 2017-11-26 16:00:00 UTC = 2017-11-27 00:00:00 CST（跨日界样本，专测 UTC+8 口径）
TS_UTC16 = 1511712000
# 2017-11-26 08:00:00 UTC = 2017-11-26 16:00:00 CST（同日样本）
TS_UTC08 = 1511683200


def run(script, stdin_text):
    r = subprocess.run(
        [sys.executable, str(SCRIPTS / script)],
        input=stdin_text, capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, f"{script} 退出码 {r.returncode}: {r.stderr[:300]}"
    return r.stdout


# ---------- daily_metrics ----------

def test_daily_mapper_basic():
    out = run("daily_metrics_mapper.py",
              f"100,200,300,pv,{TS_UTC08}\n100,200,300,buy,{TS_UTC08}\n")
    lines = sorted(out.strip().split("\n"))
    assert lines == ["2017-11-26_buy\t1", "2017-11-26_pv\t1"]


def test_daily_mapper_timezone_cst():
    """UTC 16:00 应归入北京时间的次日（三引擎时区对齐口径）。"""
    out = run("daily_metrics_mapper.py", f"100,200,300,pv,{TS_UTC16}\n")
    assert out.strip() == "2017-11-27_pv\t1"


def test_daily_mapper_skips_dirty():
    out = run("daily_metrics_mapper.py",
              "100,200,300,click,1511683200\n"      # 非法 behavior
              "1,2,3\n"                              # 列数不足
              "100,200,300,pv,not_a_number\n"        # 坏时间戳
              "100,200,300,pv,99999999999999\n")     # 溢出时间戳
    assert out.strip() == ""


def test_daily_reducer_aggregates():
    out = run("daily_metrics_reducer.py",
              "2017-11-26_pv\t1\n2017-11-26_pv\t1\n2017-11-26_pv\t1\n"
              "2017-11-27_buy\t1\n2017-11-27_buy\t1\n")
    lines = out.strip().split("\n")
    assert lines == ["2017-11-26\tpv\t3", "2017-11-27\tbuy\t2"]


def test_daily_reducer_skips_malformed():
    out = run("daily_metrics_reducer.py", "no_tab_here\n2017-11-26_pv\t1\n")
    assert out.strip() == "2017-11-26\tpv\t1"


def test_daily_pipeline_end_to_end():
    """mapper|sort|reducer 全链路（模拟 streaming 排序语义）。"""
    raw = (f"100,200,300,pv,{TS_UTC08}\n"
           f"101,200,300,pv,{TS_UTC08}\n"
           f"100,200,300,buy,{TS_UTC08}\n"
           f"102,200,300,pv,{TS_UTC16}\n")  # 最后一条跨日到 27 号
    mapped = run("daily_metrics_mapper.py", raw)
    sorted_lines = "\n".join(sorted(mapped.strip().split("\n"))) + "\n"
    out = run("daily_metrics_reducer.py", sorted_lines)
    result = dict()
    for line in out.strip().split("\n"):
        day, behavior, cnt = line.split("\t")
        result[(day, behavior)] = int(cnt)
    assert result == {
        ("2017-11-26", "pv"): 2, ("2017-11-26", "buy"): 1,
        ("2017-11-27", "pv"): 1,
    }


# ---------- user_agg ----------

def test_user_agg_mapper():
    out = run("user_agg_mapper.py",
              f"100,200,300,pv,{TS_UTC08}\n100,200,300,fav,{TS_UTC08}\n"
              f"100,200,300,click,{TS_UTC08}\n")  # click 应被丢弃
    lines = sorted(out.strip().split("\n"))
    assert lines == ["100\tfav\t1", "100\tpv\t1"]


def test_user_agg_reducer():
    out = run("user_agg_reducer.py",
              "100\tbuy\t1\n100\tbuy\t1\n100\tpv\t1\n"
              "101\tcart\t1\n")
    lines = out.strip().split("\n")
    assert lines == ["100\t1\t0\t0\t2", "101\t0\t1\t0\t0"]


# ---------- conf ----------

@pytest.mark.parametrize("xml", ["core-site.xml", "hdfs-site.xml",
                                 "hive-site.xml", "mapred-site.xml",
                                 "yarn-site.xml"])
def test_conf_xml_wellformed(xml):
    import xml.etree.ElementTree as ET
    conf = Path(__file__).resolve().parent.parent / "conf" / xml
    root = ET.parse(conf).getroot()
    assert root.tag == "configuration"
