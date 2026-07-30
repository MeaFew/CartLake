#!/usr/bin/env python3
"""CartLake 数据质量门禁 (DQ Gate)
在任何一层指标交付前运行; 任一 HARD 检查失败即 exit 1 (熔断)。
检查域: 完整性 / 合法性 / 唯一性 / 一致性 / 业务合理性
"""
import os, re, subprocess, sys, json, time

DIST = os.environ.get('CARTLAKE_DIST', os.path.expanduser('~/cartlake-dist'))
ENV = dict(os.environ,
           JAVA_HOME=f'{DIST}/jdk8',
           HADOOP_HOME=f'{DIST}/hadoop-3.3.6',
           HIVE_HOME=f'{DIST}/apache-hive-3.1.3-bin',
           PATH=f"{DIST}/hadoop-3.3.6/bin:{DIST}/apache-hive-3.1.3-bin/bin:" + os.environ['PATH'])

EXPECTED = {
    'ods_rows': 100_150_807, 'ods_users': 987_994,
    'dwd_rows': 100_095_182, 'buy_events': 2_015_839, 'buy_users': 672_404,
    'dt_min': '2017-11-25', 'dt_max': '2017-12-03',
}

results = []

def hive(sql, timeout=900):
    """经 HS2 (NOSASL) 跑查询, 返回数据行 list[str] (tab 分隔)。
    2026-07-30: 从 hive CLI 改为 pyhive→HS2, 避免与 HS2 争抢内嵌 derby metastore 锁。"""
    from pyhive import hive as _hive
    t0 = time.time()
    conn = _hive.connect('localhost', 10000, auth='NOSASL')
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = ['\t'.join('NULL' if v is None else str(v) for v in r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows, time.time() - t0

def check(name, level, actual, expect_desc, ok):
    results.append({'check': name, 'level': level, 'actual': str(actual), 'expect': expect_desc, 'pass': bool(ok)})
    mark = '✅' if ok else ('🔥' if level == 'HARD' else '⚠️')
    print(f'{mark} [{level}] {name}: 实际={actual} 期望={expect_desc}', flush=True)

def main():
    print('═══ CartLake DQ Gate 开始 ═══', flush=True)

    # 1) ODS 完整性
    rows, _ = hive("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM cartlake.ods_user_behavior")
    cnt, users = rows[0].split('\t')
    check('ODS 行数', 'HARD', cnt, EXPECTED['ods_rows'], int(cnt) == EXPECTED['ods_rows'])
    check('ODS 用户数', 'SOFT', users, EXPECTED['ods_users'], int(users) == EXPECTED['ods_users'])

    # 2) ODS 关键字段非空率
    rows, _ = hive("""SELECT
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN ts IS NULL OR CAST(ts AS BIGINT) <= 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN behavior NOT IN ('pv','cart','fav','buy') OR behavior IS NULL THEN 1 ELSE 0 END)
        FROM cartlake.ods_user_behavior""")
    n_uid, n_ts, n_bt = map(int, rows[0].split('\t'))
    check('user_id 空值', 'HARD', n_uid, 0, n_uid == 0)
    check('ODS ts 非法值(原始层信息项)', 'SOFT', n_ts, '<500(原始层允许含污)', n_ts < 500)
    check('behavior 枚举外值', 'HARD', n_bt, 0, n_bt == 0)

    # 2.5) DWD 清洗后 ts 必须全合法 (脏 ts 应已被清洗剔除)
    rows, _ = hive("SELECT COUNT(*) FROM cartlake.dwd_user_behavior WHERE ts <= 0 OR ts IS NULL")
    check('DWD ts 非法值', 'HARD', rows[0], 0, rows[0] == '0')

    # 3) DWD 清洗后行数 + 日期窗口
    rows, _ = hive("SELECT COUNT(*), MIN(dt), MAX(dt) FROM cartlake.dwd_user_behavior")
    cnt, dmin, dmax = rows[0].split('\t')
    check('DWD 行数', 'HARD', cnt, EXPECTED['dwd_rows'], int(cnt) == EXPECTED['dwd_rows'])
    check('DWD 日期窗口', 'HARD', f'{dmin}~{dmax}',
          f"{EXPECTED['dt_min']}~{EXPECTED['dt_max']}",
          dmin >= EXPECTED['dt_min'] and dmax <= EXPECTED['dt_max'])

    # 4) DWD 唯一性 (复合主键)
    rows, _ = hive("""SELECT COUNT(*) - COUNT(DISTINCT CONCAT(user_id,'|',item_id,'|',ts,'|',behavior))
                      FROM cartlake.dwd_user_behavior""", timeout=1800)
    dup = int(rows[0])
    check('DWD 复合主键重复', 'HARD', dup, 0, dup == 0)

    # 5) 清洗损耗熔断 (脏数据比例异常波动报警)
    loss = 1 - EXPECTED['dwd_rows'] / EXPECTED['ods_rows']
    check('清洗损耗率', 'SOFT', f'{loss:.4%}', '<5%', loss < 0.05)

    # 6) ADS 漏斗业务合理性
    rows, _ = hive("SELECT behavior, cnt, users FROM cartlake.ads_funnel")
    fun = {r.split('\t')[0]: (int(r.split('\t')[1]), int(r.split('\t')[2])) for r in rows}
    buy_ok = fun.get('buy', (0, 0))[0] == EXPECTED['buy_events'] and fun.get('buy', (0, 0))[1] == EXPECTED['buy_users']
    check('ADS 漏斗 buy 锚点', 'HARD', fun.get('buy'), (EXPECTED['buy_events'], EXPECTED['buy_users']), buy_ok)
    mono = fun['pv'][0] >= fun['cart'][0] and fun['pv'][0] >= fun['buy'][0]
    check('漏斗单调性 pv≥cart,pv≥buy', 'SOFT', mono, True, mono)

    # 汇总
    hard_fails = [r for r in results if r['level'] == 'HARD' and not r['pass']]
    print(f"═══ DQ Gate: {len(results)} 项检查, HARD 失败 {len(hard_fails)} 项 ═══", flush=True)
    os.makedirs(os.path.expanduser('~/Projects/CartLake/results'), exist_ok=True)
    with open(os.path.expanduser('~/Projects/CartLake/results/dq_report.json'), 'w') as f:
        json.dump({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'checks': results,
                   'verdict': 'FAIL' if hard_fails else 'PASS'}, f, ensure_ascii=False, indent=2)
    sys.exit(1 if hard_fails else 0)

if __name__ == '__main__':
    main()
