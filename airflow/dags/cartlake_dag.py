"""CartLake 每日批处理 DAG
采集入湖 → 三引擎处理(MR/Spark/Hive 由 run_pipeline.sh 编排) → 数据质量门禁
DQ 门失败则整条 DAG 失败 (熔断语义)。
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = '/home/baifan/Projects/CartLake'
DATA = '/home/baifan/Projects/Shoplytics/data/raw/UserBehavior.csv'

default_args = {
    'owner': 'azzhe',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cartlake_daily',
    description='CartLake 亿级用户行为湖仓: 入湖→MR→Spark→Hive→DQ门禁',
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule='0 3 * * *',          # 每日 03:00
    catchup=False,
    max_active_runs=1,
    tags=['cartlake', 'lakehouse'],
) as dag:

    env = 'unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY;'

    ingest_and_process = BashOperator(
        task_id='ingest_and_process',
        bash_command=f'{env} bash {REPO}/scripts/run_pipeline.sh {DATA}',
        execution_timeout=timedelta(hours=2),
    )

    dq_gate = BashOperator(
        task_id='dq_gate',
        bash_command=f'{env} /home/baifan/cartlake-dist/dq-venv/bin/python {REPO}/scripts/dq_checks.py',
        execution_timeout=timedelta(minutes=40),
    )

    ingest_and_process >> dq_gate
