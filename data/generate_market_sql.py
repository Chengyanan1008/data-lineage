#!/usr/bin/env python3
import csv
import random
import os

# 2000 行
N = 2000
OUTPUT_FILE = "/AI/data-lineage/data/market_sql.csv"

# 主题，覆盖多层级数据血缘路径
themes = [
    "market_sales",
    "market_inventory",
    "market_marketing",
    "market_fraud",
]

# 组合一个ODS->APP的输出表链，取前8个表，便于解析
def make_chain_base(theme_index):
    ods = [
        "ods_sales.order_data_replenish",
        "ods_sales.order_detail",
    ]
    dim  = [
        "dim_sales.dim_sales_summary",
        "dim_sales.dim_customer",
    ]
    dwd  = [
        "dw_dp.dwd_dp_payment_order_basic_di",
        "dw_dp.dwd_dp_payment_success_basic_di",
        "dw_dp.dwd_dp_payment_basic_di",
    ]
    dwa  = ["dw_dp.dwa_sales_agg"]
    dws  = ["dw_dp.dws_sales_report_daily"]
    ads  = ["dw_dp.ads_market_summary"]
    da   = ["dim_dp.dim_dp_aggregates"]
    dm   = ["dm_market.market_dashboard"]
    app  = ["app.market_sales_dashboard", "app.market_sales_summary",
            "app.market_inventory_dashboard"]

    chain = []
    chain.extend(ods)
    chain.extend(dim)
    chain.extend(dwd)
    chain.extend(dwa)
    chain.extend(dws)
    chain.extend(ads)
    chain.extend(da)
    chain.extend(dm)
    chain.extend(app)
    up_to = min(len(chain), 8)
    return ",".join(chain[:up_to])

# 伪造一个单行的复杂 SQL 内容，确保是一行文本
def make_job_content(theme, i):
    # 使用一个紧凑的一行 SQL，包含CTE、聚合、窗口函数等风格
    content = (
        "-- 生产任务: " + theme + "_job_" + str(i) + " 复杂 SQL：CTE+聚合+窗口函数 "
        "WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT 2 AS b) "
        "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn "
        "FROM cte1 JOIN cte2 ON 1=1;"
    )
    # 转成单行文本，CSV 会保留为单行字段
    return content

def make_owner(i):
    users = ["market.ops","data.engineer","etl.master","data.analyst","market.analyzer"]
    return users[i % len(users)]

def main():
    random.seed(42)
    header = ["flow_id","flow_name","flow_owner_user_name","job_id","job_name","output_table_list","job_conetent"]

    # 写入 CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for i in range(1, N+1):
            theme_idx = (i - 1) % len(themes)
            theme = themes[theme_idx]

            flow_id = f"F{str(i).zfill(5)}"
            flow_name = f"Market Data Pipeline - {theme}"
            owner = make_owner(i)

            job_id = f"J{str(i).zfill(5)}"
            job_name = f"{theme}_job_{i}"

            output_table_list = make_chain_base(theme_idx)
            job_conetent = make_job_content(theme, i)  # 单行文本，无换行

            row = [flow_id, flow_name, owner, job_id, job_name, output_table_list, job_conetent]
            writer.writerow(row)

    print("Wrote", N, "rows to market_sql.csv at", OUTPUT_FILE)

if __name__ == "__main__":
    main()