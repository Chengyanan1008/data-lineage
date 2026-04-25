#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成电商数仓模拟数据 market_sql.csv
- 2000 条记录
- 字段: flow_id, flow_name, flow_owner_user_name, job_id, job_name, output_table_list, job_conetent
- 主题: ODS -> DWD -> DWS -> DWA -> ADS/APP 完整链路
- 包含复杂多阶段线性依赖，模拟真实电商数仓 SQL
"""

import csv
import json
import random
import textwrap
from typing import List, Dict, Tuple

# ─── 基础配置 ────────────────────────────────────────────────────────────────

OWNERS = [
    "zhangwei", "liming", "wangfang", "liujuan", "chengang",
    "zhaolei", "yangmin", "huangtao", "zhoujing", "wuqiang",
    "sunhui", "zhengyong", "luxia", "hexin", "gaoyan",
    "linpeng", "songbo", "tangfei", "dongna", "jiangchao",
]

# 电商业务主题域
BUSINESS_DOMAINS = ["order", "pay", "user", "product", "cart", "coupon",
                    "shop", "logistics", "refund", "review", "search", "ad",
                    "inventory", "category", "brand", "activity", "vip",
                    "recommend", "comment", "flash_sale"]

# 数仓层次
LAYERS = {
    "ods": "ods_ec",
    "dim": "dim_ec",
    "dwd": "dw_ec",
    "dws": "dw_ec",
    "dwa": "dm_ec",
    "ads": "app_ec",
    "app": "app_ec",
}

# 分区字段
PARTITION_FIELDS = ["dt", "dt STRING COMMENT '数据日期'"]

# ─── 表名体系 ─────────────────────────────────────────────────────────────────

def ods_table(domain: str, suffix: str = "di") -> str:
    return f"ods_ec.ods_ec_{domain}_{suffix}"

def dim_table(domain: str, suffix: str = "da") -> str:
    return f"dim_ec.dim_ec_{domain}_{suffix}"

def dwd_table(domain: str, suffix: str = "di") -> str:
    return f"dw_ec.dwd_ec_{domain}_basic_{suffix}"

def dws_table(domain: str, grain: str, suffix: str = "di") -> str:
    return f"dw_ec.dws_ec_{domain}_{grain}_{suffix}"

def dwa_table(domain: str, metric: str, suffix: str = "di") -> str:
    return f"dm_ec.dwa_ec_{domain}_{metric}_{suffix}"

def ads_table(domain: str, metric: str, suffix: str = "di") -> str:
    return f"app_ec.ads_ec_{domain}_{metric}_{suffix}"

def app_table(subject: str, suffix: str = "di") -> str:
    return f"app_ec.app_ec_{subject}_{suffix}"

def tmp_table(base: str) -> str:
    return f"tmp_ec.tmp_{base.split('.')[-1]}"

# ─── SQL 模板库 ───────────────────────────────────────────────────────────────

def sql_header(creator: str, created: str, desc: str, deps: List[str], output: str) -> str:
    dep_lines = "\n".join(f"-- ** {d}" for d in deps)
    return f"""-- =========================================================================
-- **创建人: {creator} {creator}@ecommerce.com
-- **创建时间: {created}
-- **代码描述: {desc}
-- **涉及需求: 电商数仓自动化建设
-- **维护人: {creator} {creator}@ecommerce.com
-- **修改历史:
-- **
-- ===================META BEGIN=============================================
-- **依赖表列表:
-- **
{dep_lines}
-- **OUTPUT TABLE:
-- **
-- ** {output}
-- **
-- ===================META END==============================================="""


def make_ods_sql(domain: str, owner: str) -> Tuple[str, str, str]:
    """ODS 层：从 Kafka/MySQL 摄入原始数据"""
    out_tbl = ods_table(domain)
    tmp_tbl = tmp_table(out_tbl)
    created = f"202{random.randint(1,3)}-0{random.randint(1,9)}-{random.randint(10,28)}"

    domain_fields = {
        "order": [
            ("order_id", "string", "订单ID"),
            ("user_id", "string", "用户ID"),
            ("shop_id", "string", "店铺ID"),
            ("product_id", "string", "商品ID"),
            ("sku_id", "string", "SKU ID"),
            ("order_status", "string", "订单状态 0待付款 1已付款 2已发货 3已完成 4已取消"),
            ("order_amount", "decimal(18,4)", "订单金额"),
            ("discount_amount", "decimal(18,4)", "优惠金额"),
            ("actual_amount", "decimal(18,4)", "实付金额"),
            ("order_time", "string", "下单时间"),
            ("pay_time", "string", "支付时间"),
            ("finish_time", "string", "完成时间"),
            ("cancel_time", "string", "取消时间"),
            ("source_type", "string", "来源类型 app/h5/mini"),
            ("platform", "string", "平台 ios/android/pc"),
            ("region_id", "string", "地区ID"),
            ("channel_id", "string", "渠道ID"),
            ("ctime", "string", "创建时间"),
            ("utime", "string", "更新时间"),
        ],
        "pay": [
            ("pay_id", "string", "支付流水号"),
            ("order_id", "string", "关联订单ID"),
            ("user_id", "string", "用户ID"),
            ("pay_channel", "string", "支付渠道 alipay/wechat/bank"),
            ("pay_amount", "decimal(18,4)", "支付金额"),
            ("pay_currency", "string", "币种 CNY/USD"),
            ("pay_status", "string", "支付状态 0待支付 1成功 2失败 3退款"),
            ("pay_time", "string", "支付时间"),
            ("refund_amount", "decimal(18,4)", "退款金额"),
            ("refund_time", "string", "退款时间"),
            ("third_party_no", "string", "第三方支付流水号"),
            ("ctime", "string", "创建时间"),
            ("utime", "string", "更新时间"),
        ],
        "user": [
            ("user_id", "string", "用户ID"),
            ("username", "string", "用户名"),
            ("phone", "string", "手机号(脱敏)"),
            ("email", "string", "邮箱"),
            ("gender", "string", "性别 0未知 1男 2女"),
            ("age_range", "string", "年龄段"),
            ("register_time", "string", "注册时间"),
            ("register_channel", "string", "注册渠道"),
            ("vip_level", "string", "VIP等级 0-5"),
            ("is_active", "string", "是否活跃 0/1"),
            ("last_login_time", "string", "最近登录时间"),
            ("city_id", "string", "城市ID"),
            ("ctime", "string", "创建时间"),
            ("utime", "string", "更新时间"),
        ],
        "product": [
            ("product_id", "string", "商品ID"),
            ("product_name", "string", "商品名称"),
            ("category_id", "string", "类目ID"),
            ("brand_id", "string", "品牌ID"),
            ("shop_id", "string", "店铺ID"),
            ("price", "decimal(18,4)", "商品价格"),
            ("cost_price", "decimal(18,4)", "成本价"),
            ("stock_qty", "bigint", "库存数量"),
            ("sale_qty", "bigint", "销量"),
            ("status", "string", "商品状态 0下架 1上架"),
            ("create_time", "string", "上架时间"),
            ("ctime", "string", "创建时间"),
            ("utime", "string", "更新时间"),
        ],
        "cart": [
            ("cart_id", "string", "购物车ID"),
            ("user_id", "string", "用户ID"),
            ("product_id", "string", "商品ID"),
            ("sku_id", "string", "SKU ID"),
            ("qty", "bigint", "数量"),
            ("add_time", "string", "加入时间"),
            ("update_time", "string", "更新时间"),
            ("status", "string", "状态 0有效 1已下单 2已删除"),
            ("ctime", "string", "创建时间"),
        ],
        "coupon": [
            ("coupon_id", "string", "优惠券ID"),
            ("user_id", "string", "用户ID"),
            ("coupon_type", "string", "类型 满减/折扣/免邮"),
            ("face_value", "decimal(18,4)", "面值"),
            ("threshold", "decimal(18,4)", "使用门槛"),
            ("discount_rate", "decimal(6,4)", "折扣率"),
            ("use_status", "string", "使用状态 0未使用 1已使用 2已过期"),
            ("get_time", "string", "领取时间"),
            ("use_time", "string", "使用时间"),
            ("expire_time", "string", "过期时间"),
            ("order_id", "string", "关联订单ID"),
            ("ctime", "string", "创建时间"),
        ],
        "logistics": [
            ("logistics_id", "string", "物流ID"),
            ("order_id", "string", "订单ID"),
            ("carrier", "string", "承运商"),
            ("tracking_no", "string", "运单号"),
            ("status", "string", "状态 0待揽收 1运输中 2派送中 3已签收 4异常"),
            ("send_time", "string", "发货时间"),
            ("arrive_time", "string", "到达时间"),
            ("sign_time", "string", "签收时间"),
            ("sender_region", "string", "发货地区"),
            ("receiver_region", "string", "收货地区"),
            ("weight", "decimal(10,3)", "重量kg"),
            ("ctime", "string", "创建时间"),
        ],
        "refund": [
            ("refund_id", "string", "退款ID"),
            ("order_id", "string", "原订单ID"),
            ("user_id", "string", "用户ID"),
            ("refund_type", "string", "退款类型 1仅退款 2退货退款"),
            ("refund_amount", "decimal(18,4)", "退款金额"),
            ("refund_reason", "string", "退款原因"),
            ("refund_status", "string", "状态 0申请中 1已同意 2已拒绝 3已完成"),
            ("apply_time", "string", "申请时间"),
            ("finish_time", "string", "完成时间"),
            ("ctime", "string", "创建时间"),
        ],
        "review": [
            ("review_id", "string", "评价ID"),
            ("order_id", "string", "订单ID"),
            ("user_id", "string", "用户ID"),
            ("product_id", "string", "商品ID"),
            ("shop_id", "string", "店铺ID"),
            ("star", "int", "评分 1-5"),
            ("content", "string", "评价内容"),
            ("has_image", "string", "是否有图 0/1"),
            ("is_anonymous", "string", "是否匿名 0/1"),
            ("helpful_cnt", "bigint", "有用数"),
            ("create_time", "string", "创建时间"),
            ("ctime", "string", "创建时间"),
        ],
        "search": [
            ("search_id", "string", "搜索ID"),
            ("user_id", "string", "用户ID"),
            ("keyword", "string", "搜索关键词"),
            ("result_cnt", "bigint", "结果数量"),
            ("click_product_id", "string", "点击商品ID"),
            ("session_id", "string", "会话ID"),
            ("platform", "string", "平台"),
            ("search_time", "string", "搜索时间"),
            ("ctime", "string", "创建时间"),
        ],
        "activity": [
            ("activity_id", "string", "活动ID"),
            ("activity_name", "string", "活动名称"),
            ("activity_type", "string", "活动类型 秒杀/满减/折扣/拼团"),
            ("start_time", "string", "开始时间"),
            ("end_time", "string", "结束时间"),
            ("budget", "decimal(18,4)", "预算金额"),
            ("actual_cost", "decimal(18,4)", "实际花费"),
            ("status", "string", "状态 0未开始 1进行中 2已结束"),
            ("gmv", "decimal(18,4)", "活动GMV"),
            ("order_cnt", "bigint", "订单数"),
            ("ctime", "string", "创建时间"),
        ],
    }

    fields = domain_fields.get(domain, domain_fields["order"])
    field_defs = "\n".join(
        f"    {f[0]:<30} {f[1]:<25} COMMENT '{f[2]}',"
        for f in fields
    )
    field_names = [f[0] for f in fields]
    select_cols = "\n".join(f"         t.{f}" for f in field_names)

    header = sql_header(owner, created, f"ODS层{domain}业务表数据接入", [f"kafka_stream.kafka_{domain}_log"], out_tbl)

    sql = f"""{header}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET mapred.job.name=ods_ec_{domain}_di;

-- 清理临时表
DROP TABLE IF EXISTS {tmp_tbl};

-- 建临时表
CREATE TABLE IF NOT EXISTS {tmp_tbl} AS
SELECT
{select_cols},
         dt
FROM (
    SELECT
{select_cols},
             regexp_replace(substr(ctime, 1, 10), '-', '') AS dt,
             ROW_NUMBER() OVER (
                 PARTITION BY {field_names[0]}
                 ORDER BY utime DESC NULLS LAST
             ) AS rn
    FROM kafka_stream.kafka_{domain}_log
    WHERE dt = '${{dt}}'
      AND {field_names[0]} IS NOT NULL
      AND {field_names[0]} != ''
) t
WHERE t.rn = 1;

-- 写入目标表
INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
{select_cols}
FROM {tmp_tbl};

-- 数据质量检查
SELECT
    COUNT(1)                                   AS total_cnt,
    COUNT(DISTINCT {field_names[0]})           AS distinct_key_cnt,
    SUM(CASE WHEN {field_names[0]} IS NULL THEN 1 ELSE 0 END) AS null_key_cnt,
    MIN(ctime)                                 AS min_ctime,
    MAX(ctime)                                 AS max_ctime
FROM {out_tbl}
WHERE dt = '${{dt}}';
"""
    return out_tbl, sql, field_names


def make_dwd_sql(domain: str, owner: str, ods_tbl: str, dim_tbls: List[str]) -> Tuple[str, str]:
    """DWD 层：清洗 + 维度关联"""
    out_tbl = dwd_table(domain)
    created = f"202{random.randint(1,3)}-{random.randint(1,12):02d}-{random.randint(10,28)}"
    all_deps = [ods_tbl] + dim_tbls

    domain_enrich = {
        "order": f"""
    -- 订单基础信息清洗
    o.order_id,
    o.user_id,
    o.shop_id,
    o.product_id,
    o.sku_id,
    CASE o.order_status
        WHEN '0' THEN '待付款'
        WHEN '1' THEN '已付款'
        WHEN '2' THEN '已发货'
        WHEN '3' THEN '已完成'
        WHEN '4' THEN '已取消'
        ELSE '未知'
    END                                              AS order_status_desc,
    o.order_status,
    o.order_amount,
    o.discount_amount,
    o.actual_amount,
    COALESCE(o.actual_amount, 0)                     AS actual_amount_clean,
    COALESCE(o.order_amount - o.discount_amount, 0)  AS net_amount,
    o.order_time,
    o.pay_time,
    o.finish_time,
    o.cancel_time,
    DATEDIFF(o.finish_time, o.order_time)            AS order_cycle_days,
    o.source_type,
    o.platform,
    o.region_id,
    r.region_name,
    r.province_name,
    r.city_name,
    o.channel_id,
    c.channel_name,
    c.channel_type,
    u.username,
    u.vip_level,
    u.age_range,
    u.gender""",
        "pay": f"""
    -- 支付基础信息清洗
    p.pay_id,
    p.order_id,
    p.user_id,
    p.pay_channel,
    CASE p.pay_channel
        WHEN 'alipay'  THEN '支付宝'
        WHEN 'wechat'  THEN '微信支付'
        WHEN 'bank'    THEN '银行卡'
        ELSE '其他'
    END                                               AS pay_channel_desc,
    p.pay_amount,
    NVL(p.pay_amount, 0)                             AS pay_amount_clean,
    p.pay_currency,
    CASE p.pay_currency
        WHEN 'CNY'  THEN p.pay_amount
        WHEN 'USD'  THEN p.pay_amount * 7.2
        ELSE p.pay_amount
    END                                               AS pay_amount_cny,
    p.pay_status,
    CASE p.pay_status
        WHEN '0' THEN '待支付'
        WHEN '1' THEN '支付成功'
        WHEN '2' THEN '支付失败'
        WHEN '3' THEN '已退款'
        ELSE '未知'
    END                                               AS pay_status_desc,
    p.pay_time,
    p.refund_amount,
    NVL(p.refund_amount, 0)                          AS refund_amount_clean,
    p.refund_time,
    p.third_party_no,
    u.vip_level,
    u.register_channel""",
        "user": f"""
    -- 用户基础信息清洗
    u.user_id,
    u.username,
    u.phone,
    u.email,
    u.gender,
    CASE u.gender
        WHEN '1' THEN '男'
        WHEN '2' THEN '女'
        ELSE '未知'
    END                                               AS gender_desc,
    u.age_range,
    u.register_time,
    u.register_channel,
    u.vip_level,
    CAST(u.vip_level AS INT)                         AS vip_level_int,
    u.is_active,
    u.last_login_time,
    DATEDIFF('${{dt}}', u.last_login_time)           AS days_since_login,
    u.city_id,
    c.city_name,
    c.province_name,
    c.region_name""",
    }

    enrich_sql = domain_enrich.get(domain, domain_enrich.get("order"))
    header = sql_header(owner, created, f"DWD层{domain}业务宽表清洗", all_deps, out_tbl)

    join_clauses = ""
    if "dim_ec.dim_ec_region_da" in dim_tbls:
        join_clauses += f"\nLEFT JOIN dim_ec.dim_ec_region_da  r ON o.region_id  = r.region_id  AND r.dt = '${{latest_dim_dt}}'"
    if "dim_ec.dim_ec_channel_da" in dim_tbls:
        join_clauses += f"\nLEFT JOIN dim_ec.dim_ec_channel_da c ON o.channel_id = c.channel_id AND c.dt = '${{latest_dim_dt}}'"
    if "dim_ec.dim_ec_user_da" in dim_tbls:
        join_clauses += f"\nLEFT JOIN dim_ec.dim_ec_user_da    u ON o.user_id    = u.user_id    AND u.dt = '${{latest_dim_dt}}'"

    # 主表别名适配
    main_alias = domain[0]
    if domain == "user":
        main_alias = "u"
    elif domain == "pay":
        main_alias = "p"

    sql = f"""{header}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;

-- 获取最新维表分区
SET latest_dim_dt = (
    SELECT MAX(dt) FROM dim_ec.dim_ec_region_da WHERE dt <= '${{dt}}'
);

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT{enrich_sql},
    '${{dt}}'                                        AS dt
FROM {ods_tbl} {main_alias}{join_clauses}
WHERE {main_alias}.dt = '${{dt}}'
  AND {main_alias}.{list({
    "order": "order_id",
    "pay": "pay_id",
    "user": "user_id"
}.values())[0] if False else (
    "order_id" if domain == "order" else
    "pay_id" if domain == "pay" else
    "user_id"
)} IS NOT NULL;
"""
    return out_tbl, sql


def make_dws_sql(domain: str, grain: str, owner: str, dwd_tbls: List[str]) -> Tuple[str, str]:
    """DWS 层：轻度汇总"""
    out_tbl = dws_table(domain, grain)
    created = f"202{random.randint(1,3)}-{random.randint(1,12):02d}-{random.randint(10,28)}"

    grain_configs = {
        "user_day": {
            "group_by": "user_id, dt",
            "metrics": """
    COUNT(DISTINCT order_id)                           AS order_cnt,
    COUNT(DISTINCT CASE WHEN order_status='1' THEN order_id END) AS paid_order_cnt,
    SUM(CASE WHEN order_status='1' THEN actual_amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN order_status='4' THEN 1 ELSE 0 END) AS cancel_order_cnt,
    MAX(order_time)                                    AS last_order_time,
    MIN(order_time)                                    AS first_order_time,
    COUNT(DISTINCT product_id)                         AS distinct_product_cnt,
    COUNT(DISTINCT shop_id)                            AS distinct_shop_cnt,
    SUM(order_amount)                                  AS total_order_amount,
    SUM(discount_amount)                               AS total_discount_amount,
    AVG(actual_amount)                                 AS avg_order_amount""",
        },
        "shop_day": {
            "group_by": "shop_id, dt",
            "metrics": """
    COUNT(DISTINCT order_id)                           AS order_cnt,
    COUNT(DISTINCT user_id)                            AS buyer_cnt,
    COUNT(DISTINCT CASE WHEN order_status='1' THEN order_id END) AS paid_order_cnt,
    COUNT(DISTINCT CASE WHEN order_status='1' THEN user_id END)  AS paid_buyer_cnt,
    SUM(CASE WHEN order_status='1' THEN actual_amount ELSE 0 END) AS gmv,
    SUM(CASE WHEN order_status='4' THEN 1 ELSE 0 END) AS cancel_cnt,
    SUM(order_amount)                                  AS total_order_amount,
    SUM(discount_amount)                               AS total_discount_amount,
    COUNT(DISTINCT product_id)                         AS distinct_product_cnt,
    MAX(order_time)                                    AS last_order_time""",
        },
        "product_day": {
            "group_by": "product_id, sku_id, dt",
            "metrics": """
    COUNT(DISTINCT order_id)                           AS order_cnt,
    COUNT(DISTINCT user_id)                            AS buyer_cnt,
    SUM(CASE WHEN order_status='1' THEN actual_amount ELSE 0 END) AS gmv,
    SUM(CASE WHEN order_status='1' THEN 1 ELSE 0 END) AS paid_order_cnt,
    SUM(CASE WHEN order_status='4' THEN 1 ELSE 0 END) AS cancel_cnt,
    AVG(actual_amount)                                 AS avg_order_price,
    MAX(actual_amount)                                 AS max_order_price,
    MIN(actual_amount)                                 AS min_order_price,
    SUM(discount_amount)                               AS total_discount""",
        },
        "category_day": {
            "group_by": "category_id, dt",
            "metrics": """
    COUNT(DISTINCT order_id)                           AS order_cnt,
    COUNT(DISTINCT user_id)                            AS buyer_cnt,
    COUNT(DISTINCT product_id)                         AS product_cnt,
    SUM(CASE WHEN order_status='1' THEN actual_amount ELSE 0 END) AS gmv,
    SUM(CASE WHEN order_status='1' THEN 1 ELSE 0 END) AS paid_order_cnt,
    SUM(discount_amount)                               AS total_discount,
    AVG(actual_amount)                                 AS avg_order_amount""",
        },
        "pay_channel_day": {
            "group_by": "pay_channel, dt",
            "metrics": """
    COUNT(DISTINCT pay_id)                             AS pay_cnt,
    COUNT(DISTINCT user_id)                            AS pay_user_cnt,
    SUM(CASE WHEN pay_status='1' THEN pay_amount_cny ELSE 0 END)  AS success_amount_cny,
    COUNT(CASE WHEN pay_status='1' THEN 1 END)         AS success_cnt,
    COUNT(CASE WHEN pay_status='2' THEN 1 END)         AS fail_cnt,
    SUM(refund_amount_clean)                           AS refund_amount,
    COUNT(CASE WHEN refund_amount_clean > 0 THEN 1 END) AS refund_cnt,
    AVG(CASE WHEN pay_status='1' THEN pay_amount_cny END) AS avg_pay_amount""",
        },
    }

    cfg = grain_configs.get(grain, grain_configs["user_day"])
    main_tbl = dwd_tbls[0]
    header = sql_header(owner, created, f"DWS层{domain}按{grain}汇总", dwd_tbls, out_tbl)

    sql = f"""{header}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.map.aggr=true;
SET hive.groupby.skewindata=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    {cfg["group_by"].replace(", dt", "").replace(", ", ",\n    ")},{cfg["metrics"]},
    '${{dt}}'  AS dt
FROM {main_tbl}
WHERE dt = '${{dt}}'
  AND order_id IS NOT NULL
GROUP BY {cfg["group_by"].replace(", dt", "")}
HAVING COUNT(1) > 0;
"""
    return out_tbl, sql


def make_dwa_sql(domain: str, metric: str, owner: str, dws_tbls: List[str], dim_tbls: List[str]) -> Tuple[str, str]:
    """DWA 层：面向主题的宽表聚合"""
    out_tbl = dwa_table(domain, metric)
    created = f"202{random.randint(2,3)}-{random.randint(1,12):02d}-{random.randint(10,28)}"
    all_deps = dws_tbls + dim_tbls

    metric_sqls = {
        "user_pay_result": f"""
    -- 用户支付结果宽表
    u.user_id,
    u.username,
    u.vip_level,
    u.gender,
    u.age_range,
    u.city_name,
    u.province_name,
    -- 订单汇总
    NVL(o.order_cnt, 0)                              AS order_cnt,
    NVL(o.paid_order_cnt, 0)                         AS paid_order_cnt,
    NVL(o.paid_amount, 0)                            AS paid_order_amount,
    NVL(o.cancel_order_cnt, 0)                       AS cancel_order_cnt,
    NVL(o.avg_order_amount, 0)                       AS avg_order_amount,
    -- 支付汇总
    NVL(p.pay_cnt, 0)                                AS pay_cnt,
    NVL(p.success_cnt, 0)                            AS pay_success_cnt,
    NVL(p.fail_cnt, 0)                               AS pay_fail_cnt,
    NVL(p.success_amount_cny, 0)                     AS pay_amount_cny,
    NVL(p.refund_amount, 0)                          AS refund_amount,
    NVL(p.refund_cnt, 0)                             AS refund_cnt,
    NVL(p.avg_pay_amount, 0)                         AS avg_pay_amount,
    -- 派生指标
    CASE WHEN NVL(o.order_cnt, 0) > 0
         THEN NVL(o.paid_order_cnt, 0) * 1.0 / o.order_cnt
         ELSE 0
    END                                              AS pay_rate,
    CASE WHEN NVL(p.pay_cnt, 0) > 0
         THEN NVL(p.success_cnt, 0) * 1.0 / p.pay_cnt
         ELSE 0
    END                                              AS pay_success_rate""",
        "shop_gmv_result": f"""
    -- 店铺 GMV 宽表
    s.shop_id,
    s.shop_name,
    s.shop_type,
    s.category_id,
    s.province_name,
    -- 当日指标
    NVL(o.order_cnt, 0)                              AS order_cnt,
    NVL(o.buyer_cnt, 0)                              AS buyer_cnt,
    NVL(o.paid_order_cnt, 0)                         AS paid_order_cnt,
    NVL(o.paid_buyer_cnt, 0)                         AS paid_buyer_cnt,
    NVL(o.gmv, 0)                                    AS gmv,
    NVL(o.cancel_cnt, 0)                             AS cancel_cnt,
    NVL(o.total_discount_amount, 0)                  AS total_discount,
    NVL(o.distinct_product_cnt, 0)                   AS distinct_product_cnt,
    -- 退款指标（关联退款汇总）
    NVL(r.refund_amount, 0)                          AS refund_amount,
    NVL(r.refund_cnt, 0)                             AS refund_cnt,
    -- 派生指标
    CASE WHEN NVL(o.buyer_cnt, 0) > 0
         THEN NVL(o.paid_buyer_cnt, 0) * 1.0 / o.buyer_cnt
         ELSE 0
    END                                              AS pay_buyer_rate,
    NVL(o.gmv, 0) - NVL(r.refund_amount, 0)         AS net_gmv""",
        "product_sale_result": f"""
    -- 商品销售宽表
    p.product_id,
    p.product_name,
    p.category_id,
    c.category_name,
    c.parent_category_id,
    c.parent_category_name,
    p.brand_id,
    b.brand_name,
    p.shop_id,
    -- 销售指标
    NVL(s.order_cnt, 0)                              AS order_cnt,
    NVL(s.buyer_cnt, 0)                              AS buyer_cnt,
    NVL(s.gmv, 0)                                    AS gmv,
    NVL(s.paid_order_cnt, 0)                         AS paid_order_cnt,
    NVL(s.cancel_cnt, 0)                             AS cancel_cnt,
    NVL(s.avg_order_price, 0)                        AS avg_order_price,
    NVL(s.total_discount, 0)                         AS total_discount,
    p.price                                          AS list_price,
    -- 折扣率
    CASE WHEN p.price > 0
         THEN (p.price - NVL(s.avg_order_price, p.price)) / p.price
         ELSE 0
    END                                              AS discount_rate""",
    }

    metric_sql = metric_sqls.get(metric, metric_sqls["user_pay_result"])
    main_dws = dws_tbls[0]
    header = sql_header(owner, created, f"DWA层{domain}{metric}宽表", all_deps, out_tbl)

    # 根据 metric 构造不同 JOIN
    join_sql = ""
    if metric == "user_pay_result":
        join_sql = f"""FROM dim_ec.dim_ec_user_da  u
LEFT JOIN {dws_tbls[0]}   o ON u.user_id = o.user_id  AND o.dt = '${{dt}}'
LEFT JOIN {dws_tbls[1] if len(dws_tbls) > 1 else dws_tbls[0]}   p ON u.user_id = p.user_id  AND p.dt = '${{dt}}'
WHERE u.dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_user_da WHERE dt <= '${{dt}}')"""
    elif metric == "shop_gmv_result":
        join_sql = f"""FROM dim_ec.dim_ec_shop_da  s
LEFT JOIN {dws_tbls[0]}   o ON s.shop_id = o.shop_id  AND o.dt = '${{dt}}'
LEFT JOIN dw_ec.dws_ec_refund_shop_day_di r ON s.shop_id = r.shop_id AND r.dt = '${{dt}}'
WHERE s.dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_shop_da WHERE dt <= '${{dt}}')"""
    else:
        join_sql = f"""FROM dim_ec.dim_ec_product_da p
LEFT JOIN dim_ec.dim_ec_category_da c ON p.category_id = c.category_id AND c.dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_category_da WHERE dt <= '${{dt}}')
LEFT JOIN dim_ec.dim_ec_brand_da    b ON p.brand_id    = b.brand_id    AND b.dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_brand_da    WHERE dt <= '${{dt}}')
LEFT JOIN {dws_tbls[0]}   s ON p.product_id = s.product_id  AND s.dt = '${{dt}}'
WHERE p.dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_product_da WHERE dt <= '${{dt}}')"""

    sql = f"""{header}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;
SET hive.optimize.skewjoin=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT{metric_sql},
    '${{dt}}'  AS dt
{join_sql};
"""
    return out_tbl, sql


def make_ads_sql(subject: str, owner: str, dwa_tbls: List[str], extra_deps: List[str] = None) -> Tuple[str, str]:
    """ADS/APP 层：面向应用的指标聚合"""
    out_tbl = ads_table(subject.split("_")[0], subject)
    created = f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}"
    all_deps = dwa_tbls + (extra_deps or [])

    subject_sqls = {
        "user_rfm_score": f"""
    -- 用户 RFM 打分模型
    user_id,
    username,
    vip_level,
    -- R: Recency 最近购买距今天数
    DATEDIFF('${{dt}}', last_order_time)             AS recency_days,
    CASE
        WHEN DATEDIFF('${{dt}}', last_order_time) <= 7  THEN 5
        WHEN DATEDIFF('${{dt}}', last_order_time) <= 30 THEN 4
        WHEN DATEDIFF('${{dt}}', last_order_time) <= 90 THEN 3
        WHEN DATEDIFF('${{dt}}', last_order_time) <= 180 THEN 2
        ELSE 1
    END                                              AS recency_score,
    -- F: Frequency 累计购买次数（近90天）
    paid_order_cnt_90d,
    CASE
        WHEN paid_order_cnt_90d >= 20 THEN 5
        WHEN paid_order_cnt_90d >= 10 THEN 4
        WHEN paid_order_cnt_90d >= 5  THEN 3
        WHEN paid_order_cnt_90d >= 2  THEN 2
        ELSE 1
    END                                              AS frequency_score,
    -- M: Monetary 消费金额（近90天）
    pay_amount_90d,
    CASE
        WHEN pay_amount_90d >= 10000 THEN 5
        WHEN pay_amount_90d >= 5000  THEN 4
        WHEN pay_amount_90d >= 1000  THEN 3
        WHEN pay_amount_90d >= 300   THEN 2
        ELSE 1
    END                                              AS monetary_score,
    -- 综合 RFM 分数
    (
        CASE WHEN DATEDIFF('${{dt}}', last_order_time) <= 30 THEN 5 ELSE 1 END * 0.3 +
        CASE WHEN paid_order_cnt_90d >= 5 THEN 5 ELSE paid_order_cnt_90d END * 0.3 +
        CASE WHEN pay_amount_90d >= 1000 THEN 5 ELSE 1 END * 0.4
    )                                                AS rfm_score,
    -- 用户分层
    CASE
        WHEN pay_amount_90d >= 10000 AND paid_order_cnt_90d >= 10 THEN '高价值用户'
        WHEN pay_amount_90d >= 1000  AND paid_order_cnt_90d >= 3  THEN '中价值用户'
        WHEN pay_amount_90d > 0                                   THEN '低价值用户'
        WHEN last_order_time IS NOT NULL                          THEN '流失风险用户'
        ELSE '未购买用户'
    END                                              AS user_segment""",
        "shop_daily_kpi": f"""
    -- 店铺每日 KPI 大盘
    shop_id,
    shop_name,
    shop_type,
    category_id,
    province_name,
    -- 当日核心指标
    order_cnt,
    buyer_cnt,
    paid_order_cnt,
    paid_buyer_cnt,
    gmv,
    net_gmv,
    refund_amount,
    refund_cnt,
    total_discount,
    CASE WHEN order_cnt > 0 THEN paid_order_cnt * 1.0 / order_cnt ELSE 0 END AS order_pay_rate,
    CASE WHEN gmv > 0 THEN refund_amount * 1.0 / gmv ELSE 0 END              AS refund_rate,
    CASE WHEN buyer_cnt > 0 THEN gmv * 1.0 / buyer_cnt ELSE 0 END            AS arpu,
    -- 环比（与昨日对比，关联 lag 数据）
    NVL(gmv, 0) - NVL(gmv_yesterday, 0)             AS gmv_mom_diff,
    CASE WHEN NVL(gmv_yesterday, 0) > 0
         THEN (NVL(gmv, 0) - NVL(gmv_yesterday, 0)) / gmv_yesterday
         ELSE NULL
    END                                              AS gmv_mom_rate""",
        "product_hot_rank": f"""
    -- 商品热销排行
    product_id,
    product_name,
    category_id,
    category_name,
    parent_category_name,
    brand_name,
    shop_id,
    gmv,
    order_cnt,
    buyer_cnt,
    paid_order_cnt,
    avg_order_price,
    list_price,
    discount_rate,
    total_discount,
    -- 类目内排名
    ROW_NUMBER() OVER (
        PARTITION BY category_id
        ORDER BY gmv DESC
    )                                                AS rank_in_category,
    -- 全站排名
    ROW_NUMBER() OVER (
        ORDER BY gmv DESC
    )                                                AS rank_overall,
    -- 品牌内排名
    ROW_NUMBER() OVER (
        PARTITION BY brand_name
        ORDER BY gmv DESC
    )                                                AS rank_in_brand""",
        "pay_channel_analysis": f"""
    -- 支付渠道分析
    pay_channel,
    pay_channel_desc,
    pay_cnt,
    pay_user_cnt,
    success_amount_cny,
    success_cnt,
    fail_cnt,
    refund_amount,
    refund_cnt,
    avg_pay_amount,
    CASE WHEN pay_cnt > 0 THEN success_cnt * 1.0 / pay_cnt ELSE 0 END        AS success_rate,
    CASE WHEN pay_cnt > 0 THEN fail_cnt    * 1.0 / pay_cnt ELSE 0 END        AS fail_rate,
    CASE WHEN success_amount_cny > 0
         THEN refund_amount * 1.0 / success_amount_cny
         ELSE 0
    END                                              AS refund_rate,
    -- 与昨日对比
    NVL(success_amount_cny, 0) - NVL(success_amount_cny_yesterday, 0)   AS amount_mom_diff""",
    }

    subject_sql = subject_sqls.get(subject, subject_sqls["shop_daily_kpi"])
    main_tbl = dwa_tbls[0]

    # 构造 FROM/JOIN
    if subject == "user_rfm_score":
        from_sql = f"""FROM (
    SELECT
        a.user_id,
        a.username,
        a.vip_level,
        a.pay_amount_cny                              AS pay_amount_90d,
        a.paid_order_cnt                              AS paid_order_cnt_90d,
        b.last_order_time
    FROM {main_tbl} a
    LEFT JOIN dw_ec.dws_ec_order_user_day_di b
           ON a.user_id = b.user_id
          AND b.dt      = '${{dt}}'
    WHERE a.dt = '${{dt}}'
) t"""
    elif subject == "shop_daily_kpi":
        from_sql = f"""FROM (
    SELECT
        t1.*,
        t2.gmv AS gmv_yesterday
    FROM {main_tbl} t1
    LEFT JOIN {main_tbl} t2
           ON t1.shop_id = t2.shop_id
          AND t2.dt      = DATE_SUB('${{dt}}', 1)
    WHERE t1.dt = '${{dt}}'
) t"""
    elif subject == "pay_channel_analysis":
        from_sql = f"""FROM (
    SELECT
        t1.*,
        t2.success_amount_cny AS success_amount_cny_yesterday
    FROM {main_tbl} t1
    LEFT JOIN {main_tbl} t2
           ON t1.pay_channel = t2.pay_channel
          AND t2.dt          = DATE_SUB('${{dt}}', 1)
    WHERE t1.dt = '${{dt}}'
) t"""
    else:
        from_sql = f"""FROM {main_tbl}
WHERE dt = '${{dt}}'"""

    header = sql_header(owner, created, f"ADS层{subject}指标计算", all_deps, out_tbl)

    sql = f"""{header}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT{subject_sql},
    '${{dt}}'  AS dt
{from_sql};
"""
    return out_tbl, sql


def make_dim_sql(domain: str, owner: str, source: str) -> Tuple[str, str]:
    """DIM 层：维度表"""
    out_tbl = dim_table(domain)
    tmp_tbl = tmp_table(out_tbl)
    created = f"202{random.randint(1,2)}-{random.randint(1,12):02d}-{random.randint(10,28)}"

    dim_fields = {
        "user": ["user_id", "username", "phone", "email", "gender", "age_range",
                 "vip_level", "register_time", "register_channel", "is_active",
                 "last_login_time", "city_id", "city_name", "province_name", "region_name"],
        "product": ["product_id", "product_name", "category_id", "brand_id", "shop_id",
                    "price", "status", "create_time"],
        "shop": ["shop_id", "shop_name", "shop_type", "category_id", "owner_user_id",
                 "province_name", "city_name", "region_id", "status", "create_time"],
        "category": ["category_id", "category_name", "parent_category_id", "parent_category_name",
                     "level", "status"],
        "brand": ["brand_id", "brand_name", "country", "status"],
        "region": ["region_id", "region_name", "province_name", "city_name", "district_name"],
        "channel": ["channel_id", "channel_name", "channel_type", "platform", "status"],
    }

    fields = dim_fields.get(domain, dim_fields["user"])
    select_cols = "\n".join(f"     t.{f}" for f in fields)

    header = sql_header(owner, created, f"DIM维度表{domain}全量刷新", [source], out_tbl)

    sql = f"""{header}

SET hive.exec.parallel=true;

-- 全量覆盖写入维表
DROP TABLE IF EXISTS {tmp_tbl};

CREATE TABLE IF NOT EXISTS {tmp_tbl} AS
SELECT
{select_cols},
     '${{dt}}' AS dt
FROM {source}
WHERE dt = '${{dt}}'
  AND {fields[0]} IS NOT NULL;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
{select_cols}
FROM {tmp_tbl};
"""
    return out_tbl, sql


def make_window_sql(domain: str, grain: str, owner: str, base_tbl: str) -> Tuple[str, str]:
    """带窗口函数的 DWS 汇总（滚动7/30/90天）"""
    out_tbl = dws_table(domain, f"{grain}_rolling")
    created = f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}"

    sql = f"""{sql_header(owner, created, f"DWS层{domain}滚动窗口{grain}指标", [base_tbl], out_tbl)}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.map.aggr=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    user_id,
    -- 近7天
    SUM(paid_order_cnt)  OVER w7                    AS paid_order_cnt_7d,
    SUM(paid_amount)     OVER w7                    AS paid_amount_7d,
    -- 近30天
    SUM(paid_order_cnt)  OVER w30                   AS paid_order_cnt_30d,
    SUM(paid_amount)     OVER w30                   AS paid_amount_30d,
    -- 近90天
    SUM(paid_order_cnt)  OVER w90                   AS paid_order_cnt_90d,
    SUM(paid_amount)     OVER w90                   AS paid_amount_90d,
    -- 用户首单时间
    MIN(first_order_time) OVER (PARTITION BY user_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ever_first_order_time,
    -- 当日
    paid_order_cnt                                  AS paid_order_cnt_1d,
    paid_amount                                     AS paid_amount_1d,
    dt
FROM {base_tbl}
WHERE dt BETWEEN DATE_SUB('${{dt}}', 90) AND '${{dt}}'
WINDOW
    w7  AS (PARTITION BY user_id ORDER BY dt ROWS BETWEEN 6  PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY user_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    w90 AS (PARTITION BY user_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW);
"""
    return out_tbl, sql


def make_funnel_sql(owner: str, deps: List[str]) -> Tuple[str, str]:
    """转化漏斗分析"""
    out_tbl = ads_table("funnel", "order_funnel_analysis")
    created = f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}"

    sql = f"""{sql_header(owner, created, "ADS层电商转化漏斗分析", deps, out_tbl)}

SET hive.exec.parallel=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    channel_id,
    channel_name,
    platform,
    -- 各漏斗层级用户数
    COUNT(DISTINCT visit_user_id)                    AS visit_uv,
    COUNT(DISTINCT search_user_id)                   AS search_uv,
    COUNT(DISTINCT add_cart_user_id)                 AS add_cart_uv,
    COUNT(DISTINCT create_order_user_id)             AS create_order_uv,
    COUNT(DISTINCT pay_user_id)                      AS pay_uv,
    -- 各层转化率
    CASE WHEN COUNT(DISTINCT visit_user_id) > 0
         THEN COUNT(DISTINCT search_user_id) * 1.0 / COUNT(DISTINCT visit_user_id)
         ELSE 0
    END                                              AS visit_to_search_rate,
    CASE WHEN COUNT(DISTINCT search_user_id) > 0
         THEN COUNT(DISTINCT add_cart_user_id) * 1.0 / COUNT(DISTINCT search_user_id)
         ELSE 0
    END                                              AS search_to_cart_rate,
    CASE WHEN COUNT(DISTINCT add_cart_user_id) > 0
         THEN COUNT(DISTINCT create_order_user_id) * 1.0 / COUNT(DISTINCT add_cart_user_id)
         ELSE 0
    END                                              AS cart_to_order_rate,
    CASE WHEN COUNT(DISTINCT create_order_user_id) > 0
         THEN COUNT(DISTINCT pay_user_id) * 1.0 / COUNT(DISTINCT create_order_user_id)
         ELSE 0
    END                                              AS order_to_pay_rate,
    -- 整体转化率
    CASE WHEN COUNT(DISTINCT visit_user_id) > 0
         THEN COUNT(DISTINCT pay_user_id) * 1.0 / COUNT(DISTINCT visit_user_id)
         ELSE 0
    END                                              AS overall_cvr,
    '${{dt}}'  AS dt
FROM (
    SELECT
        s.channel_id,
        c.channel_name,
        s.platform,
        s.user_id                                    AS visit_user_id,
        sr.user_id                                   AS search_user_id,
        ct.user_id                                   AS add_cart_user_id,
        o.user_id                                    AS create_order_user_id,
        p.user_id                                    AS pay_user_id
    FROM dw_ec.dwd_ec_session_basic_di       s
    LEFT JOIN dim_ec.dim_ec_channel_da       c  ON s.channel_id    = c.channel_id
    LEFT JOIN dw_ec.dwd_ec_search_basic_di   sr ON s.user_id       = sr.user_id   AND sr.dt = '${{dt}}'
    LEFT JOIN dw_ec.dwd_ec_cart_basic_di     ct ON s.user_id       = ct.user_id   AND ct.dt = '${{dt}}'
    LEFT JOIN dw_ec.dwd_ec_order_basic_di    o  ON s.user_id       = o.user_id    AND o.dt  = '${{dt}}'
    LEFT JOIN dw_ec.dwd_ec_pay_basic_di      p  ON s.user_id       = p.user_id    AND p.dt  = '${{dt}}'
                                                AND p.pay_status   = '1'
    WHERE s.dt = '${{dt}}'
) t
GROUP BY channel_id, channel_name, platform;
"""
    return out_tbl, sql


def make_cohort_sql(owner: str, deps: List[str]) -> Tuple[str, str]:
    """留存/Cohort 分析"""
    out_tbl = ads_table("user", "cohort_retention_di")
    created = f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}"

    sql = f"""{sql_header(owner, created, "ADS层用户Cohort留存分析", deps, out_tbl)}

SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    register_dt,
    cohort_size,
    retention_day,
    retained_user_cnt,
    CASE WHEN cohort_size > 0
         THEN retained_user_cnt * 1.0 / cohort_size
         ELSE 0
    END                                              AS retention_rate,
    '${{dt}}'  AS dt
FROM (
    SELECT
        u.register_dt,
        COUNT(DISTINCT u.user_id)                    AS cohort_size,
        DATEDIFF(o.dt, u.register_dt)                AS retention_day,
        COUNT(DISTINCT o.user_id)                    AS retained_user_cnt
    FROM (
        SELECT
            user_id,
            regexp_replace(substr(register_time, 1, 10), '-', '') AS register_dt
        FROM dim_ec.dim_ec_user_da
        WHERE dt = (SELECT MAX(dt) FROM dim_ec.dim_ec_user_da WHERE dt <= '${{dt}}')
          AND register_time >= DATE_SUB('${{dt}}', 90)
    ) u
    LEFT JOIN dw_ec.dws_ec_order_user_day_di o
           ON u.user_id = o.user_id
          AND o.dt BETWEEN u.register_dt AND '${{dt}}'
          AND o.paid_order_cnt > 0
    GROUP BY u.register_dt, DATEDIFF(o.dt, u.register_dt)
    HAVING DATEDIFF(o.dt, u.register_dt) BETWEEN 0 AND 30
) t;
"""
    return out_tbl, sql


# ─── 主生成逻辑 ───────────────────────────────────────────────────────────────

def generate_records() -> List[Dict]:
    """生成 2000 条血缘记录"""
    records = []
    flow_id_counter = 1000
    job_id_counter = 10000

    def next_flow_id():
        nonlocal flow_id_counter
        flow_id_counter += 1
        return flow_id_counter

    def next_job_id():
        nonlocal job_id_counter
        job_id_counter += 1
        return job_id_counter

    def add_record(flow_id, flow_name, owner, job_id, job_name, output_tables, sql_content):
        """追加一条记录，job_content 按 online_sql.csv 格式：纯 SQL 直接存，无 JSON 包裹"""
        records.append({
            "flow_id": flow_id,
            "flow_name": flow_name,
            "flow_owner_user_name": owner,
            "job_id": job_id,
            "job_name": job_name,
            "output_table_list": ",".join(output_tables) if isinstance(output_tables, list) else output_tables,
            "job_conetent": sql_content,
        })

    # ── 1. DIM 层：10 个维度表 ─────────────────────────────────────────────
    dim_domains = ["user", "product", "shop", "category", "brand", "region",
                   "channel", "coupon_type", "logistics_carrier", "activity_type"]
    dim_sources = {
        "user": "ods_ec.ods_ec_user_di",
        "product": "ods_ec.ods_ec_product_di",
        "shop": "ods_ec.ods_ec_shop_di",
        "category": "ods_ec.ods_ec_category_di",
        "brand": "ods_ec.ods_ec_brand_di",
        "region": "ods_ec.ods_ec_region_di",
        "channel": "ods_ec.ods_ec_channel_di",
        "coupon_type": "ods_ec.ods_ec_coupon_type_di",
        "logistics_carrier": "ods_ec.ods_ec_logistics_carrier_di",
        "activity_type": "ods_ec.ods_ec_activity_type_di",
    }

    for d in dim_domains:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_dim_sql(d, owner, dim_sources.get(d, f"ods_ec.ods_ec_{d}_di"))
        tmp_tbl = tmp_table(out_tbl)
        add_record(fid, f"dim_ec_{d}_da", owner, jid, f"dim_ec_{d}_da",
                   [tmp_tbl, out_tbl], sql)

    # ── 2. ODS 层：20 个业务表（每个业务域各 1-2 个来源表）────────────────
    ods_domains = [
        "order", "pay", "user", "product", "cart", "coupon",
        "logistics", "refund", "review", "search",
        "ad_click", "ad_impression", "activity", "flash_sale",
        "shop", "brand", "category", "vip_upgrade", "recommend_click", "session",
    ]

    for d in ods_domains:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl = ods_table(d)
        tmp_tbl = tmp_table(out_tbl)

        # 简单 ODS 接入 SQL
        sql = f"""{sql_header(owner, f"202{random.randint(1,3)}-{random.randint(1,12):02d}-{random.randint(10,28)}",
                              f"ODS层{d}原始数据接入", [f"kafka_stream.kafka_{d}_log"], out_tbl)}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT *
FROM kafka_stream.kafka_{d}_log
WHERE dt = '${{dt}}'
  AND id IS NOT NULL;
"""
        add_record(fid, f"ods_ec_{d}_di", owner, jid, f"ods_ec_{d}_di",
                   [tmp_tbl, out_tbl], sql)

    # ── 3. DWD 层：主要业务域清洗宽表 ──────────────────────────────────────
    dwd_configs = [
        ("order",     [dim_table("region"), dim_table("channel"), dim_table("user")]),
        ("pay",       [dim_table("user")]),
        ("user",      [dim_table("region")]),
        ("product",   [dim_table("category"), dim_table("brand"), dim_table("shop")]),
        ("cart",      [dim_table("product"), dim_table("user")]),
        ("coupon",    [dim_table("user")]),
        ("logistics", [dim_table("region")]),
        ("refund",    [dim_table("user")]),
        ("review",    [dim_table("product"), dim_table("user")]),
        ("search",    [dim_table("user")]),
        ("ad_click",  [dim_table("channel")]),
        ("activity",  [dim_table("category")]),
        ("flash_sale",[dim_table("product")]),
        ("session",   [dim_table("channel"), dim_table("user")]),
        ("vip_upgrade",[dim_table("user")]),
    ]

    for domain, dim_tbls in dwd_configs:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_dwd_sql(domain, owner, ods_table(domain), dim_tbls)
        add_record(fid, f"dwd_ec_{domain}_basic_di", owner, jid,
                   f"dwd_ec_{domain}_basic_di", [out_tbl], sql)

    # ── 4. DWS 层：轻度汇总（多粒度） ──────────────────────────────────────
    dws_configs = [
        ("order", "user_day",         [dwd_table("order")]),
        ("order", "shop_day",         [dwd_table("order")]),
        ("order", "product_day",      [dwd_table("order")]),
        ("order", "category_day",     [dwd_table("order")]),
        ("order", "channel_day",      [dwd_table("order")]),
        ("order", "region_day",       [dwd_table("order")]),
        ("pay",   "pay_channel_day",  [dwd_table("pay")]),
        ("pay",   "user_day",         [dwd_table("pay")]),
        ("user",  "active_day",       [dwd_table("user")]),
        ("cart",  "user_day",         [dwd_table("cart")]),
        ("refund","shop_day",         [dwd_table("refund")]),
        ("review","product_day",      [dwd_table("review")]),
        ("search","keyword_day",      [dwd_table("search")]),
        ("ad_click","channel_day",    [dwd_table("ad_click")]),
        ("logistics","carrier_day",   [dwd_table("logistics")]),
    ]

    for domain, grain, dwd_tbls in dws_configs:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_dws_sql(domain, grain, owner, dwd_tbls)
        name = f"dws_ec_{domain}_{grain}_di"
        add_record(fid, name, owner, jid, name, [out_tbl], sql)

    # ── 5. 滚动窗口 DWS ──────────────────────────────────────────────────
    rolling_configs = [
        ("order", "user",    dws_table("order", "user_day")),
        ("pay",   "user",    dws_table("pay", "user_day")),
        ("cart",  "user",    dws_table("cart", "user_day")),
    ]
    for domain, grain, base in rolling_configs:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_window_sql(domain, grain, owner, base)
        name = f"dws_ec_{domain}_{grain}_rolling_di"
        add_record(fid, name, owner, jid, name, [out_tbl], sql)

    # ── 6. DWA 层：宽表主题 ──────────────────────────────────────────────
    dwa_configs = [
        ("user",    "pay_result",      [dws_table("order","user_day"), dws_table("pay","user_day")],     [dim_table("user")]),
        ("shop",    "gmv_result",      [dws_table("order","shop_day")],                                  [dim_table("shop")]),
        ("product", "sale_result",     [dws_table("order","product_day")],                               [dim_table("product"), dim_table("category"), dim_table("brand")]),
        ("pay",     "channel_result",  [dws_table("pay","pay_channel_day")],                             []),
        ("category","sale_result",     [dws_table("order","category_day")],                              [dim_table("category")]),
        ("ad",      "roi_result",      [dws_table("ad_click","channel_day")],                            [dim_table("channel")]),
        ("user",    "active_result",   [dws_table("user","active_day")],                                 [dim_table("user")]),
        ("refund",  "shop_result",     [dws_table("refund","shop_day")],                                 [dim_table("shop")]),
        ("review",  "product_result",  [dws_table("review","product_day")],                              [dim_table("product")]),
        ("logistics","carrier_result", [dws_table("logistics","carrier_day")],                           []),
    ]

    for domain, metric, dws_tbls, d_tbls in dwa_configs:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_dwa_sql(domain, metric, owner, dws_tbls, d_tbls)
        name = f"dwa_ec_{domain}_{metric}_di"
        add_record(fid, name, owner, jid, name, [out_tbl], sql)

    # ── 7. ADS/APP 层：面向应用指标 ──────────────────────────────────────
    ads_configs = [
        ("user_rfm_score",         [dwa_table("user","pay_result")],         []),
        ("shop_daily_kpi",         [dwa_table("shop","gmv_result")],         []),
        ("product_hot_rank",       [dwa_table("product","sale_result")],     []),
        ("pay_channel_analysis",   [dwa_table("pay","channel_result")],      []),
    ]
    for subject, dwa_tbls, extra in ads_configs:
        owner = random.choice(OWNERS)
        fid = next_flow_id()
        jid = next_job_id()
        out_tbl, sql = make_ads_sql(subject, owner, dwa_tbls, extra)
        name = f"ads_ec_{subject}_di"
        add_record(fid, name, owner, jid, name, [out_tbl], sql)

    # 漏斗 & Cohort
    owner = random.choice(OWNERS)
    fid = next_flow_id(); jid = next_job_id()
    out_tbl, sql = make_funnel_sql(owner, [dwd_table("session"), dwd_table("order"), dwd_table("pay")])
    add_record(fid, "ads_ec_order_funnel_analysis_di", owner, jid,
               "ads_ec_order_funnel_analysis_di", [out_tbl], sql)

    owner = random.choice(OWNERS)
    fid = next_flow_id(); jid = next_job_id()
    out_tbl, sql = make_cohort_sql(owner, [dws_table("order","user_day"), dim_table("user")])
    add_record(fid, "ads_ec_cohort_retention_di", owner, jid,
               "ads_ec_cohort_retention_di", [out_tbl], sql)

    # ── 8. 批量生成剩余记录，用变体填满 2000 条 ──────────────────────────

    current_count = len(records)
    print(f"基础链路已生成 {current_count} 条，开始批量生成剩余 {2000 - current_count} 条...")

    # 扩展业务主题
    extended_domains = [
        "order", "pay", "user", "product", "cart", "coupon",
        "logistics", "refund", "review", "search", "ad_click",
        "activity", "flash_sale", "session", "vip_upgrade",
        "recommend", "shop", "brand", "category", "inventory",
    ]

    # 已有的产出表集合（避免完全重复的 job）
    used_job_names = {r["job_name"] for r in records}

    extra_layers = ["ods", "dwd", "dws", "dwa", "ads"]

    while len(records) < 2000:
        domain = random.choice(extended_domains)
        layer  = random.choice(extra_layers)
        owner  = random.choice(OWNERS)
        suffix_num = random.randint(1, 99)
        fid = next_flow_id()
        jid = next_job_id()

        if layer == "ods":
            # ODS 变体：增量/全量两种模式
            mode = random.choice(["incremental", "full"])
            out_tbl = f"ods_ec.ods_ec_{domain}_{'inc' if mode == 'incremental' else 'full'}_di"
            job_name = f"ods_ec_{domain}_{mode}_di_v{suffix_num}"
            if job_name in used_job_names:
                continue
            used_job_names.add(job_name)
            source_kafka = f"kafka_stream.kafka_{domain}_{'delta' if mode == 'incremental' else 'snapshot'}"
            extra_filter = "AND is_deleted = '0'" if mode == "full" else "AND op_type IN ('I','U')"
            sql = f"""{sql_header(owner, f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}",
                                  f"ODS层{domain}{mode}数据接入", [source_kafka], out_tbl)}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.name=ods_ec_{domain}_{mode};

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    id,
    data,
    op_type,
    op_time,
    '${{dt}}' AS dt
FROM {source_kafka}
WHERE dt = '${{dt}}'
  AND id IS NOT NULL
  {extra_filter};
"""
            add_record(fid, job_name, owner, jid, job_name, [out_tbl], sql)

        elif layer == "dwd":
            ods_src = f"ods_ec.ods_ec_{domain}_di"
            dim_join = random.choice([dim_table("region"), dim_table("user"), dim_table("product"), dim_table("channel")])
            out_tbl  = f"dw_ec.dwd_ec_{domain}_detail_di_v{suffix_num}"
            job_name = f"dwd_ec_{domain}_detail_v{suffix_num}"
            if job_name in used_job_names:
                continue
            used_job_names.add(job_name)
            sql = f"""{sql_header(owner, f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}",
                                  f"DWD层{domain}明细宽表v{suffix_num}", [ods_src, dim_join], out_tbl)}

SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    t.*,
    d.region_name,
    d.province_name,
    CASE WHEN t.status = '1' THEN '有效' ELSE '无效' END AS status_desc,
    NVL(t.amount, 0)                              AS amount_clean,
    '${{dt}}'                                     AS dt
FROM {ods_src} t
LEFT JOIN {dim_join} d
       ON t.region_id = d.region_id
      AND d.dt = (SELECT MAX(dt) FROM {dim_join} WHERE dt <= '${{dt}}')
WHERE t.dt = '${{dt}}'
  AND t.id IS NOT NULL;
"""
            add_record(fid, job_name, owner, jid, job_name, [out_tbl], sql)

        elif layer == "dws":
            grain_opts = ["user_week", "user_month", "shop_week", "shop_month",
                          "product_week", "category_week", "channel_week",
                          "region_day", "platform_day", "vip_day"]
            grain = random.choice(grain_opts)
            dwd_src = f"dw_ec.dwd_ec_{domain}_basic_di"
            out_tbl  = f"dw_ec.dws_ec_{domain}_{grain}_di_v{suffix_num}"
            job_name = f"dws_ec_{domain}_{grain}_v{suffix_num}"
            if job_name in used_job_names:
                continue
            used_job_names.add(job_name)

            period = "7" if "week" in grain else "30" if "month" in grain else "1"
            group_key = grain.split("_")[0] + "_id"
            sql = f"""{sql_header(owner, f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}",
                                  f"DWS层{domain}按{grain}汇总v{suffix_num}", [dwd_src], out_tbl)}

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.map.aggr=true;
SET hive.groupby.skewindata=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    {group_key},
    COUNT(DISTINCT id)                             AS record_cnt,
    COUNT(DISTINCT user_id)                        AS user_cnt,
    SUM(NVL(amount, 0))                            AS total_amount,
    AVG(NVL(amount, 0))                            AS avg_amount,
    MAX(NVL(amount, 0))                            AS max_amount,
    MIN(CASE WHEN amount > 0 THEN amount END)      AS min_positive_amount,
    SUM(CASE WHEN status = '1' THEN 1 ELSE 0 END)  AS success_cnt,
    SUM(CASE WHEN status = '0' THEN 1 ELSE 0 END)  AS fail_cnt,
    COUNT(DISTINCT platform)                       AS platform_cnt,
    MAX(create_time)                               AS last_action_time,
    MIN(create_time)                               AS first_action_time,
    '${{dt}}'                                      AS dt
FROM {dwd_src}
WHERE dt BETWEEN DATE_SUB('${{dt}}', {period}) AND '${{dt}}'
  AND {group_key} IS NOT NULL
GROUP BY {group_key}
HAVING COUNT(1) > 0;
"""
            add_record(fid, job_name, owner, jid, job_name, [out_tbl], sql)

        elif layer == "dwa":
            metric_opts = ["performance_result", "growth_result", "risk_result",
                           "retention_result", "conversion_result", "revenue_result",
                           "cost_result", "roi_result", "ltv_result", "churn_result"]
            metric = random.choice(metric_opts)
            dws_src1 = f"dw_ec.dws_ec_{domain}_user_day_di"
            dws_src2 = f"dw_ec.dws_ec_{domain}_shop_day_di"
            dim_src  = random.choice([dim_table("user"), dim_table("shop"), dim_table("product")])
            out_tbl  = f"dm_ec.dwa_ec_{domain}_{metric}_di_v{suffix_num}"
            job_name = f"dwa_ec_{domain}_{metric}_v{suffix_num}"
            if job_name in used_job_names:
                continue
            used_job_names.add(job_name)
            sql = f"""{sql_header(owner, f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}",
                                  f"DWA层{domain}{metric}宽表v{suffix_num}", [dws_src1, dws_src2, dim_src], out_tbl)}

SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;
SET hive.optimize.skewjoin=true;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    d.user_id,
    d.vip_level,
    d.gender,
    d.age_range,
    NVL(o.record_cnt,    0)                        AS order_cnt,
    NVL(o.total_amount,  0)                        AS order_amount,
    NVL(o.success_cnt,   0)                        AS success_cnt,
    NVL(o.avg_amount,    0)                        AS avg_order_amount,
    NVL(p.total_amount,  0)                        AS pay_amount,
    NVL(p.success_cnt,   0)                        AS pay_success_cnt,
    CASE WHEN NVL(o.record_cnt, 0) > 0
         THEN NVL(o.success_cnt, 0) * 1.0 / o.record_cnt
         ELSE 0
    END                                            AS success_rate,
    NVL(o.total_amount, 0) - NVL(p.total_amount, 0) AS net_amount,
    '${{dt}}'                                      AS dt
FROM {dim_src} d
LEFT JOIN {dws_src1} o ON d.user_id = o.user_id AND o.dt = '${{dt}}'
LEFT JOIN {dws_src2} p ON d.user_id = p.user_id AND p.dt = '${{dt}}'
WHERE d.dt = (SELECT MAX(dt) FROM {dim_src} WHERE dt <= '${{dt}}');
"""
            add_record(fid, job_name, owner, jid, job_name, [out_tbl], sql)

        else:  # ads
            app_opts = ["daily_report", "weekly_summary", "monthly_kpi", "realtime_board",
                        "alert_monitor", "growth_analysis", "ab_test_result",
                        "campaign_report", "category_insight", "user_portrait"]
            app_type = random.choice(app_opts)
            dwa_src  = f"dm_ec.dwa_ec_{domain}_performance_result_di"
            out_tbl  = f"app_ec.app_ec_{domain}_{app_type}_di_v{suffix_num}"
            job_name = f"app_ec_{domain}_{app_type}_v{suffix_num}"
            if job_name in used_job_names:
                continue
            used_job_names.add(job_name)
            sql = f"""{sql_header(owner, f"2023-{random.randint(1,12):02d}-{random.randint(10,28)}",
                                  f"APP层{domain}{app_type}指标v{suffix_num}", [dwa_src], out_tbl)}

SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE {out_tbl}
PARTITION (dt = '${{dt}}')
SELECT
    user_id,
    vip_level,
    gender,
    age_range,
    order_cnt,
    order_amount,
    pay_amount,
    pay_success_cnt,
    success_rate,
    net_amount,
    avg_order_amount,
    -- 排名
    ROW_NUMBER() OVER (ORDER BY pay_amount DESC)    AS rank_by_pay,
    ROW_NUMBER() OVER (ORDER BY order_cnt   DESC)   AS rank_by_order,
    -- 分层标签
    CASE
        WHEN pay_amount >= 5000 THEN 'S级'
        WHEN pay_amount >= 1000 THEN 'A级'
        WHEN pay_amount >= 300  THEN 'B级'
        WHEN pay_amount > 0     THEN 'C级'
        ELSE 'N级'
    END                                            AS user_tier,
    '${{dt}}'                                      AS dt
FROM {dwa_src}
WHERE dt = '${{dt}}'
  AND user_id IS NOT NULL;
"""
            add_record(fid, job_name, owner, jid, job_name, [out_tbl], sql)

    return records[:2000]


# ─── 写 CSV ──────────────────────────────────────────────────────────────────

def write_csv(records: List[Dict], output_path: str):
    fieldnames = ["flow_id", "flow_name", "flow_owner_user_name",
                  "job_id", "job_name", "output_table_list", "job_conetent"]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"已写入 {len(records)} 条记录到 {output_path}")


if __name__ == "__main__":
    import os
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "market_sql.csv")

    print("开始生成电商数仓模拟数据...")
    records = generate_records()
    print(f"共生成 {len(records)} 条记录")
    write_csv(records, output_path)
    print("完成!")
