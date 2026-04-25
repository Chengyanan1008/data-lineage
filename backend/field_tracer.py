"""
字段级血缘完整追溯引擎
逐层向上追溯到 ODS / 源头，生成结构化口径报告
"""

import re
from sql_parser import (
    detect_layer,
    LAYER_ORDER,
    _extract_cte_map,
    _find_outer_from_tables,
)


# 层级显示名
LAYER_DISPLAY = {
    "ods": "ODS层（原始数据）",
    "dim": "DIM层（维度层）",
    "dwd": "DWD层",
    "dwa": "DWA层",
    "dws": "DWS层",
    "ads": "ADS层",
    "da": "DA层",
    "dm": "DM层（目标）",
    "rpt": "RPT层",
    "tmp": "TMP层（中间）",
    "app": "APP层",
    "unknown": "源系统/外部",
}

# 认为是源头层（不再往上追）
SOURCE_LAYERS = {"ods", "unknown"}


def _resolve_subquery_alias(source_fields: list, output_field: str, job_sql: str) -> list:
    """
    检测 source_fields 里是否只有"别名引用"（alias.field）且字段名与 output_field 同名。
    如果是，说明 source_fields 引用的可能是一个子查询的输出别名，而非真实列。
    此时在 job_sql 里搜索子查询定义中 'as <output_field>' 的实际表达式，
    提取其引用的真实上游字段，作为修正后的 source_fields 返回。
    若找不到或无需修正，原样返回。
    """
    if not source_fields or not job_sql:
        return source_fields

    # 只处理：有 table（别名）且字段名与 output_field 同名的情况
    # 例：[{table: dwd_dp_payment_success_basic_di, field: pay_amount, alias: t3}]
    # 说明是 t3.pay_amount 这种别名引用，t3 是子查询
    alias_ref = [sf for sf in source_fields
                 if sf.get("table") and sf.get("field", "").lower() == output_field.lower()
                 and sf.get("alias")]
    if not alias_ref:
        return source_fields

    # 在 job_sql 里找子查询中 "... as <output_field>" 的表达式
    # 策略：在 SQL 里搜索 "as pay_amount"（忽略大小写），取前面的表达式
    field_lower = output_field.lower()
    # 去注释
    sql_no_comment = re.sub(r"--[^\n]*", "", job_sql)

    # 找所有 "expr as output_field" 出现的位置（函数或列名形式）
    # 匹配形如 sum(...) as pay_amount 或 column_name as pay_amount
    pattern = re.compile(
        r"(\S[^\n]*?)\s+as\s+" + re.escape(field_lower) + r"\b",
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(sql_no_comment))
    if not matches:
        return source_fields

    # 取最后一个匹配（通常是子查询内最终赋值的那个）
    best_match = matches[-1]
    sub_expr = best_match.group(1).strip()

    # 如果 sub_expr 就是纯 output_field 同名（透传），不修正
    if sub_expr.lower() == field_lower or sub_expr.lower() == f"t.{field_lower}":
        return source_fields

    # 从 sub_expr 里提取裸字段名（去函数包装）
    _SQL_KW = {
        "nvl", "coalesce", "sum", "count", "avg", "max", "min",
        "case", "when", "then", "else", "end", "if", "ifnull",
        "round", "cast", "floor", "ceil", "concat", "substr",
        "date_sub", "date_add", "datediff", "unix_timestamp",
        "from_unixtime", "get_json_object", "split", "lpad",
        "isnull", "nullif", "greatest", "least", "null",
        "true", "false", "and", "or", "not", "in", "is",
        "like", "between", "distinct", "over", "partition",
        "by", "order", "rows", "select", "from", "where",
        "int", "bigint", "double", "float", "string", "boolean",
    }
    raw_names = re.findall(r"\b([a-z_][a-z0-9_]*)\b", sub_expr.lower())
    seen: set = set()
    resolved_fields = []
    for name in raw_names:
        if name not in _SQL_KW and not name.isdigit() and name not in seen:
            seen.add(name)
            # 保留原 source_fields 的 table 信息（知道数据从哪张表来）
            resolved_fields.append({
                "table": alias_ref[0].get("table", ""),
                "field": name,
                "alias": alias_ref[0].get("alias", ""),
            })

    return resolved_fields if resolved_fields else source_fields


def extract_filter_conditions(sql_content: str, field_name: str, output_table: str) -> list:
    """
    从 SQL 中提取**与目标字段直接相关**的过滤条件。

    核心策略：
      1. 去注释，定位写入 output_table 的最后一个 INSERT 块
      2. 在该 INSERT 块里，找到字段表达式（含 field_name）出现的行号
      3. 从该行向上回溯，找到**最近包围它的子查询/CTE 块**的起止范围
      4. 只在该范围内扫描 WHERE 子句，提取条件
      5. 对条件进行分类、清理、去重后返回
    """
    if not sql_content:
        return []

    field_lower = field_name.lower()
    tbl_short = output_table.split(".")[-1].lower()

    # ── 1. 去掉纯注释行（保留行数对齐）──
    raw_lines = sql_content.split("\n")
    clean_lines = [
        "" if ln.strip().startswith("--") else ln
        for ln in raw_lines
    ]
    clean_sql = "\n".join(clean_lines)

    # ── 2. 定位目标 INSERT 块 ──
    insert_pos = -1
    for pat in [
        r"insert\s+(?:overwrite|into)\s+(?:table\s+)?" + re.escape(tbl_short),
        r"insert\s+(?:overwrite|into)\s+(?:table\s+)?" + re.escape(output_table.replace(".", r"\.")),
    ]:
        for m in re.finditer(pat, clean_sql, re.IGNORECASE):
            insert_pos = m.start()

    if insert_pos >= 0:
        rest = clean_sql[insert_pos:]
        nxt = re.search(r"\binsert\b", rest[10:], re.IGNORECASE)
        insert_block = rest[: nxt.start() + 10] if nxt else rest
    else:
        insert_block = clean_sql  # fallback

    # ── 3. 找到字段表达式所在行（在 INSERT 块内） ──
    block_lines = insert_block.split("\n")
    field_line_idx = None
    # 匹配 "as pay_amount" 或 "pay_amount as pay_amount" 或 ",pay_amount"（作为输出字段）
    field_pat = re.compile(
        r"(?:as\s+)?" + re.escape(field_lower) + r"\b",
        re.IGNORECASE,
    )
    for i, ln in enumerate(block_lines):
        ln_l = ln.lower().strip()
        # 跳过纯 CREATE TABLE 字段定义行（只有字段名和类型，没有计算逻辑）
        if re.match(r"`?" + re.escape(field_lower) + r"`?\s+\w+\s+comment", ln_l):
            continue
        if field_pat.search(ln_l):
            field_line_idx = i
            # 优先取 "as field_name" 形式的行（是输出字段赋值行）
            if re.search(r"\bas\s+" + re.escape(field_lower) + r"\b", ln_l):
                break  # 找到最精确的赋值行，停止

    # ── 4. 从字段行向上找最近包围它的子查询块 ──
    # 思路：追踪括号深度，从字段行往上扫，找到子查询的开始 (SELECT) 和对应的结束 )
    def find_enclosing_subquery(lines, start_idx):
        """
        从 start_idx 行往上扫，找到最近的 ( SELECT ... ) 子查询块范围。
        返回 (begin_line_idx, end_line_idx)，若未找到则返回 (0, len(lines)-1)
        """
        # 先向下找"结束括号"——找与开始 SELECT 配对的 )
        depth = 0
        end_idx = len(lines) - 1
        for j in range(start_idx, len(lines)):
            for ch in lines[j]:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    if depth == 0:
                        end_idx = j
                        break
                    depth -= 1
            else:
                continue
            break

        # 向上找开头：找 depth 对应的 ( 以及之前的 SELECT
        depth = 0
        begin_idx = 0
        for j in range(start_idx, -1, -1):
            for ch in reversed(lines[j]):
                if ch == ')':
                    depth += 1
                elif ch == '(':
                    if depth == 0:
                        begin_idx = j
                        break
                    depth -= 1
            else:
                continue
            break

        return begin_idx, end_idx

    if field_line_idx is not None:
        begin_idx, end_idx = find_enclosing_subquery(block_lines, field_line_idx)
        target_block = "\n".join(block_lines[begin_idx: end_idx + 1])
    else:
        target_block = insert_block

    # ── 5. 确定最终扫描范围 ──
    # 若字段行所在的最近子查询块内没有 WHERE（或 WHERE 都被 SKIP），
    # 则扩展到整个 INSERT 块扫描（两层都扫，取并集）。
    # 为避免引入完全无关的条件，最终仍通过 KEY_PATTERNS 过滤。
    # 合并：先扫子查询块，再扫整个 INSERT 块，去重。
    scan_blocks = [target_block]
    if target_block != insert_block:
        scan_blocks.append(insert_block)

    # ── 6. 在各 scan_block 里提取 WHERE 条件 ──
    conditions = []
    seen = set()
    raw_conds = []

    for scan_block in scan_blocks:
        where_blocks = re.findall(
            r"\bwhere\b(.+?)(?=\b(?:group\s+by|order\s+by|having|limit|union|insert)\b|$)",
            scan_block,
            re.IGNORECASE | re.DOTALL,
        )
        for wb in where_blocks:
            # 先把 WHERE 块里从 ") t..." 开始的 JOIN 尾巴整体截掉
            wb = re.sub(r"\)\s*t\d*\s+(?:left|right|inner|cross)?\s*(?:join|on)\b.*$",
                        "", wb, flags=re.IGNORECASE | re.DOTALL)
            # 按 AND 拆分（保守：不按 OR 拆，OR 通常是一个完整条件的一部分）
            parts = re.split(r"\band\b", wb, flags=re.IGNORECASE)
            for p in parts:
                p = p.strip().rstrip(";").strip()
                if p:
                    raw_conds.append(p)

    # ── 过滤规则：只保留有业务意义的条件 ──
    SKIP_PATTERNS = [
        r"^ds\s*=\s*['\$]",             # ds='${DATA_DATE}' 纯分区
        r"^\$\{",                         # 纯变量占位符
        r"^rn\s*=\s*1$",                # 窗口函数去重
        r"^substr\s*\(update_time",      # 时间截取
        r"^t\d+\.\w+\s+is\s+null",      # JOIN null 过滤
        r"^1\s*=\s*1$",                 # 恒真条件
        r"appid_productid\s+is\s+null",  # 内部排重
        r"^uptime\s*=",                  # 汇率表分区过滤
        r"^currency\s*=",               # 汇率币种（维表条件）
        r"^need_exclude\s*=\s*'1'",     # dim 表内部字段
        r"\)\s*t\d*\s*(left|right|inner|join|on)\b",  # 子查询尾巴带 join
        r"\bkafka_topic\s*=\s*'",       # 单个 topic 等值（不是 IN 列表）
    ]

    KEY_PATTERNS = [
        (r"ds\s*>=",                       "日期范围"),
        (r"ds\s*<=",                       "日期范围"),
        (r"date_sub|date_add",             "日期范围"),
        (r"order_state\s*=",               "订单状态"),
        (r"state_code\s+in",               "订单状态"),
        (r"\bpay_amount\b",                "金额过滤"),
        (r"is\s+not\s+null",               "非空过滤"),
        (r"nvl\s*\(pay_amount.*\)\s*>\s*0","正数过滤"),
        (r"appid\s+like",                  "AppID 范围"),
        (r"appid\s+in\s*\(",               "AppID 范围"),
        (r"is_test\s*=\s*0",               "排除测试数据"),
        (r"is_delete\s*<>",                "排除删除记录"),
        (r"f_test\s*=\s*0",               "排除测试数据"),
        (r"supplier_currency\s+not\s+in",  "排除特定商品"),
        (r"kafka_topic\s+in",              "数据来源Topic"),
        (r"`database`\s*=|`table`\s*=",    "源表范围"),
        (r"data\[.update_time.\]",         "数据时间范围"),
    ]

    for cond in raw_conds:
        # 清理尾部子查询残留 ") t / ) t1" 及控制变量
        cond = re.sub(r"\)\s*t\d*\s*$", "", cond.strip()).strip()
        cond = re.sub(r"\$\{(?!DATA_DATE)[^}]+\}", "", cond).strip()
        cond_clean = " ".join(cond.split())
        cond_lower = cond_clean.lower()

        if len(cond_clean) < 5:
            continue

        skip = False
        for pat in SKIP_PATTERNS:
            if re.search(pat, cond_clean, re.IGNORECASE):
                skip = True
                break
        if skip:
            continue

        label = None
        for pat, lbl in KEY_PATTERNS:
            if re.search(pat, cond_lower):
                label = lbl
                break
        if label is None:
            continue

        dedup_key = label + ":" + cond_clean[:40].lower()
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # 最终清理展示文本
        display = re.sub(r"\s*--[^'\n]*$", "", cond_clean).strip()
        # 截掉尾部 ) t1 left join ... 残留
        display = re.sub(r"\s*\)\s*t\d*\s.*$", "", display, flags=re.IGNORECASE | re.DOTALL).strip()
        display = re.sub(r"\s+", " ", display).strip()
        if not display or len(display) < 5:
            continue
        display = display if len(display) <= 80 else display[:77] + "…"
        conditions.append({"label": label, "condition": display})

    return conditions


def extract_logic_description(
    expression: str, sql_content: str, field_name: str, job_info: dict
) -> str:
    """
    从 SQL 表达式 + 注释提炼处理逻辑描述
    """
    expr = (expression or "").strip()
    desc_parts = []

    # 1. 优先从代码行内注释提取（--后面的中文注释）
    lines = sql_content.split("\n")
    for line in lines:
        lower_line = line.lower()
        # 找字段名对应的行
        if re.search(r"\b" + re.escape(field_name.lower()) + r"\b", lower_line):
            comment_m = re.search(r"--\s*(.+)$", line)
            if comment_m:
                comment = comment_m.group(1).strip()
                if len(comment) > 2 and comment not in desc_parts:
                    desc_parts.append(comment)

    # 2. 分析表达式本身的转换逻辑
    expr_desc = _analyze_expression(expr, field_name)
    if expr_desc:
        desc_parts.append(expr_desc)

    # 3. 从 job 的头部注释抓关键信息（代码描述行）
    header_desc = _extract_header_comment(sql_content)
    if header_desc:
        desc_parts.append(f"【任务描述】{header_desc}")

    return "；".join(dict.fromkeys(desc_parts))[:300] if desc_parts else "直接透传"


def _analyze_expression(expr: str, field_name: str) -> str:
    """分析表达式推断处理逻辑"""
    if not expr or expr.strip().lower() in (
        field_name.lower(),
        f"{field_name} as {field_name}",
    ):
        return ""
    el = expr.lower()

    # SUM / COUNT / AVG 聚合
    if re.search(r"\bsum\s*\(", el):
        return f"SUM 聚合：{expr.strip()[:100]}"
    if re.search(r"\bcount\s*\(", el):
        return f"COUNT 聚合：{expr.strip()[:100]}"
    if re.search(r"\bavg\s*\(", el):
        return f"AVG 聚合：{expr.strip()[:100]}"

    # CASE WHEN 条件映射
    if re.search(r"\bcase\s+when\b", el):
        return f"条件映射(CASE WHEN)：{expr.strip()[:120]}"

    # NVL / COALESCE 空值处理
    if re.search(r"\b(nvl|coalesce|ifnull)\s*\(", el):
        return f"空值处理：{expr.strip()[:100]}"

    # 货币换算相关
    if re.search(r"(exchange|rate|cny|usd|currency)", el):
        return f"货币换算：{expr.strip()[:100]}"

    # datediff / date_sub 日期运算
    if re.search(r"\b(datediff|date_sub|date_add)\b", el):
        return f"日期计算：{expr.strip()[:100]}"

    # 有别名
    if " as " in el:
        return f"字段映射：{expr.strip()[:100]}"

    return ""


def _extract_header_comment(sql_content: str) -> str:
    """提取 SQL 头部 **代码描述 行"""
    for line in sql_content.split("\n")[:40]:
        m = re.search(r"\*\*代码描述[：:]\s*(.+)", line)
        if m:
            desc = m.group(1).strip()
            if desc and len(desc) > 2:
                return desc[:100]
    return ""


def _get_layer_display(table_name: str, is_target: bool = False) -> str:
    layer = detect_layer(table_name)
    disp = LAYER_DISPLAY.get(layer, layer.upper() + "层")
    if is_target:
        return f"{disp}（目标）"
    return disp


def trace_field_lineage_full(
    table_name: str, field_name: str, graph: dict, max_depth: int = 10
) -> dict:
    """
    完整字段血缘追溯，返回结构化报告数据：
    {
      chain: [          # 按层级从目标到源头排列
        {
          step: 1,
          table, field, layer, layer_display,
          expression,
          logic_desc,   # 处理逻辑描述
          job_name, job_owner, script_type,
          source_fields: [{table, field}]  # 上游字段
        }, ...
      ],
      flow_text: str,   # 数据流向简化文本
      source_tables: [] # 最终源头表列表
    }
    """
    jobs_map = graph["jobs"]
    tables_map = graph["tables"]

    chain = []
    visited = set()

    def _trace(tbl, fld, depth):
        key = f"{tbl}.{fld}"
        if key in visited or depth > max_depth:
            return
        visited.add(key)

        layer = detect_layer(tbl)
        tbl_info = tables_map.get(tbl, {})
        produced_by = tbl_info.get("produced_by", [])

        step = {
            "step": depth,
            "table": tbl,
            "field": fld,
            "layer": layer,
            "layer_display": _get_layer_display(tbl, depth == 0),
            "expression": "",
            "logic_desc": "",
            "job_id": "",
            "job_name": "",
            "job_owner": "",
            "script_type": "",
            "source_fields": [],
            "is_source": layer in SOURCE_LAYERS or not produced_by,
        }

        # 找生产这张表的 job，以及该字段的血缘
        # 优先选择 source_fields 中含有 table 信息的 job（血缘更完整）；
        # 若所有 job 的 source_fields 都没有 table，退而选择 input_tables
        # 数量最多的 job（上游依赖最丰富，更可信）。
        matched = False
        candidate_with_table = None   # (job_id, job, fl) source_fields 有 table 信息
        candidate_fallback   = None   # (job_id, job, fl) source_fields 无 table 但有字段血缘

        for job_id in produced_by:
            job = jobs_map.get(job_id, {})
            field_lineage = job.get("field_lineage", [])

            for fl in field_lineage:
                if fl.get("output_field", "").lower() == fld.lower():
                    has_table = any(sf.get("table") for sf in fl.get("source_fields", []))
                    if has_table:
                        # 优先：source_fields 里有完整表信息
                        if candidate_with_table is None:
                            candidate_with_table = (job_id, job, fl)
                    else:
                        # 次选：只有裸字段名，按 input_tables 数量取最丰富的
                        if candidate_fallback is None or len(job.get("input_tables", [])) > len(
                            jobs_map.get(candidate_fallback[0], {}).get("input_tables", [])
                        ):
                            candidate_fallback = (job_id, job, fl)
                    break  # 同一 job 内找到字段即可

        chosen = candidate_with_table or candidate_fallback
        if chosen:
            job_id, job, fl = chosen
            expr = fl.get("expression", "")
            content = job.get("content", "")
            step["expression"] = expr
            step["logic_desc"] = extract_logic_description(expr, content, fld, job)
            step["job_id"] = job_id
            step["job_name"] = job.get("job_name", "")
            step["job_owner"] = job.get("owner", "")
            step["script_type"] = job.get("script_type", "")

            raw_sf = fl.get("source_fields", [])

            # 修正：若 source_fields 里有 table 引用，但目标字段只是同名透传的别名
            # （例如 t3.pay_amount 实际上是子查询里 sum(nvl(pay_amount_cny,0)) as pay_amount），
            # 则在当前 job 的 SQL 里搜索子查询定义，找到真实的 expression 和字段。
            corrected_sf = _resolve_subquery_alias(raw_sf, fld, content)
            step["source_fields"] = corrected_sf
            matched = True

        # 如果没匹配到字段血缘但有 job，补充 job 信息
        if not matched and produced_by:
            job_id = produced_by[0]
            job = jobs_map.get(job_id, {})
            step["job_id"] = job_id
            step["job_name"] = job.get("job_name", "")
            step["job_owner"] = job.get("owner", "")
            step["script_type"] = job.get("script_type", "")
            content = job.get("content", "")
            # 即使没有字段血缘，也从代码注释提取信息
            step["logic_desc"] = (
                extract_logic_description(fld, content, fld, job)
                or "直接透传（无字段级解析）"
            )

        # 提取该层的过滤条件
        cur_job_content = ""
        cur_job_id_for_filter = step.get("job_id")
        if cur_job_id_for_filter:
            cur_job_for_filter = jobs_map.get(cur_job_id_for_filter, {})
            cur_job_content = cur_job_for_filter.get("content", "")
        step["filter_conditions"] = extract_filter_conditions(cur_job_content, fld, tbl)

        chain.append(step)

        # 递归向上
        if not step["is_source"]:
            next_fields = {}  # table -> [fields]

            # 当前 job 的 CTE 映射（用于展开 CTE 名 → 真实表）
            job_cte_map = {}
            cur_job_id = step.get("job_id")
            if cur_job_id:
                cur_job = jobs_map.get(cur_job_id, {})
                cur_content = cur_job.get("content", "")
                if cur_content:
                    sql_clean = re.sub(r"--[^\n]*", "", cur_content)
                    job_cte_map = _extract_cte_map(
                        sql_clean
                    )  # {cte_name: [real_tables]}

            # 优先用字段血缘里的 source_fields（跳过维表/配置表，它们不是主链路）
            for sf in step["source_fields"]:
                src_tbl = sf.get("table", "")
                src_fld = sf.get("field", "")
                if not src_tbl or not src_fld or src_tbl == tbl:
                    continue
                if detect_layer(src_tbl) in ("dim", "tmp"):
                    continue

                # Fix 1: 如果 src_tbl 是 CTE 名，展开为真实表
                if src_tbl.lower() in job_cte_map:
                    real_tables = job_cte_map[src_tbl.lower()]
                    # 过滤掉其他 CTE 名，取第一个真实表
                    real = next((t for t in real_tables if t not in job_cte_map), None)
                    if real:
                        src_tbl = real
                    elif real_tables:
                        # 二级展开
                        inner = real_tables[0]
                        if inner in job_cte_map:
                            inner_real = next(
                                (t for t in job_cte_map[inner] if t not in job_cte_map),
                                None,
                            )
                            if inner_real:
                                src_tbl = inner_real

                if src_tbl not in next_fields:
                    next_fields[src_tbl] = []
                if src_fld not in next_fields[src_tbl]:
                    next_fields[src_tbl].append(src_fld)

            # fallback：source_fields 没有 table 信息（裸字段名透传）
            # 用 job 的 input_tables，继续用同名字段往上追，但不乱猜
            if not next_fields:
                job_id = step.get("job_id")
                if job_id:
                    job = jobs_map.get(job_id, {})

                    # 确定往上追的字段名
                    # 优先级：
                    # 1. source_fields 里有 field 名（table 为空也没关系，字段名是对的）
                    # 2. expression 里能提取到与 output field 不同的裸列名
                    #    且该列名在候选上游表的 field_lineage 里真实存在
                    # 3. fallback：同名追
                    src_fld_name = fld  # 默认同名追

                    # 情况1：source_fields 里有字段名
                    sf_fields = [sf["field"] for sf in step["source_fields"] if sf.get("field")]
                    if sf_fields:
                        src_fld_name = sf_fields[0]
                    else:
                        # 情况2：从 expression 提取，验证存在于上游表
                        expr_raw = step.get("expression", "")
                        if expr_raw:
                            expr_no_alias = re.sub(r"\s+as\s+\w+\s*$", "", expr_raw.strip(), flags=re.IGNORECASE).strip()
                            SQL_KW = {"nvl", "coalesce", "sum", "count", "avg", "max", "min",
                                      "case", "when", "then", "else", "end", "if", "ifnull",
                                      "round", "cast", "floor", "ceil", "concat", "substr",
                                      "date_sub", "date_add", "datediff", "unix_timestamp",
                                      "from_unixtime", "get_json_object", "split", "lpad",
                                      "isnull", "nullif", "greatest", "least", "null",
                                      "true", "false", "and", "or", "not", "in", "is",
                                      "like", "between", "distinct", "over", "partition",
                                      "by", "order", "rows", "range", "preceding", "following"}
                            raw_candidates = re.findall(r"\b([a-z_][a-z0-9_]*)\b", expr_no_alias.lower())
                            candidates = []
                            seen_c: set = set()
                            for c in raw_candidates:
                                if c not in SQL_KW and not c.isdigit() and c not in seen_c:
                                    seen_c.add(c)
                                    candidates.append(c)
                            # 优先选与 output field 不同的候选（有转换逻辑）
                            non_self = [c for c in candidates if c != fld.lower()]
                            # 进一步验证：候选字段在某个 candidate_table 的 field_lineage 里存在
                            candidate_tables_tmp = [
                                t for t in job.get("input_tables", [])
                                if detect_layer(t) not in ("dim", "tmp")
                            ] or job.get("input_tables", [])
                            def _exists_in_upstream(fname):
                                for t in candidate_tables_tmp:
                                    tbl_info = tables_map.get(t, {})
                                    for jid in tbl_info.get("produced_by", []):
                                        j2 = jobs_map.get(jid, {})
                                        for fl2 in j2.get("field_lineage", []):
                                            if fl2.get("output_field", "").lower() == fname:
                                                return True
                                return False
                            verified = [c for c in non_self if _exists_in_upstream(c)]
                            if verified:
                                src_fld_name = verified[0]
                            # else: 保持 fld（同名追）
                    # 排除维表/配置表，并优先选择字段名包含待追字段名的表，
                    # 或在该表的 field_lineage 中能找到同名字段的表（更精准），
                    # 最终只取 1 张最相关的表，避免分叉污染血缘链路。
                    candidate_tables = [
                        t
                        for t in job.get("input_tables", [])
                        if detect_layer(t) not in ("dim", "tmp")
                    ] or job.get("input_tables", [])

                    def _table_score(t):
                        """分数越高越优先：该表的 produced_by job 里有同名字段血缘 +2，
                        表名含字段关键词 +1，否则 0。"""
                        score = 0
                        tbl_info = tables_map.get(t, {})
                        for jid in tbl_info.get("produced_by", []):
                            j = jobs_map.get(jid, {})
                            for fl2 in j.get("field_lineage", []):
                                if fl2.get("output_field", "").lower() == src_fld_name.lower():
                                    score += 2
                                    break
                        if src_fld_name.lower() in t.lower():
                            score += 1
                        return score

                    best = max(candidate_tables, key=_table_score, default=None)
                    # 如果最高分为 0（没有表有 src_fld_name 字段），
                    # 尝试剥离货币/单位后缀（_cny/_usd/_day 等），用原始字段名再试
                    if best and _table_score(best) == 0:
                        base_fld = re.sub(r"_(cny|usd|day|cnt|total|sum|avg)$", "",
                                          src_fld_name.lower())
                        if base_fld != src_fld_name.lower():
                            def _table_score_base(t):
                                score = 0
                                tbl_info = tables_map.get(t, {})
                                for jid in tbl_info.get("produced_by", []):
                                    j = jobs_map.get(jid, {})
                                    for fl2 in j.get("field_lineage", []):
                                        if fl2.get("output_field", "").lower() == base_fld:
                                            score += 2
                                            break
                                if base_fld in t.lower():
                                    score += 1
                                return score
                            best_base = max(candidate_tables, key=_table_score_base, default=None)
                            if best_base and _table_score_base(best_base) > 0:
                                best = best_base
                                src_fld_name = base_fld
                    if best:
                        next_fields[best] = [src_fld_name]

            for next_tbl, fields in next_fields.items():
                for next_fld in fields[:2]:
                    _trace(next_tbl, next_fld, depth + 1)

    _trace(table_name, field_name, 0)

    # 生成数据流向简化文本
    flow_text = _build_flow_text(chain, table_name, field_name)

    # 找最终源头
    source_tables = list(
        {
            s["table"]
            for s in chain
            if s.get("is_source") or detect_layer(s["table"]) in SOURCE_LAYERS
        }
    )

    return {
        "target_table": table_name,
        "target_field": field_name,
        "chain": chain,
        "flow_text": flow_text,
        "source_tables": source_tables,
        "total_steps": len(chain),
    }


def _build_flow_text(chain: list, table: str, field: str) -> str:
    """生成数据流向文字描述"""
    if not chain:
        return ""

    lines = [f"{table}.{field} 血缘链路\n"]

    # 按步骤深度排序（源头优先）
    sorted_chain = sorted(chain, key=lambda x: -x["step"])

    lines.append("数据流向（从源头到目标）：")
    prev = None
    for step in sorted_chain:
        arrow = "  →" if prev else "  "
        tag = ""
        if step["layer"] in ("ods", "unknown"):
            tag = "[源头]"
        elif step["layer"] in ("dwd", "dwa"):
            tag = f"[{step['layer'].upper()}]"
        logic = step.get("logic_desc", "")
        logic_short = f" [{logic[:60]}]" if logic and logic != "直接透传" else ""
        lines.append(f"{arrow} {step['table']}.{step['field']}{logic_short} {tag}")
        prev = step

    return "\n".join(lines)
