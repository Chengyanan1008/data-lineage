"""
SQL/Script 解析引擎
支持 Hive SQL、Spark SQL、Shell/Bash 脚本中的表血缘与字段血缘分析
"""

import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis
from sqlparse.tokens import Keyword, DML, CTE


# 数据层级识别规则（表名前缀匹配）
LAYER_PATTERNS = {
    "ods": r"^ods_",
    "dim": r"^dim_",
    "dwd": r"^dwd_",
    "dwa": r"^dwa_",
    "dws": r"^dws_",
    "ads": r"^ads_",
    "tmp": r"^tmp_",
    "da": r"^da_",
    "dm": r"^dm_",
    "rpt": r"^rpt_",
    "app": r"^app_",
}

# schema 名到层的精确映射
SCHEMA_LAYER_MAP = {
    "ods_dp": "ods",
    "ods": "ods",
    "dim_dp": "dim",
    "dw_dp": "dwd",
    "dwa_dp": "dwa",
    "dws_dp": "dws",
    "ads_dp": "ads",
    "tmp_dp": "tmp",
    "da_uiue": "da",
    "da_dp": "da",
    "advertising": "da",
    "dm_dp": "dm",
    "rpt_dp": "rpt",
}

LAYER_ORDER = ["ods", "dim", "dwd", "dwa", "dws", "ads", "da", "dm", "rpt", "tmp", "app"]


def detect_layer(table_name: str) -> str:
    """检测表所在的数据层，优先匹配表名前缀，再匹配 schema"""
    name = table_name.lower().strip()
    schema = ""
    table_part = name

    if "." in name:
        parts = name.split(".", 1)
        schema = parts[0]
        table_part = parts[1]

    # 1. 先检查表名（去掉schema后的部分）前缀
    for layer, pattern in LAYER_PATTERNS.items():
        if re.match(pattern, table_part):
            return layer

    # 2. 检查 schema 名称前缀
    if schema:
        for layer, pattern in LAYER_PATTERNS.items():
            if re.match(pattern, schema):
                return layer
        # schema 精确映射
        if schema in SCHEMA_LAYER_MAP:
            return SCHEMA_LAYER_MAP[schema]
        # dw_ 开头的 schema 归入 dwd
        if schema.startswith("dw_"):
            return "dwd"

    return "unknown"


def detect_script_type(content: str) -> str:
    """检测脚本类型"""
    content_lower = content.lower()
    if content.startswith("#!/bin/bash") or content.startswith("#!/bin/sh"):
        return "bash"
    if content.startswith("#!/usr/bin/env python") or content.startswith(
        "#!/usr/bin/python"
    ):
        return "python"
    if re.search(r"\bspark\.(sql|read|write)\b", content_lower):
        return "spark"
    if re.search(
        r"\b(create\s+table|insert\s+into|insert\s+overwrite|select\b)", content_lower
    ):
        return "hive_sql"
    if "#!/bin/bash" in content or "hive -e" in content or "beeline" in content:
        return "bash"
    return "sql"


def extract_tables_from_sql(sql_text: str):
    """
    从 SQL 文本中提取输入表和输出表
    返回: (input_tables, output_tables)
    """
    input_tables = set()
    output_tables = set()

    # 清理注释
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # 提取 INSERT INTO / INSERT OVERWRITE 的目标表
    insert_patterns = [
        r"INSERT\s+OVERWRITE\s+TABLE\s+([\w.]+)",
        r"INSERT\s+INTO\s+TABLE\s+([\w.]+)",
        r"INSERT\s+OVERWRITE\s+([\w.]+)(?:\s+PARTITION|\s+SELECT|\s*$|\s+\()",
        r"INSERT\s+INTO\s+([\w.]+)(?:\s+PARTITION|\s+SELECT|\s*$|\s+\()",
    ]
    for pattern in insert_patterns:
        for m in re.finditer(pattern, sql_clean, re.IGNORECASE):
            tbl = m.group(1).strip().lower()
            if tbl not in ("values", "select", "partition"):
                output_tables.add(tbl)

    # 提取 CREATE TABLE AS SELECT 或 CREATE TABLE IF NOT EXISTS
    create_patterns = [
        r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)",
    ]
    for pattern in create_patterns:
        for m in re.finditer(pattern, sql_clean, re.IGNORECASE):
            tbl = m.group(1).strip().lower()
            output_tables.add(tbl)

    # 提取 FROM 子句中的表（输入表）
    from_patterns = [
        r"FROM\s+([\w.]+)(?:\s+(?:AS\s+)?\w+)?",
        r"JOIN\s+([\w.]+)(?:\s+(?:AS\s+)?\w+)?",
        r"LEFT\s+(?:OUTER\s+)?JOIN\s+([\w.]+)",
        r"RIGHT\s+(?:OUTER\s+)?JOIN\s+([\w.]+)",
        r"INNER\s+JOIN\s+([\w.]+)",
        r"FULL\s+(?:OUTER\s+)?JOIN\s+([\w.]+)",
        r"CROSS\s+JOIN\s+([\w.]+)",
    ]
    for pattern in from_patterns:
        for m in re.finditer(pattern, sql_clean, re.IGNORECASE):
            tbl = m.group(1).strip().lower()
            # 过滤掉关键字、子查询别名等
            if not _is_sql_keyword(tbl) and "(" not in tbl:
                input_tables.add(tbl)

    # 移除 CTE 名称（WITH xxx AS ...）
    cte_names = set()
    for m in re.finditer(r"\bWITH\s+([\w]+)\s+AS\s*\(", sql_clean, re.IGNORECASE):
        cte_names.add(m.group(1).lower())
    # 多个CTE
    for m in re.finditer(r",\s*([\w]+)\s+AS\s*\(", sql_clean, re.IGNORECASE):
        cte_names.add(m.group(1).lower())

    input_tables = input_tables - cte_names - output_tables
    input_tables = {t for t in input_tables if not _is_sql_keyword(t) and len(t) > 1}

    return list(input_tables), list(output_tables)


def extract_tables_from_bash(script: str):
    """从 bash/shell 脚本中提取表名"""
    input_tables = set()
    output_tables = set()

    # 提取 hive -e 内嵌 SQL
    hive_sql_blocks = re.findall(
        r'hive\s+-e\s+["\']([^"\']+)["\']', script, re.IGNORECASE | re.DOTALL
    )
    # 提取 heredoc 风格的 hive SQL
    heredoc_blocks = re.findall(
        r'<<\s*["\']?EOF["\']?\s*(.*?)\s*EOF', script, re.DOTALL | re.IGNORECASE
    )

    for block in hive_sql_blocks + heredoc_blocks:
        inp, out = extract_tables_from_sql(block)
        input_tables.update(inp)
        output_tables.update(out)

    # 提取 LOAD DATA ... INTO TABLE
    for m in re.finditer(
        r"LOAD\s+DATA\s+.*?INTO\s+TABLE\s+([\w.]+)", script, re.IGNORECASE
    ):
        output_tables.add(m.group(1).lower())

    # 提取嵌入在 echo/mysql 中的表名（mysql 导出到 hive）
    mysql_tables = re.findall(r"from\s+([\w]+\.[\w]+)", script, re.IGNORECASE)
    for t in mysql_tables:
        input_tables.add(t.lower())

    # 使用 hive -e select from table 模式
    for m in re.finditer(
        r'hive\s+-e\s+["\']?\s*select\s+.*?from\s+([\w.]+)',
        script,
        re.IGNORECASE | re.DOTALL,
    ):
        input_tables.add(m.group(1).lower())

    return list(input_tables), list(output_tables)


def extract_tables_from_python(script: str):
    """从 Python/Spark 脚本中提取表名"""
    input_tables = set()
    output_tables = set()

    # spark.sql(...)
    sql_blocks = re.findall(
        r'spark\.sql\s*\(\s*["\'{](.*?)["\'}]\s*\)', script, re.DOTALL
    )
    # spark.read.table(...)
    for m in re.finditer(r'spark\.read\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', script):
        input_tables.add(m.group(1).lower())
    # df.write.saveAsTable(...)
    for m in re.finditer(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', script):
        output_tables.add(m.group(1).lower())
    # df.write.insertInto(...)
    for m in re.finditer(r'\.insertInto\s*\(\s*["\']([^"\']+)["\']\s*\)', script):
        output_tables.add(m.group(1).lower())

    for block in sql_blocks:
        inp, out = extract_tables_from_sql(block)
        input_tables.update(inp)
        output_tables.update(out)

    return list(input_tables), list(output_tables)


def _is_sql_keyword(word: str) -> bool:
    """判断是否为 SQL 关键字"""
    keywords = {
        "select",
        "from",
        "where",
        "join",
        "on",
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "as",
        "by",
        "order",
        "group",
        "having",
        "limit",
        "union",
        "all",
        "distinct",
        "case",
        "when",
        "then",
        "else",
        "end",
        "into",
        "values",
        "set",
        "update",
        "delete",
        "insert",
        "create",
        "table",
        "view",
        "index",
        "drop",
        "alter",
        "with",
        "cte",
        "partition",
        "overwrite",
        "local",
        "inpath",
        "external",
        "stored",
        "format",
        "location",
        "tblproperties",
        "if",
        "exists",
        "like",
        "between",
        "true",
        "false",
        "lateral",
        "view",
        "explode",
        "posexplode",
        "inline",
        "json_tuple",
        "row",
        "rows",
        "unbounded",
        "preceding",
        "following",
        "current",
        "over",
        "window",
        "rank",
        "dense_rank",
        "row_number",
        "partition",
        "distribute",
        "sort",
        "cluster",
        "tablesample",
        "using",
        "natural",
        "cross",
        "left",
        "right",
        "inner",
        "outer",
        "full",
        "semi",
        "anti",
    }
    return word.lower() in keywords


def _extract_cte_map(sql_clean: str) -> dict:
    """
    解析 WITH ... AS (...) CTE 块，返回 {cte_name: real_table_name}。
    real_table_name 取 CTE 内部最外层 FROM 的第一张真实表。
    若 CTE 内部来自多张表（JOIN/UNION），返回主表（第一个 FROM 后的表）。
    """
    cte_map = {}  # cte_name -> real_table (str or None 表示纯计算)

    # 找 WITH 关键字（可在 SET 语句之后，但不能是 WITH SERDEPROPERTIES 等）
    with_m = re.search(r"\bWITH\b(?!\s+SERDEPROPERTIES)", sql_clean, re.IGNORECASE)
    if not with_m:
        return cte_map

    rest = sql_clean[with_m.end() :]

    # 逐个解析 cte_name AS ( ... )
    while rest.strip():
        # cte 名称
        name_m = re.match(r"\s*(\w+)\s+AS\s*\(", rest, re.IGNORECASE)
        if not name_m:
            break
        cte_name = name_m.group(1).lower()
        paren_start = name_m.end() - 1  # 指向 (

        # 找匹配的右括号
        depth = 0
        i = paren_start
        while i < len(rest):
            if rest[i] == "(":
                depth += 1
            elif rest[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1

        cte_body = rest[paren_start + 1 : i]  # 括号内内容

        # 从 CTE body 里找所有真实表（排除子 CTE 名）
        # 取最外层 FROM 后第一个 word.word 或 word 作为来源表
        body_no_comments = re.sub(r"--[^\n]*", "", cte_body)
        # 找最外层的 FROM（depth=0）
        real_tables = _find_outer_from_tables(body_no_comments)
        # 排除自身及其他 CTE 名（稍后再过滤）
        cte_map[cte_name] = real_tables  # list of table names found in this CTE

        # 跳过这个 CTE，继续找下一个（, 或 INSERT/SELECT）
        rest = rest[i + 1 :]
        comma_m = re.match(r"\s*,", rest)
        if comma_m:
            rest = rest[comma_m.end() :]
        else:
            break  # 没有逗号说明 CTE 结束

    return cte_map


def _find_outer_from_tables(sql_body: str, _depth_limit: int = 4) -> list:
    """
    找 SQL 片段里所有 FROM/JOIN 后的真实表名（递归进入子查询）。
    优先取最深层（最原始）的表名。
    """
    if _depth_limit <= 0:
        return []
    tables = []
    depth = 0
    i = 0
    while i < len(sql_body):
        c = sql_body[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0:
            for kw in ("FROM", "JOIN"):
                kl = len(kw)
                if sql_body[i : i + kl].upper() == kw:
                    before = sql_body[i - 1] if i > 0 else " "
                    after = sql_body[i + kl] if i + kl < len(sql_body) else " "
                    if not (before.isalnum() or before == "_") and not (
                        after.isalnum() or after == "_"
                    ):
                        rest = sql_body[i + kl :].lstrip()
                        if rest.startswith("("):
                            # 子查询：找到匹配括号，递归解析里面的 FROM
                            d2, j = 0, 0
                            while j < len(rest):
                                if rest[j] == "(":
                                    d2 += 1
                                elif rest[j] == ")":
                                    d2 -= 1
                                    if d2 == 0:
                                        break
                                j += 1
                            inner = rest[1:j]
                            inner_tables = _find_outer_from_tables(
                                inner, _depth_limit - 1
                            )
                            for t in inner_tables:
                                if t not in tables:
                                    tables.append(t)
                        else:
                            tbl_m = re.match(r"([\w.]+)", rest)
                            if tbl_m:
                                tbl = tbl_m.group(1).lower()
                                if not _is_sql_keyword(tbl) and tbl not in tables:
                                    tables.append(tbl)
                    break
        i += 1
    return tables


def extract_field_lineage(sql_text: str, output_table: str):
    """
    提取字段级血缘关系，支持：
    - 同一脚本内多个 INSERT 语句（按目标表匹配）
    - WITH...AS CTE 语法（CTE名解析为真实表）
    - 无 AS 别名时从 CREATE TABLE 字段顺序推断字段名
    返回: [{"output_field": "xxx", "source_fields": [...], "source_tables": [...], "expression": "..."}]
    """
    field_lineage = []

    # 清理行注释（保留换行）
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # ── Fix 1: 解析 CTE 映射 ──────────────────────────────────────────
    cte_map = _extract_cte_map(sql_clean)  # {cte_name: [real_table, ...]}

    # ── Fix 3: 从 CREATE TABLE 提取字段顺序 ──────────────────────────
    create_fields = _extract_create_table_fields(sql_text, output_table)

    # output_table 匹配
    tbl_short = (
        output_table.split(".")[-1].lower()
        if "." in output_table
        else output_table.lower()
    )
    tbl_full = output_table.lower()

    # 找所有 INSERT 块
    insert_positions = [
        m.start()
        for m in re.finditer(
            r"\bINSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?[\w.]+",
            sql_clean,
            re.IGNORECASE,
        )
    ]
    insert_blocks = []
    for i, pos in enumerate(insert_positions):
        end = (
            insert_positions[i + 1] if i + 1 < len(insert_positions) else len(sql_clean)
        )
        insert_blocks.append(sql_clean[pos:end])

    if not insert_blocks:
        sel_m = re.search(r"(SELECT\s+.*)", sql_clean, re.IGNORECASE | re.DOTALL)
        if sel_m:
            insert_blocks = [sel_m.group(0)]

    target_block = None
    for block in insert_blocks:
        m = re.match(
            r"\bINSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?([\w.]+)",
            block.strip(),
            re.IGNORECASE,
        )
        if m:
            bt = m.group(1).lower()
            if bt == tbl_full or bt.split(".")[-1] == tbl_short:
                target_block = block
                break

    if target_block is None and insert_blocks:
        target_block = insert_blocks[0]
    if target_block is None:
        return field_lineage

    select_match = re.search(
        r"INSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?[\w.]+\s*(?:PARTITION\s*\([^)]*\))?\s*(SELECT\s+.*)",
        target_block,
        re.IGNORECASE | re.DOTALL,
    )
    if not select_match:
        select_match = re.search(
            r"(SELECT\s+.*)", target_block, re.IGNORECASE | re.DOTALL
        )
    if not select_match:
        return field_lineage

    select_sql = select_match.group(1)
    cols_part = _extract_select_columns(select_sql)
    if not cols_part:
        return field_lineage

    from_part = select_sql[len(cols_part) :]
    table_aliases = _extract_table_aliases(from_part)

    # ── Fix 1: 把 CTE 名映射到真实表 ────────────────────────────────
    # table_aliases 里 CTE 名当前指向自身，用 cte_map 覆盖
    for cte_name, real_tables in cte_map.items():
        # 过滤掉其他 CTE 名，取第一个非 CTE 的真实表
        real = next((t for t in real_tables if t not in cte_map), None)
        if real:
            table_aliases[cte_name] = real
        # 若 CTE 内部引用了另一个 CTE，继续展开一层
        elif real_tables:
            inner_cte = real_tables[0]
            if inner_cte in cte_map:
                inner_real = next(
                    (t for t in cte_map[inner_cte] if t not in cte_map), None
                )
                if inner_real:
                    table_aliases[cte_name] = inner_real

    # 解析每个字段
    columns = _split_columns(cols_part)
    for idx, col_expr in enumerate(columns):
        col_expr = col_expr.strip()
        if not col_expr:
            continue
        field_info = _parse_column_expression(col_expr, table_aliases)
        if not field_info:
            continue

        # ── Fix 3: 字段名兜底 ── 若 output_field 不像合法列名，从 CREATE TABLE 按位置推断
        if create_fields and not _is_valid_column_name(field_info["output_field"]):
            if idx < len(create_fields):
                field_info["output_field"] = create_fields[idx]
            else:
                # 超出 CREATE TABLE 字段数，跳过（不乱猜）
                continue

        field_lineage.append(field_info)

    return field_lineage


def _is_valid_column_name(name: str) -> bool:
    """判断字符串是否像一个合法的列名（纯标识符）"""
    return bool(re.match(r"^[a-zA-Z_]\w*$", name.strip()))


def _extract_create_table_fields(sql_text: str, table_name: str) -> list:
    """
    从 SQL 文本里找 CREATE TABLE <table_name> 定义，按顺序返回字段名列表。
    同时搜索注释掉的 CREATE TABLE（-- create table）。
    """
    fields = []
    # 允许注释掉的 CREATE TABLE（去掉行首 -- 后解析）
    # 先把注释版的 CREATE TABLE 还原（只去掉行首 --）
    lines = sql_text.split("\n")
    cleaned_lines = []
    for line in lines:
        s = line.lstrip()
        if s.startswith("--"):
            cleaned_lines.append(s[2:])  # 去掉 -- 前缀
        else:
            cleaned_lines.append(line)
    sql_uncommented = "\n".join(cleaned_lines)

    tbl_short = (
        table_name.split(".")[-1].lower() if "." in table_name else table_name.lower()
    )
    tbl_full = table_name.lower()

    # 找 CREATE TABLE 块
    for pattern in [
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({re.escape(tbl_full)})\s*\(",
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+\.{re.escape(tbl_short)})\s*\(",
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({re.escape(tbl_short)})\s*\(",
    ]:
        m = re.search(pattern, sql_uncommented, re.IGNORECASE)
        if not m:
            continue
        # 找匹配的右括号
        start = m.end() - 1
        depth = 0
        i = start
        while i < len(sql_uncommented):
            if sql_uncommented[i] == "(":
                depth += 1
            elif sql_uncommented[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        body = sql_uncommented[start + 1 : i]
        # 逐行提取字段名（每行第一个 word，跳过 PARTITIONED/ROW/STORED/COMMENT 等关键字）
        skip_kw = {
            "partitioned",
            "row",
            "stored",
            "comment",
            "collection",
            "map",
            "fields",
            "terminated",
            "as",
            "by",
            "orcfile",
            "orc",
            "format",
            "location",
            "tblproperties",
        }
        for col_line in body.split("\n"):
            col_line = col_line.strip().lstrip(",").strip()
            if not col_line:
                continue
            wm = re.match(r"(\w+)\s+", col_line)
            if wm:
                w = wm.group(1).lower()
                if w not in skip_kw and not _is_sql_keyword(w):
                    fields.append(w)
        if fields:
            break

    return fields


def _extract_select_columns(select_sql: str) -> str:
    """提取 SELECT 关键字到 FROM 之间的列部分"""
    # 去掉 SELECT 关键字
    m = re.match(r"SELECT\s+(?:DISTINCT\s+)?", select_sql, re.IGNORECASE)
    if not m:
        return ""
    rest = select_sql[m.end() :]

    # 找到与 SELECT 同级的 FROM 位置（需处理嵌套括号）
    depth = 0
    i = 0
    while i < len(rest):
        c = rest[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0:
            # 检查是否是 FROM 关键字
            if rest[i : i + 4].upper() == "FROM" and (
                i == 0 or not rest[i - 1].isalnum()
            ):
                return rest[:i]
        i += 1
    return rest


def _extract_table_aliases(from_sql: str) -> dict:
    """从 FROM 子句中提取表名和别名映射，支持直接表别名和子查询别名"""
    aliases = {}

    # 1. 直接表别名：FROM/JOIN table [AS] alias
    direct_patterns = [
        r"(?:FROM|JOIN)\s+([\w.]+)\s+(?:AS\s+)?(\w+)",
        r"(?:FROM|JOIN)\s+([\w.]+)(?=\s*(?:WHERE|ON|LEFT|RIGHT|INNER|FULL|CROSS|JOIN|GROUP|ORDER|HAVING|LIMIT|$|\)))",
    ]
    for pattern in direct_patterns:
        for m in re.finditer(pattern, from_sql, re.IGNORECASE):
            if m.lastindex >= 2:
                table = m.group(1).lower()
                alias = m.group(2).lower()
                if not _is_sql_keyword(alias) and re.match(r"^\w+$", alias):
                    aliases[alias] = table
                aliases[table] = table
            else:
                table = m.group(1).lower()
                aliases[table] = table

    # 2. 子查询别名：) alias 或 ) AS alias
    #    从 FROM 块里提取子查询的真实表名（看子查询内部的 FROM）
    # 先找所有 (...) alias 的子查询块
    subquery_alias_pattern = re.compile(r"\)\s+(?:AS\s+)?(\w+)\b", re.IGNORECASE)
    for alias_m in subquery_alias_pattern.finditer(from_sql):
        alias = alias_m.group(1).lower()
        if _is_sql_keyword(alias):
            continue
        # 往前找匹配的 (，提取子查询内部
        end_pos = alias_m.start()  # 对应 ) 的位置
        # 找到这个 ) 对应的 (
        depth = 0
        start_pos = end_pos
        for i in range(end_pos, -1, -1):
            if from_sql[i] == ")":
                depth += 1
            elif from_sql[i] == "(":
                depth -= 1
                if depth == 0:
                    start_pos = i
                    break
        subquery = from_sql[start_pos + 1 : end_pos]
        # 从子查询里找最外层 FROM 的表名
        inner_tables = re.findall(r"\bFROM\s+([\w.]+)", subquery, re.IGNORECASE)
        if inner_tables:
            # 取最后一个（最外层的 FROM）作为代表表
            real_table = inner_tables[-1].lower()
            aliases[alias] = real_table
            aliases[real_table] = real_table

    return aliases


def _split_columns(cols_str: str) -> list:
    """按顶层逗号分割列表达式"""
    columns = []
    depth = 0
    current = []
    for c in cols_str:
        if c == "(":
            depth += 1
            current.append(c)
        elif c == ")":
            depth -= 1
            current.append(c)
        elif c == "," and depth == 0:
            columns.append("".join(current).strip())
            current = []
        else:
            current.append(c)
    if current:
        columns.append("".join(current).strip())
    return columns


def _parse_column_expression(expr: str, table_aliases: dict) -> dict:
    """解析单个列表达式，提取输出字段名和来源字段"""
    expr = expr.strip()

    # 1. 有显式 AS 别名：xxx AS alias_name
    alias_match = re.search(r"\bAS\s+(\w+)\s*$", expr, re.IGNORECASE)
    if alias_match:
        output_field = alias_match.group(1)
        expression = expr[: alias_match.start()].strip()
    else:
        # 2. 纯 table.column 形式（无函数、无空格），输出字段名 = column 部分
        #    例如：t3.pay_amount  →  output_field = 'pay_amount'
        simple_dotted = re.match(r"^(\w+)\.(\w+)$", expr.strip())
        if simple_dotted:
            output_field = simple_dotted.group(2)  # 取列名，不带表前缀
            expression = expr
        else:
            # 3. 无 AS 且含空格，最后一个 token 可能是隐式别名（Hive 允许）
            #    但要排除表达式末尾是关键字或含点号的情况
            parts = expr.rsplit(None, 1)
            if (
                len(parts) == 2
                and not _is_sql_keyword(parts[1])
                and "(" not in parts[1]
                and "." not in parts[1]
                and re.match(r"^\w+$", parts[1])
            ):
                output_field = parts[1]
                expression = parts[0].strip()
            else:
                # 4. fallback：整个表达式作为字段名（无法识别别名）
                output_field = expr
                expression = expr

    # 提取表达式中引用的字段（table.field 或 alias.field 格式）
    source_fields = []
    source_tables = []

    # 匹配 table.column 格式
    for m in re.finditer(r"([\w]+)\.([\w]+)", expression):
        tbl_or_alias = m.group(1).lower()
        field = m.group(2).lower()
        actual_table = table_aliases.get(tbl_or_alias, tbl_or_alias)
        source_fields.append(
            {"table": actual_table, "field": field, "alias": tbl_or_alias}
        )
        if actual_table not in source_tables:
            source_tables.append(actual_table)

    # 如果没有找到 table.field，尝试提取裸字段名
    if not source_fields:
        # 先去掉 AS 别名
        expr_body = re.sub(r"\s+as\s+\w+\s*$", "", expression.strip(), flags=re.IGNORECASE).strip()
        # 简单列名（无函数）
        if re.match(r"^[\w]+$", expr_body):
            source_fields.append(
                {"table": "", "field": expr_body.lower(), "alias": ""}
            )
        else:
            # 含函数/表达式：提取所有裸列名（排除SQL关键字和数字）
            _SQL_KW = {
                "nvl", "coalesce", "sum", "count", "avg", "max", "min",
                "case", "when", "then", "else", "end", "if", "ifnull",
                "round", "cast", "floor", "ceil", "concat", "substr",
                "date_sub", "date_add", "datediff", "unix_timestamp",
                "from_unixtime", "get_json_object", "split", "lpad",
                "isnull", "nullif", "greatest", "least", "null",
                "true", "false", "and", "or", "not", "in", "is",
                "like", "between", "distinct", "over", "partition",
                "by", "order", "rows", "range", "preceding", "following",
                "select", "from", "where", "join", "on", "as", "with",
                "int", "bigint", "double", "float", "string", "boolean",
            }
            raw_names = re.findall(r"\b([a-z_][a-z0-9_]*)\b", expr_body.lower())
            seen_sf: set = set()
            for name in raw_names:
                if name not in _SQL_KW and not name.isdigit() and name not in seen_sf:
                    seen_sf.add(name)
                    source_fields.append({"table": "", "field": name, "alias": ""})

    return {
        "output_field": output_field.lower(),
        "expression": expr,
        "source_fields": source_fields,
        "source_tables": source_tables,
    }


def find_field_in_sql(sql_text: str, field_name: str):
    """
    在 SQL 文本中找到字段对应的代码行和位置
    返回: [{"line": 行号, "start": 开始位置, "end": 结束位置, "text": 行文本}]
    """
    highlights = []
    lines = sql_text.split("\n")

    field_lower = field_name.lower()
    patterns = [
        # AS alias_name
        rf"\bAS\s+{re.escape(field_lower)}\b",
        # alias_name or field_name directly
        rf"\b{re.escape(field_lower)}\b",
    ]

    for line_idx, line in enumerate(lines):
        line_lower = line.lower()
        for pattern in patterns:
            for m in re.finditer(pattern, line_lower, re.IGNORECASE):
                highlights.append(
                    {
                        "line": line_idx + 1,
                        "start": m.start(),
                        "end": m.end(),
                        "text": line,
                        "match": line[m.start() : m.end()],
                    }
                )
            break  # 用第一个匹配的 pattern

    return highlights
