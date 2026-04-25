"""
CSV 数据加载器
解析 online_sql.csv 并构建血缘图
"""

import csv
import json
import re
import os
from typing import List, Dict, Any
from sql_parser import (
    detect_script_type,
    detect_layer,
    extract_tables_from_sql,
    extract_tables_from_bash,
    extract_tables_from_python,
    extract_field_lineage,
    find_field_in_sql,
    LAYER_ORDER,
)


def parse_job_content(raw_content: str) -> str:
    """解析 job_content 字段，可能是 JSON 包裹的内容"""
    if not raw_content:
        return ""
    if isinstance(raw_content, dict):
        return raw_content.get("content", str(raw_content))
    raw = str(raw_content).strip()
    # 尝试解析 JSON
    try:
        if raw.startswith("{"):
            obj = json.loads(raw)
            if isinstance(obj, dict):
                if "content" in obj:
                    content = obj["content"]
                    if isinstance(content, str):
                        return content
                    return str(content)
                # 递归找 content
                for v in obj.values():
                    if isinstance(v, str) and len(v) > 50:
                        return v
    except Exception:
        pass
    return raw


def load_csv(filepath: str) -> List[Dict]:
    """加载并解析 CSV 文件"""
    csv.field_size_limit(10 * 1024 * 1024)  # 10MB
    jobs = []
    seen_job_ids = set()  # 按 job_id 去重
    print(f"开始读${filepath} 文件")
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            job_id = row.get("job_id", "").strip()
            if job_id and job_id in seen_job_ids:
                continue  # 跳过重复的 job_id
            if job_id:
                seen_job_ids.add(job_id)
            content = parse_job_content(row.get("job_conetent", ""))
            script_type = detect_script_type(content)

            # 根据脚本类型提取表
            if script_type in ("bash", "shell"):
                input_tables, output_tables = extract_tables_from_bash(content)
                # bash 脚本的输出表从 output_table_list 补充
            elif script_type in ("python", "spark"):
                input_tables, output_tables = extract_tables_from_python(content)
            else:
                input_tables, output_tables = extract_tables_from_sql(content)

            # 用 output_table_list 字段补充输出表
            declared_outputs = []
            output_list_raw = row.get("output_table_list", "")
            if output_list_raw:
                for t in output_list_raw.split(","):
                    t = t.strip().lower()
                    if t:
                        declared_outputs.append(t)

            # 合并，声明的输出表优先
            all_output_tables = list(set(output_tables + declared_outputs))

            # 去重并过滤掉临时表别名等无效表名
            input_tables = list(
                dict.fromkeys(t for t in input_tables if is_valid_table(t))
            )
            all_output_tables = list(
                dict.fromkeys(t for t in all_output_tables if is_valid_table(t))
            )

            # 提取字段血缘（仅 SQL 类型）
            field_lineage = []
            if script_type in ("hive_sql", "sql", "spark"):
                for out_tbl in all_output_tables:
                    fls = extract_field_lineage(content, out_tbl)
                    if fls:
                        field_lineage.extend(fls)

            job = {
                "flow_id": row.get("flow_id", ""),
                "flow_name": row.get("flow_name", ""),
                "owner": row.get("flow_owner_user_name", ""),
                "job_id": row.get("job_id", ""),
                "job_name": row.get("job_name", ""),
                "script_type": script_type,
                "content": content,
                "input_tables": input_tables,
                "output_tables": all_output_tables,
                "field_lineage": field_lineage,
            }
            jobs.append(job)

    return jobs


def is_valid_table(name: str) -> bool:
    """过滤无效表名"""
    if not name or len(name) < 2:
        return False
    # 过滤纯数字
    if name.isdigit():
        return False
    # 过滤变量占位符
    if re.search(r"[\$\{\}\[\]<>]", name):
        return False
    # 过滤常见非表名关键字
    invalid = {
        "select",
        "from",
        "where",
        "join",
        "on",
        "null",
        "true",
        "false",
        "and",
        "or",
        "not",
        "in",
        "is",
        "as",
        "by",
        "set",
        "if",
        "then",
        "else",
        "end",
        "case",
        "when",
        "into",
        "partition",
        "overwrite",
        "table",
        "view",
        "insert",
        "create",
        "drop",
        "alter",
        "with",
    }
    nl = name.lower()
    if nl in invalid:
        return False
    # 过滤 Python/Java 包路径（如 requests.adapters、urllib3.exceptions）
    # 特征：schema 部分是已知 Python/Java 库名，或全小写无下划线的单词
    if "." in name:
        schema = name.split(".")[0].lower()
        # 已知 Python 库 schema
        python_libs = {
            "requests",
            "urllib3",
            "urllib",
            "http",
            "os",
            "sys",
            "re",
            "json",
            "csv",
            "io",
            "abc",
            "ast",
            "collections",
            "datetime",
            "logging",
            "pathlib",
            "socket",
            "ssl",
            "threading",
            "time",
            "typing",
            "uuid",
            "warnings",
            "xml",
            "yaml",
            "boto3",
            "botocore",
            "pandas",
            "numpy",
            "sqlalchemy",
            "flask",
            "django",
            "celery",
            "schedule",
            "report",
            "traceback",
            "enum",
            "functools",
        }
        if schema in python_libs:
            return False
    # 过滤明显不是 schema.table 格式的单词（如 schedule、report 单独出现）
    if "." not in name:
        non_table_words = {
            "schedule",
            "report",
            "traceback",
            "exception",
            "error",
            "warning",
            "debug",
            "info",
            "logger",
            "handler",
            "adapter",
            "session",
            "response",
            "request",
            "client",
            "server",
        }
        if nl in non_table_words:
            return False
    return True


def build_lineage_graph(jobs: List[Dict]) -> Dict:
    """
    构建血缘图数据结构
    返回节点列表和边列表（适合前端 D3/ReactFlow 渲染）
    """
    # 收集所有表
    all_tables = {}  # table_name -> {jobs producing it, jobs consuming it}

    for job in jobs:
        for tbl in set(job["output_tables"]):  # set 去重，同一 job 内避免重复
            if tbl not in all_tables:
                all_tables[tbl] = {
                    "name": tbl,
                    "layer": detect_layer(tbl),
                    "produced_by": [],
                    "consumed_by": [],
                }
            if job["job_id"] not in all_tables[tbl]["produced_by"]:
                all_tables[tbl]["produced_by"].append(job["job_id"])

        for tbl in set(job["input_tables"]):  # set 去重
            if tbl not in all_tables:
                all_tables[tbl] = {
                    "name": tbl,
                    "layer": detect_layer(tbl),
                    "produced_by": [],
                    "consumed_by": [],
                }
            if job["job_id"] not in all_tables[tbl]["consumed_by"]:
                all_tables[tbl]["consumed_by"].append(job["job_id"])

    # 构建节点
    nodes = []
    for tbl_name, tbl_info in all_tables.items():
        nodes.append(
            {
                "id": tbl_name,
                "type": "table",
                "label": tbl_name,
                "layer": tbl_info["layer"],
                "produced_by": tbl_info["produced_by"],
                "consumed_by": tbl_info["consumed_by"],
            }
        )

    # 构建边（表级血缘）
    edges = []
    edge_set = set()
    for job in jobs:
        for in_tbl in job["input_tables"]:
            for out_tbl in job["output_tables"]:
                edge_key = f"{in_tbl}->{out_tbl}@{job['job_id']}"
                if edge_key not in edge_set:
                    edge_set.add(edge_key)
                    edges.append(
                        {
                            "id": edge_key,
                            "source": in_tbl,
                            "target": out_tbl,
                            "job_id": job["job_id"],
                            "job_name": job["job_name"],
                            "flow_id": job["flow_id"],
                        }
                    )

    return {
        "nodes": nodes,
        "edges": edges,
        "jobs": {j["job_id"]: j for j in jobs},
        "tables": all_tables,
    }


def get_upstream_lineage(
    table_name: str, graph: Dict, max_depth: int = 10, max_nodes: int = 300
) -> Dict:
    """
    获取某张表的完整上游血缘链路（直到 ODS 层或无更多上游）
    """
    jobs_map = graph["jobs"]
    tables_map = graph["tables"]

    visited = set()
    result = {
        "table": table_name,
        "layers": {},  # layer -> [tables]
        "nodes": [],
        "edges": [],
        "path": [],
        "truncated": False,
    }

    def dfs(tbl, depth):
        if tbl in visited or depth > max_depth:
            return
        if len(visited) >= max_nodes:
            result["truncated"] = True
            return
        visited.add(tbl)

        tbl_info = tables_map.get(
            tbl, {"layer": detect_layer(tbl), "produced_by": [], "consumed_by": []}
        )
        layer = tbl_info.get("layer", detect_layer(tbl))

        if layer not in result["layers"]:
            result["layers"][layer] = []
        if tbl not in result["layers"][layer]:
            result["layers"][layer].append(tbl)

        result["nodes"].append(
            {
                "table": tbl,
                "layer": layer,
                "depth": depth,
            }
        )

        # 找产生该表的 job
        for job_id in tbl_info.get("produced_by", []):
            job = jobs_map.get(job_id, {})
            for in_tbl in job.get("input_tables", []):
                result["edges"].append(
                    {
                        "from": in_tbl,
                        "to": tbl,
                        "job_id": job_id,
                        "job_name": job.get("job_name", ""),
                        "script_type": job.get("script_type", ""),
                    }
                )
                dfs(in_tbl, depth + 1)

    dfs(table_name, 0)
    return result


def get_downstream_lineage(
    table_name: str, graph: Dict, max_depth: int = 10, max_nodes: int = 200
) -> Dict:
    """
    获取某张表的完整下游血缘链路
    max_nodes: 节点总数上限，防止下游扇出过大导致卡死
    """
    jobs_map = graph["jobs"]
    tables_map = graph["tables"]

    visited = set()
    result = {
        "table": table_name,
        "nodes": [],
        "edges": [],
        "truncated": False,
    }

    def dfs(tbl, depth):
        if tbl in visited or depth > max_depth:
            return
        if len(visited) >= max_nodes:
            result["truncated"] = True
            return
        visited.add(tbl)

        tbl_info = tables_map.get(
            tbl, {"layer": detect_layer(tbl), "produced_by": [], "consumed_by": []}
        )
        layer = tbl_info.get("layer", detect_layer(tbl))

        result["nodes"].append(
            {
                "table": tbl,
                "layer": layer,
                "depth": depth,
            }
        )

        for job_id in tbl_info.get("consumed_by", []):
            job = jobs_map.get(job_id, {})
            for out_tbl in job.get("output_tables", []):
                result["edges"].append(
                    {
                        "from": tbl,
                        "to": out_tbl,
                        "job_id": job_id,
                        "job_name": job.get("job_name", ""),
                        "script_type": job.get("script_type", ""),
                    }
                )
                dfs(out_tbl, depth + 1)

    dfs(table_name, 0)
    return result


def trace_metric_caliber(table_name: str, field_name: str, graph: Dict) -> Dict:
    """
    追溯统计口径：从目标表的某个字段一直追溯到 ODS 层
    返回层级清晰的追溯结果
    """
    jobs_map = graph["jobs"]
    tables_map = graph["tables"]

    result = {
        "target_table": table_name,
        "target_field": field_name,
        "trace_path": [],  # 按层级排列的追溯路径
        "summary": "",
    }

    def find_field_source(tbl, field, depth, path_so_far):
        """递归追溯字段来源"""
        if depth > 15:
            return

        layer = detect_layer(tbl)
        node = {
            "table": tbl,
            "field": field,
            "layer": layer,
            "depth": depth,
            "transform": "",
            "source_fields": [],
            "job_info": None,
        }

        tbl_info = tables_map.get(tbl, {})
        for job_id in tbl_info.get("produced_by", []):
            job = jobs_map.get(job_id, {})
            # 在字段血缘中找
            for fl in job.get("field_lineage", []):
                if fl.get("output_field", "").lower() == field.lower():
                    node["transform"] = fl.get("expression", "")
                    node["job_info"] = {
                        "job_id": job_id,
                        "job_name": job.get("job_name", ""),
                        "script_type": job.get("script_type", ""),
                        "owner": job.get("owner", ""),
                    }
                    node["source_fields"] = fl.get("source_fields", [])

                    result["trace_path"].append(node)

                    # 继续向上追溯
                    for sf in fl.get("source_fields", []):
                        if sf.get("table") and sf.get("field"):
                            find_field_source(
                                sf["table"],
                                sf["field"],
                                depth + 1,
                                path_so_far + [node],
                            )
                    return

        # 没找到字段血缘，但记录表节点
        result["trace_path"].append(node)

    find_field_source(table_name, field_name, 0, [])

    # 生成口径摘要
    result["summary"] = generate_caliber_summary(
        result["trace_path"], table_name, field_name
    )
    return result


def generate_caliber_summary(trace_path: list, table_name: str, field_name: str) -> str:
    """生成统计口径的文字描述"""
    if not trace_path:
        return f"字段 {field_name} 在表 {table_name} 中的口径信息暂未找到"

    lines = [f"字段 [{field_name}] 统计口径追溯（{table_name}）\n"]
    lines.append("=" * 60)

    layer_map = {}
    for node in trace_path:
        layer = node.get("layer", "unknown")
        if layer not in layer_map:
            layer_map[layer] = []
        layer_map[layer].append(node)

    for layer in LAYER_ORDER + ["unknown"]:
        if layer in layer_map:
            lines.append(f"\n[{layer.upper()} 层]")
            for node in layer_map[layer]:
                tbl = node.get("table", "")
                fld = node.get("field", "")
                transform = node.get("transform", "")
                lines.append(f"  表: {tbl}.{fld}")
                if transform:
                    lines.append(f"  计算逻辑: {transform[:200]}")
                job_info = node.get("job_info")
                if job_info:
                    lines.append(
                        f"  Job: {job_info.get('job_name', '')} (负责人: {job_info.get('owner', '')})"
                    )

    return "\n".join(lines)
