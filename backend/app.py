"""
数据血缘系统后端 Flask API
"""

import os
import json
import re
from flask import Flask, jsonify, request
from flask_cors import CORS
from data_loader import (
    load_csv,
    build_lineage_graph,
    get_upstream_lineage,
    get_downstream_lineage,
    trace_metric_caliber,
)
from field_tracer import trace_field_lineage_full
from sql_parser import extract_field_lineage, find_field_in_sql, detect_layer

app = Flask(__name__)
CORS(app)

# 数据文件路径
# DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "online_sql.csv")
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "market_sql.csv")

# 全局缓存
_graph = None
_jobs_list = None


def get_graph():
    global _graph, _jobs_list
    if _graph is None:
        print("Loading and parsing CSV data...")
        _jobs_list = load_csv(DATA_FILE)
        _graph = build_lineage_graph(_jobs_list)
        print(
            f"Loaded {len(_jobs_list)} jobs, {len(_graph['nodes'])} tables, {len(_graph['edges'])} edges"
        )
    return _graph


def get_jobs_list():
    get_graph()
    return _jobs_list


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/stats", methods=["GET"])
def stats():
    """获取整体统计信息"""
    graph = get_graph()
    jobs = get_jobs_list()

    layer_stats = {}
    for node in graph["nodes"]:
        layer = node["layer"]
        layer_stats[layer] = layer_stats.get(layer, 0) + 1

    script_type_stats = {}
    for job in jobs:
        t = job["script_type"]
        script_type_stats[t] = script_type_stats.get(t, 0) + 1

    return jsonify(
        {
            "total_tables": len(graph["nodes"]),
            "total_edges": len(graph["edges"]),
            "total_jobs": len(jobs),
            "layer_distribution": layer_stats,
            "script_type_distribution": script_type_stats,
        }
    )


@app.route("/api/tables", methods=["GET"])
def list_tables():
    """获取所有表列表"""
    graph = get_graph()
    search = request.args.get("search", "").lower()
    layer = request.args.get("layer", "")
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 50))

    tables = graph["nodes"]

    if search:
        tables = [t for t in tables if search in t["id"].lower()]
        # 有搜索词时不限制返回数量（搜索结果通常很少）
        page_size = max(page_size, len(tables))
    if layer:
        tables = [t for t in tables if t["layer"] == layer]

    total = len(tables)
    start = (page - 1) * page_size
    end = start + page_size

    return jsonify(
        {
            "total": total,
            "page": page,
            "page_size": page_size,
            "tables": tables[start:end],
        }
    )


@app.route("/api/table/<path:table_name>/lineage", methods=["GET"])
def table_lineage(table_name):
    """获取表的血缘关系（上游+下游）"""
    graph = get_graph()
    direction = request.args.get("direction", "both")
    depth = int(request.args.get("depth", 5))

    result = {
        "table": table_name,
        "layer": detect_layer(table_name),
    }

    if direction in ("upstream", "both"):
        result["upstream"] = get_upstream_lineage(table_name, graph, max_depth=depth)

    if direction in ("downstream", "both"):
        result["downstream"] = get_downstream_lineage(
            table_name, graph, max_depth=depth
        )

    # 获取产生该表的 job 信息
    tbl_info = graph["tables"].get(table_name, {})
    jobs_info = []
    for job_id in tbl_info.get("produced_by", []):
        job = graph["jobs"].get(job_id, {})
        if job:
            jobs_info.append(
                {
                    "job_id": job_id,
                    "job_name": job.get("job_name", ""),
                    "flow_name": job.get("flow_name", ""),
                    "owner": job.get("owner", ""),
                    "script_type": job.get("script_type", ""),
                    "input_tables": job.get("input_tables", []),
                    "output_tables": job.get("output_tables", []),
                }
            )
    result["produced_by_jobs"] = jobs_info

    return jsonify(result)


@app.route("/api/table/<path:table_name>/fields", methods=["GET"])
def table_fields(table_name):
    """获取表的字段列表（从 job 的字段血缘中提取）"""
    graph = get_graph()
    tbl_info = graph["tables"].get(table_name, {})

    fields = set()
    field_details = []

    for job_id in tbl_info.get("produced_by", []):
        job = graph["jobs"].get(job_id, {})
        for fl in job.get("field_lineage", []):
            fname = fl.get("output_field", "")
            if fname and fname not in fields:
                fields.add(fname)
                field_details.append(
                    {
                        "field": fname,
                        "expression": fl.get("expression", ""),
                        "source_fields": fl.get("source_fields", []),
                        "source_tables": fl.get("source_tables", []),
                        "job_id": job_id,
                    }
                )

    return jsonify(
        {
            "table": table_name,
            "fields": field_details,
        }
    )


@app.route("/api/table/<path:table_name>/field/<field_name>/lineage", methods=["GET"])
def field_lineage(table_name, field_name):
    """获取字段级血缘关系"""
    graph = get_graph()
    jobs_map = graph["jobs"]
    tbl_info = graph["tables"].get(table_name, {})

    field_info = None
    job_content = ""
    job_detail = None

    for job_id in tbl_info.get("produced_by", []):
        job = jobs_map.get(job_id, {})
        for fl in job.get("field_lineage", []):
            if fl.get("output_field", "").lower() == field_name.lower():
                field_info = fl
                job_content = job.get("content", "")
                job_detail = {
                    "job_id": job_id,
                    "job_name": job.get("job_name", ""),
                    "owner": job.get("owner", ""),
                    "script_type": job.get("script_type", ""),
                    "flow_name": job.get("flow_name", ""),
                }
                break

    if not field_info:
        return jsonify(
            {"error": f"Field {field_name} not found in table {table_name}"}
        ), 404

    # 找到字段在代码中的高亮位置
    highlights = find_field_in_sql(job_content, field_name)
    # 也高亮来源字段
    source_highlights = {}
    for sf in field_info.get("source_fields", []):
        sf_name = sf.get("field", "")
        if sf_name:
            source_highlights[sf_name] = find_field_in_sql(job_content, sf_name)

    return jsonify(
        {
            "table": table_name,
            "field": field_name,
            "expression": field_info.get("expression", ""),
            "source_fields": field_info.get("source_fields", []),
            "source_tables": field_info.get("source_tables", []),
            "job": job_detail,
            "code": job_content,
            "highlights": highlights,
            "source_highlights": source_highlights,
        }
    )


@app.route("/api/job/<job_id>", methods=["GET"])
def job_detail(job_id):
    """获取 Job 详情（含完整 SQL/脚本）"""
    graph = get_graph()
    job = graph["jobs"].get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(
        {
            "job_id": job_id,
            "job_name": job.get("job_name", ""),
            "flow_id": job.get("flow_id", ""),
            "flow_name": job.get("flow_name", ""),
            "owner": job.get("owner", ""),
            "script_type": job.get("script_type", ""),
            "content": job.get("content", ""),
            "input_tables": job.get("input_tables", []),
            "output_tables": job.get("output_tables", []),
            "field_lineage": job.get("field_lineage", []),
        }
    )


@app.route("/api/table/<path:table_name>/caliber", methods=["GET"])
def metric_caliber(table_name):
    """追溯统计口径"""
    graph = get_graph()
    field_name = request.args.get("field", "")

    if not field_name:
        # 没有指定字段，返回表级口径（列出所有字段的口径）
        tbl_info = graph["tables"].get(table_name, {})
        jobs_map = graph["jobs"]

        all_fields = []
        for job_id in tbl_info.get("produced_by", []):
            job = jobs_map.get(job_id, {})
            for fl in job.get("field_lineage", []):
                all_fields.append(
                    {
                        "field": fl.get("output_field", ""),
                        "expression": fl.get("expression", ""),
                        "source_fields": fl.get("source_fields", []),
                    }
                )

        # 生成上游链路摘要
        upstream = get_upstream_lineage(table_name, graph)

        return jsonify(
            {
                "table": table_name,
                "layer": detect_layer(table_name),
                "fields": all_fields,
                "upstream_summary": upstream,
                "caliber_text": _generate_table_caliber(table_name, graph),
            }
        )

    # 字段级口径追溯
    result = trace_metric_caliber(table_name, field_name, graph)
    return jsonify(result)


def _generate_table_caliber(table_name: str, graph: dict) -> str:
    """生成表级口径描述"""
    upstream = get_upstream_lineage(table_name, graph)
    layers = upstream.get("layers", {})

    tbl_info = graph["tables"].get(table_name, {})
    jobs_map = graph["jobs"]

    lines = [f"表 [{table_name}] 统计口径分析报告", "=" * 60]

    # 表基本信息
    layer = detect_layer(table_name)
    lines.append(f"\n数据层级: {layer.upper()}")

    # 生产 job 信息
    for job_id in tbl_info.get("produced_by", []):
        job = jobs_map.get(job_id, {})
        lines.append(
            f"生产任务: {job.get('job_name', '')} (负责人: {job.get('owner', '')})"
        )
        lines.append(f"脚本类型: {job.get('script_type', '')}")

    # 上游链路
    lines.append(f"\n上游数据链路:")
    for layer_name in ["ods", "dim", "dwd", "dwa", "dws", "ads", "da", "unknown"]:
        tbls = layers.get(layer_name, [])
        if tbls and layer_name != detect_layer(table_name):
            lines.append(f"  [{layer_name.upper()}层] {', '.join(tbls)}")

    # 字段信息
    field_count = 0
    for job_id in tbl_info.get("produced_by", []):
        job = jobs_map.get(job_id, {})
        field_count += len(job.get("field_lineage", []))

    if field_count > 0:
        lines.append(f"\n字段血缘: 共追踪到 {field_count} 个字段的来源关系")

    return "\n".join(lines)


@app.route("/api/graph", methods=["GET"])
def full_graph():
    """获取完整血缘图数据（用于全局视图）"""
    graph = get_graph()

    # 限制返回规模，避免前端压力过大
    max_nodes = int(request.args.get("max_nodes", 200))
    layer_filter = request.args.get("layer", "")
    search = request.args.get("search", "").lower()

    nodes = graph["nodes"]
    edges = graph["edges"]

    if layer_filter:
        valid_tables = {n["id"] for n in nodes if n["layer"] == layer_filter}
        nodes = [n for n in nodes if n["layer"] == layer_filter]
        edges = [
            e
            for e in edges
            if e["source"] in valid_tables or e["target"] in valid_tables
        ]

    if search:
        valid_tables = {n["id"] for n in nodes if search in n["id"].lower()}
        nodes = [n for n in nodes if search in n["id"].lower()]
        edges = [
            e
            for e in edges
            if e["source"] in valid_tables and e["target"] in valid_tables
        ]

    # 截取
    nodes = nodes[:max_nodes]
    node_ids = {n["id"] for n in nodes}
    edges = [e for e in edges if e["source"] in node_ids and e["target"] in node_ids]

    return jsonify(
        {
            "nodes": nodes,
            "edges": edges,
            "total_nodes": len(graph["nodes"]),
            "total_edges": len(graph["edges"]),
        }
    )


@app.route("/api/table/<path:table_name>/subgraph", methods=["GET"])
def table_subgraph(table_name):
    """获取以某张表为中心的子图（供血缘图渲染）"""
    graph = get_graph()
    depth = int(request.args.get("depth", 3))
    direction = request.args.get("direction", "both")  # upstream / downstream / both
    # 下游扇出通常比上游大很多，限制节点数防止卡死
    up_max_nodes = int(request.args.get("max_nodes", 300))
    down_max_nodes = int(request.args.get("max_nodes", 100))

    nodes_map = {}
    edges_list = []
    edge_set = set()

    def add_node(tbl, depth_val, dir_label):
        if tbl not in nodes_map:
            nodes_map[tbl] = {
                "id": tbl,
                "label": tbl,
                "layer": detect_layer(tbl),
                "is_center": tbl == table_name,
                "direction": dir_label,
                "depth": depth_val,
            }

    def add_edges(edge_list, dir_label):
        for edge in edge_list:
            ek = f"{edge['from']}->{edge['to']}@{dir_label}"
            if ek not in edge_set:
                edge_set.add(ek)
                edges_list.append(
                    {
                        "id": ek,
                        "source": edge["from"],
                        "target": edge["to"],
                        "job_id": edge.get("job_id", ""),
                        "job_name": edge.get("job_name", ""),
                        "script_type": edge.get("script_type", ""),
                        "direction": dir_label,
                    }
                )

    add_node(table_name, 0, "center")

    truncated = False
    if direction in ("upstream", "both"):
        upstream = get_upstream_lineage(
            table_name, graph, max_depth=depth, max_nodes=up_max_nodes
        )
        for node in upstream.get("nodes", []):
            add_node(node["table"], node["depth"], "upstream")
        add_edges(upstream.get("edges", []), "upstream")
        if upstream.get("truncated"):
            truncated = True

    if direction in ("downstream", "both"):
        downstream = get_downstream_lineage(
            table_name, graph, max_depth=depth, max_nodes=down_max_nodes
        )
        for node in downstream.get("nodes", []):
            add_node(node["table"], node["depth"], "downstream")
        add_edges(downstream.get("edges", []), "downstream")
        if downstream.get("truncated"):
            truncated = True

    return jsonify(
        {
            "center": table_name,
            "direction": direction,
            "truncated": truncated,
            "nodes": list(nodes_map.values()),
            "edges": edges_list,
        }
    )


@app.route("/api/table/<path:table_name>/field/<field_name>/caliber", methods=["GET"])
def field_caliber_trace(table_name, field_name):
    """
    字段级完整口径追溯
    返回从目标字段一路到 ODS/源头的完整血缘链路报告
    """
    graph = get_graph()
    result = trace_field_lineage_full(table_name, field_name, graph)
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
