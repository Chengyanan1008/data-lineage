#!/bin/bash
# 数据血缘系统停止脚本
# 彻底释放后端(5001)和前端(8080)端口，解决旧进程残留导致新进程无法启动的问题

BACKEND_PORT=5001
FRONTEND_PORT=8080

echo "========================================"
echo "     数据血缘系统 停止中..."
echo "========================================"

# ── 通用函数：按端口杀进程 ──────────────────────────────────────────────────
kill_port() {
    local port=$1
    local pids
    pids=$(lsof -ti TCP:"$port" 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "  [端口 $port] 发现进程: $pids，发送 SIGTERM..."
        echo "$pids" | xargs kill -15 2>/dev/null
        sleep 1
        # 若仍存活，强制 SIGKILL
        local remaining
        remaining=$(lsof -ti TCP:"$port" 2>/dev/null)
        if [ -n "$remaining" ]; then
            echo "  [端口 $port] 进程未退出，强制 SIGKILL: $remaining"
            echo "$remaining" | xargs kill -9 2>/dev/null
            sleep 1
        fi
        # 最终确认
        remaining=$(lsof -ti TCP:"$port" 2>/dev/null)
        if [ -z "$remaining" ]; then
            echo "  [端口 $port] 已释放"
        else
            echo "  [端口 $port] 警告：进程 $remaining 仍然存在"
        fi
    else
        echo "  [端口 $port] 无占用进程"
    fi
}

# ── 通用函数：按进程名杀进程 ────────────────────────────────────────────────
kill_by_name() {
    local pattern=$1
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "  [进程名] 匹配 \"$pattern\": $pids，发送 SIGTERM..."
        echo "$pids" | xargs kill -15 2>/dev/null
        sleep 1
        local remaining
        remaining=$(pgrep -f "$pattern" 2>/dev/null)
        if [ -n "$remaining" ]; then
            echo "  [进程名] 强制 SIGKILL: $remaining"
            echo "$remaining" | xargs kill -9 2>/dev/null
        fi
    fi
}

echo ""
echo "[1/3] 停止后端服务 (端口 $BACKEND_PORT)..."
kill_by_name "python3 app.py"
kill_port "$BACKEND_PORT"

echo ""
echo "[2/3] 停止前端服务 (端口 $FRONTEND_PORT)..."
kill_by_name "http.server $FRONTEND_PORT"
kill_port "$FRONTEND_PORT"

echo ""
echo "[3/3] 最终确认..."
all_clear=true
for port in $BACKEND_PORT $FRONTEND_PORT; do
    if lsof -ti TCP:"$port" &>/dev/null; then
        echo "  [警告] 端口 $port 仍被占用"
        all_clear=false
    else
        echo "  [OK] 端口 $port 空闲"
    fi
done

echo ""
if $all_clear; then
    echo "========================================"
    echo "  所有服务已停止，端口已释放"
    echo "  重新启动请运行: ./start.sh"
    echo "========================================"
else
    echo "========================================"
    echo "  部分端口仍被占用，请手动检查:"
    echo "    lsof -iTCP:$BACKEND_PORT -iTCP:$FRONTEND_PORT"
    echo "========================================"
    exit 1
fi
