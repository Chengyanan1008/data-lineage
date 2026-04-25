#!/bin/bash
# 数据血缘系统启动脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"
PORT=5001
FRONTEND_PORT=8080

echo "========================================"
echo "     数据血缘系统 启动中..."
echo "========================================"

# 检查 Python3
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] 未找到 python3，请先安装 Python 3.8+"
    exit 1
fi

# 安装依赖（如果需要）
echo "[1/3] 检查 Python 依赖..."
python3 -c "import flask, flask_cors, pandas, sqlparse, networkx" 2>/dev/null || {
    echo "  安装依赖..."
    pip3 install flask flask-cors pandas sqlparse networkx -q
}

# 停止旧进程
pkill -f "python3 app.py" 2>/dev/null
sleep 1

# 启动后端
echo "[2/3] 启动后端服务 (端口 $PORT)..."
cd "$BACKEND_DIR"
nohup python3 app.py > ../logs/lineage-backend.log 2>&1 &
BACKEND_PID=$!
echo "  后端 PID: $BACKEND_PID"

# 等待后端就绪
echo "  等待后端加载数据（首次启动需要解析 CSV，约 30-60 秒）..."
for i in $(seq 1 60); do
    sleep 2
    if curl -s http://localhost:$PORT/api/health > /dev/null 2>&1; then
        echo "  后端已就绪！"
        break
    fi
    printf "  ."
done
echo ""

# 启动前端（用 Python 的 http.server）
echo "[3/3] 启动前端服务 (端口 $FRONTEND_PORT)..."
pkill -f "http.server $FRONTEND_PORT" 2>/dev/null
cd "$FRONTEND_DIR"
nohup python3 -m http.server $FRONTEND_PORT > /tmp/lineage-frontend.log 2>&1 &
FRONTEND_PID=$!
echo "  前端 PID: $FRONTEND_PID"
sleep 1

echo ""
echo "========================================"
echo "  系统启动完成！"
echo ""
echo "  访问地址: http://localhost:$FRONTEND_PORT"
echo "  后端API:  http://localhost:$PORT/api"
echo ""
echo "  停止服务: kill $BACKEND_PID $FRONTEND_PID"
echo "========================================"

# 自动打开浏览器（macOS）
if command -v open &> /dev/null; then
    sleep 1
    open "http://localhost:$FRONTEND_PORT"
fi
