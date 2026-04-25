# Data-Lineage: 简化血缘 API 使用指南

<img width="1512" height="828" alt="image" src="https://github.com/user-attachments/assets/5cf71d0c-5a2c-42bf-8159-01e38e1b7447" />
本项目提供一个最小化的数据血缘验证入口，聚焦于简化的血缘关系查询，不依赖 DolphinScheduler/CI/CD。核心能力是通过后端提供的简化血缘接口 /api/simple_lineage，快速查看指定表的上下游血缘关系，便于教育、原型验证和内部演示。
<img width="1367" height="670" alt="image" src="https://github.com/user-attachments/assets/49875d61-5a7e-4674-b1bd-7a327d7c32c5" />


核心能力
- 简化血缘查询：给定一个表名，返回该表的上游和下游血缘边与节点的简化视图。
- 无需配置 CI/CD、DolphinScheduler，直接在本地后端环境中运行测试。
- 支持 depth 参数，控制向上/向下遍历的层级深度。

查看处理逻辑：
<img width="993" height="411" alt="image" src="https://github.com/user-attachments/assets/6250ae96-af10-4200-903b-531b10497b6a" />


关键过滤条件：
<img width="620" height="321" alt="image" src="https://github.com/user-attachments/assets/d407c0be-69b9-4920-b984-d476b7ac5924" />


部署与测试
- 依赖：Python3、Flask 等基础依赖。后端已包含一个 start.sh 脚本用于快速启动。
- 启动后端：在项目根目录执行
  ./data-lineage/start.sh
- 验证简单血缘接口（示例）：
  curl -s "http://localhost:5001/api/simple_lineage?table=ods_ec.ods_ec_user_di&depth=2" | head -n 20
- 停止服务：
  ./data-lineage/stop.sh

接口说明
- /api/simple_lineage?table={table}&depth={depth}
  - table: 需要分析血缘的目标表，如 ods_ec.ods_ec_user_di
  - depth: 上下游追溯深度，默认 5，可根据需要调整
  - 返回值结构（简化）:
    {
      "table": "ods_ec.ods_ec_user_di",
      "upstream": {
        "edges": [ {"source": "ods_ec.ods_ec_user_di", "target": "dim_ec.dim_ec_user_da", "job_id": "...", "job_name": "...", "script_type": "hive_sql"}, ... ],
        "nodes": [ ... ],
        "layers": { ... },
        "truncated": false
      },
      "downstream": {
        "edges": [ {"source": "dim_ec.dim_ec_user_da", "target": "dw_ec.dwd_ec_user_di", ...}, ... ],
        "nodes": [ ... ],
        "truncated": false
      }
    }

测试数据与结果
- 测试数据基于 market_sql.csv（生成脚本已包含在项目中），你可以本地执行数据生成脚本并通过简单血缘 API 验证血缘图是否构建成功。
- 实测点：在本地启动后端后，/api/simple_lineage 能返回简化的血缘边信息，帮助你快速验证上下游关系是否符合预期。

目录结构（简要）
- backend/            数据血缘后端实现
- data/                CSV 示例数据与输出
- data-lineage/        数据血缘核心入口，包含 start.sh/stop.sh 与 API
- templates/           落地模板示例（可选）
- tools/               辅助脚本，例如 mock 数据生成
- README.md             本文档（本文件）

版本与演进
- 本 README 记录为简化血缘 API 的落地与演练指南，后续如需扩展至完整血缘图谱及元数据治理，可逐步增加血缘可视化、字段级血缘等功能。

如需我基于你们的域名和数据模型，定制一个更完整的示例（包含数据字典、字段血缘、以及一个最小的前端演示），告诉我你的域名清单与要分析的表名即可。
