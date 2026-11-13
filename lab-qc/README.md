# 环境实验室仪器读数入仓质控校验 API

FastAPI + SQLite 实现的仪器 CSV 入仓校验服务。上传 CSV（样品号、项目、整数读数），
按**当前生效规则**（各项目含端点的整数上下限闭区间）**整批接收或拒绝**；规则替换在
**后台线程**核查历史数据，核查期间系统继续按旧规则接收上传与查询，只有切换瞬间确认
**全部已提交记录均合格**才原子启用新规则，否则保留旧规则并列出违规样品。

## 目录结构

```
lab-qc/
├── app/
│   ├── __init__.py
│   ├── database.py      # SQLite 连接、WAL、表结构、启动恢复
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── services.py      # 规则、CSV 整批入库、替换后台核查/切换
│   └── main.py          # FastAPI 路由
├── tests/test_api.py    # 8 个端到端测试（含并发与重启恢复）
├── samples/             # 演示用 CSV
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── demo.sh              # 可选：curl 全流程演示脚本
```

## 快速启动（Docker Compose）

```bash
cd lab-qc
docker compose up --build
```

启动后访问：

| 地址 | 用途 |
| --- | --- |
| http://localhost:8000/docs | Swagger 交互式 API 文档（可直接 Try it out） |
| http://localhost:8000/redoc | ReDoc 文档 |
| http://localhost:8000/health | 健康检查 |

SQLite 数据保存在命名卷 `qc-data`（容器内 `/data/lab_qc.db`），容器重建数据不丢。

> 注意：服务设计为**单进程单 worker**（SQLite + 进程内后台替换线程 + 内存替换锁），
> 请勿加 `--workers >1`。水平扩展时需把替换锁/后台任务迁移到独立组件。

## 无 Docker 时本地运行

```bash
pip install -r requirements.txt
LAB_QC_DB=./lab_qc.db uvicorn app.main:app --host 0.0.0.0 --port 8000
pytest tests/          # 运行测试
bash demo.sh           # curl 全流程演示（默认 BASE_URL=http://127.0.0.1:8000）
```

## API 总览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/rules` | 提交**首版**规则；已有生效规则时返回 409，变更必须走替换 |
| GET | `/rules/active` | 查询当前生效规则（替换核查期间仍返回旧规则） |
| GET | `/rules/{rule_id}` | 查询任意规则 |
| POST | `/rules/replacements` | 发起规则替换，立即返回 202，后台核查，不阻塞上传 |
| GET | `/rules/replacements/{id}` | 查询替换状态与违规样品明细 |
| GET | `/rules/replacements` | 替换单列表 |
| POST | `/uploads` | 上传 CSV，整批校验入库（multipart 字段名 `file`） |
| GET | `/batches` | 批次查询（可按 `rule_id` 过滤） |
| GET | `/records` | 记录查询（可按 `batch_id`/`project` 过滤） |
| GET | `/health` | 健康检查与启动恢复信息 |

### CSV 格式

表头支持中英文：`样品号,项目,读数` 或 `sample_no,project,reading`，读数必须是整数；
空行跳过，任何格式问题整批拒收（422）。

## 交互文档演示流程（/docs 中按顺序 Try it out）

1. **POST `/rules`** 提交首版规则（见下方请求体示例）。
2. **POST `/uploads`** 选择 `samples/batch_ok.csv` → 整批接收，返回 `rule_id`。
3. 再传 `samples/batch_bad.csv` → 422 整批拒收，响应列出每条违规原因；查询
   `/batches` 确认没有留下半批。
4. **POST `/rules/replacements`** 提交更严格的新规则 → 返回 202，`status=checking`。
5. **GET `/rules/replacements/{id}`** 轮询：
   - 历史全部合格 → `switched`，`GET /rules/active` 显示新规则号；
   - 存在违规 → `failed`，`violations` 列出违规样品号/项目/读数/原因，旧规则保持生效。
6. 替换核查期间随时可以继续 **POST `/uploads`**，始终按提交时的生效规则校验。

首版规则请求体示例：

```json
{
  "note": "2026 年度质控范围",
  "limits": [
    {"project": "pH",  "lower_int": 6, "upper_int": 9},
    {"project": "COD", "lower_int": 10, "upper_int": 300}
  ]
}
```

## 关键设计与需求对应

| 需求 | 实现方式 |
| --- | --- |
| 首次先提交规则，缺规则拒收 | 无 active 规则时 `/uploads` 返回 409 |
| 含端点整数上下限 | 校验 `lower <= reading <= upper`（SQL `BETWEEN` 同语义） |
| 整批接收/拒绝，不留半批 | 上传在单事务 `BEGIN IMMEDIATE` 内先全量校验再插入，任一不合格即回滚 |
| 整批记录与所用规则号一起保存 | `batches.rule_id` 与 `records.rule_id` 在同一事务写入 |
| 并发上传按提交时生效规则校验，不漏检 | 上传事务持有 active 规则行的读语义；规则切换也在写事务内，二者由 SQLite 串行化，规则号在事务内读取 |
| 后台核查不占入库写事务 | 核查在独立只读事务中执行；WAL 模式下读不阻塞写，`PRAGMA busy_timeout` 防写锁冲突 |
| 核查期间不暂停上传 | 替换接口仅建单并启动守护线程，立即返回；旧规则全程保持 active |
| 切换时全部已提交记录合格才启用 | 切换事务内做 tail check（复核核查期间新入库且 id 大于快照的记录），合格才原子翻转 `is_active` |
| 失败列出违规样品 | 违规明细（record/batch、样品号、项目、读数、原因）持久化在替换单上 |
| 只允许一次替换 | `replacements` 在途状态检查 + `replacement_locks` 单行表双保险，第二次请求 409 |
| 失败/中断保留旧规则 | 失败路径只更新替换单，绝不改 active 标志 |
| 重启后未完成替换标失败 | 启动 `recover_unfinished()` 把 `checking/pending_switch` 置 `failed` 并释放锁；已入库记录与生效规则原样保留 |

## 测试

```bash
pytest tests/ -v
```

覆盖：无规则拒收、边界值、缺规则项目整批拒收、替换成功切换、替换失败保留旧规则并
列违规、在途替换互斥、8 路并发上传无半批、tail check 拦截、重启恢复后可重新替换。
