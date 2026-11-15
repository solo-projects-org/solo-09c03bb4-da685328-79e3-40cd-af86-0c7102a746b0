# 机场入口客流 · DST 回拨日 UTC 入仓服务

纽约（`America/New_York`）入口计数器在 **2024-11-03 时钟回拨** 当天导出的
CSV 只有本地钟面时刻、不带偏移。回拨瞬间时钟从 `02:00 EDT` 跳回 `01:00 EST`，
于是本地 `[01:00, 02:00)` 这一个小时会**走两遍**，同一钟面时刻对应两个
UTC（相差 1 小时）。本服务把补传 CSV 原子入仓，并只对外发布 **UTC 在所有
合法时间线中都唯一** 的客流明细；其余采样号列为"待定"，待后续补传消除歧义。
已结算的早期采样可按**截断号清退**腾出入仓空间：清退保留必要的时间线边界，
剩余读数的 UTC 判定与后续补传的合法性都与未清退时完全一致。

- 技术栈：FastAPI + SQLite（WAL）+ 标准库 `zoneinfo`，Docker Compose 启动
- 无第三方时区库：`fold=0/1` 直接得到两个候选 UTC
- 核心算法只依赖标准库，可在没有 Web 框架的环境中完整单测

## 1. 目录结构

```
app/
  timeline.py   # DST 候选 UTC、合法时间线贪心夹逼（纯标准库）
  db.py         # SQLite 连接、WAL、BEGIN IMMEDIATE 写事务、快照读
  service.py    # 整批受理规则、前缀清退、歧义查询、审计
  csv_utils.py  # 补传 CSV 校验与解析
  main.py       # FastAPI 路由 / Swagger
samples/        # 回拨日演示 CSV（含 2 个清退后补传场景）
tests/          # 36 个测试（含暴力枚举对照、并发测试、HTTP 测试）
scripts/demo.sh # 一键 curl 演示（含清退前后对比）
Dockerfile, docker-compose.yml, requirements.txt
```

## 2. 启动

```bash
docker compose up --build
# Swagger UI:  http://localhost:8000/docs
# ReDoc:       http://localhost:8000/redoc
# OpenAPI:     http://localhost:8000/openapi.json
```

SQLite 数据文件放在命名卷 `traffic-data`（容器内 `/srv/data`），
`samples/` 与 `scripts/` 以只读卷挂入容器。

## 3. 核心规则

| 规则 | 处理 | HTTP |
|---|---|---|
| 同采样号、同内容重复补传 | 幂等跳过，只收一次 | 200 |
| 同采样号、不同内容 | **整批拒收**，已入库数据不变 | 409 |
| 与全表合并后排不出合法 UTC 时间线 | **整批回滚** | 422 |
| CSV 格式非法（缺列/带偏移/负数/批内重复等） | 拒收 | 400 |
| 批次含 ≤ 截断号的采样号（已清退） | **整批拒收** | 409 |
| 清退截断号未前移（≤ 当前截断号） | 拒绝，未做任何删除 | 409 |

"合法时间线"指：按采样号升序，每个采样选择一个候选 UTC，
使 UTC **严格递增**（采样号越大 UTC 必须越晚）。

### 待定（pending）如何判定

对每个采样做两遍贪心：

- 前向：在严格递增前提下取**最早**可行 UTC；
- 反向：取**最晚**可行 UTC。

若某采样的最早值 == 最晚值，则它在**每一条**合法时间线中都是同一 UTC，
可安全供数；否则它存在多个可能 UTC，只列入 `pending_sample_ids`，
并附带 `possible_utc_earliest / possible_utc_latest`。
该贪心结论已用**全量暴力枚举**所有时间线交叉验证（见 `tests/test_timeline.py`）。

### 并发安全

- 所有写入走 `BEGIN IMMEDIATE`，SQLite 写锁在事务开始即获取，
  同一时刻只有一个批次在校验/落库，并发的冲突批次不可能同时被收下；
- 可行性校验基于**全表快照**而非仅本批采样号（避免局部合法、全局矛盾）；
- 查询在单个读事务里取一致性快照，绝不会把新记录与旧判定拼接；
- 拒收审计通过独立 autocommit 连接写入，业务事务回滚后审计仍保留。

### 前缀清退（已结算采样）

`POST /purge` 接受一个**截断号**，删除所有 `sample_id ≤ 截断号` 的原始读数，
返回清退数量与新截断号。规则与不变式：

- **截断号只前移**：不大于当前截断号的清退请求直接拒绝（409），
  不留下任何部分删除；清退的删除、边界更新、审计在同一个
  `BEGIN IMMEDIATE` 事务里提交，与并发补传完全串行化；
- **已清退号永不回收**：之后任何批次只要含 `≤ 截断号` 的采样号就整批
  拒收（409）——原始读数已删，连内容比对都无法进行；
- **边界保留**：清退时把"最后被清退且真实存在过的采样"的**最早候选
  UTC** 存为时间线边界（`purge_state` 单例行）。数学上，被清退前缀对
  剩余数据的全部影响（前向贪心种子）就压缩在这一个值里：以它为种子
  重跑分析，剩余读数的确定/待定判定、以及任何后续补传（采样号 >
  截断号）的可行性，都与未清退时**逐点一致**；
- **重复钟面截断点**：截断点落在 `[01:00, 02:00)` 重复小时时，边界取
  该采样最早候选（EDT 第一遍）或补传消除歧义后已钉死的 UTC，两种
  情形都由同一条规则覆盖；
- **空仓情形**：截断号超过仓内最大采样号时读数清空，但边界仍然保留，
  空仓后的第一批补传依旧受"必须晚于边界"约束，与未清退时相同；
- **重启安全**：截断号与边界持久化在 SQLite（`purge_state`），
  查询与清退状态在同一读事务快照中读取，重启、清退与补传交错都不会
  把旧边界与新读数混用。

`GET /purge` 返回当前截断号、保留的边界与清退历史（`purge_audit`，
随清退事务一并提交，失败的清退不留痕）。

## 4. Swagger 演示（回拨样例）

打开 `http://localhost:8000/docs`，在 `POST /upload` 处依次上传
`samples/` 中的文件，并用 `GET /traffic` 观察补传前后变化。

### 场景一：初始批次（01）

`01_initial_fallback_day.csv`

| sample_id | local_time (New York, 无偏移) | pass_count |
|---:|---|---:|
| 100 | 2024-11-03T00:45:00 | 120 |
| 128 | 2024-11-03T01:20:00 | 340 |
| 148 | 2024-11-03T01:55:00 | 300 |
| 160 | 2024-11-03T02:15:00 | 260 |
| 180 | 2024-11-03T03:10:00 | 210 |

`GET /traffic` 返回 5 条记录中只有 3 条 UTC 唯一（100、160、180），
**128、148 列为待定**。例如 128 的本地 `01:20` 既可能是
`05:20Z`（EDT 第一遍）也可能是 `06:20Z`（EST 第二遍）。

```json
"pending_sample_ids": [128, 148]
```

### 场景二：同内容重传 → 幂等

再次上传 `01`：返回 `inserted_sample_ids: []`，
`skipped_duplicate_sample_ids` 列出全部 5 个号，库内仍只有 5 行。

### 场景三：同号异值 → 409 整批拒收

`03_conflict_same_id_different_value.csv` 把 128 的人数改成 999：

```json
{ "accepted": false,
  "error": "sample_id 128 already stored with different content ..." }
```

### 场景四：排不出合法时间线 → 422 整批回滚

`04_illegal_timeline.csv` 在已存的 128（同为本地 01:20）之外再补
127、129 两条 **同一钟面** 记录。一个歧义钟面只有两个 UTC 槽位，
三条同刻记录无法满足严格递增 → 整批回滚，库内仍为 5 行。

### 场景五：补传消除歧义（02）

`02_backfill_resolves_ambiguity.csv` 仅补一条与 128 **同钟面**的 127：

| sample_id | local_time | pass_count |
|---:|---|---:|
| 127 | 2024-11-03T01:20:00 | 84 |

同钟面的 127、128 被强制分别落到两个 UTC 槽位
（127→`05:20Z`，128→`06:20Z`），连锁地让 148 只能取晚段 `06:55Z`。
此时合法时间线只剩 1 条，`GET /traffic` 的 `pending_sample_ids` 清空，
6 条记录全部带确定 UTC 供数。

### 场景六：清退已结算前缀，继续接收补传

在 `POST /purge` 处提交 `{"truncate_upto_sample_id": 148}`：

```json
{ "purged": true, "deleted_count": 4, "watermark": 148,
  "boundary_sample_id": 148, "boundary_utc": "2024-11-03T06:55:00+00:00" }
```

100/127/128/148 四条已结算读数被删除；截断点 148 落在回拨重复小时，
其钉死的 UTC（`06:55Z`）被保留为时间线边界（`GET /purge` 可查）。
随后：

- `GET /traffic`：只剩 160、180，UTC 判定与清退前完全一致；
- 重传 `01`（含 ≤148 的号）→ **409 整批拒收**；
- 再提交 `{"truncate_upto_sample_id": 100}` → **409**，截断号不许回退；
- 上传 `05_post_purge_backfill.csv`（150@`01:58`）：单看钟面有两个候选
  UTC，但边界 `06:55Z` 只留下晚段 → 直接确定为 `06:58Z`，
  与未清退时补传的结果相同；
- 上传 `06_post_purge_illegal.csv`（149@`01:30`，两个候选 UTC 都不晚于
  边界）→ **422 整批回滚**，同样与未清退时一致。

也可跳过界面直接跑：

```bash
./scripts/demo.sh
```

## 5. 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/upload` | multipart 表单字段 `file`，UTF-8 CSV |
| GET | `/traffic` | 确定 UTC 的明细 + 待定采样号（含当前截断号） |
| POST | `/purge` | JSON `{"truncate_upto_sample_id": N}`，清退 ≤N 的读数 |
| GET | `/purge` | 当前截断号、保留边界与清退历史 |
| GET | `/batches` | 批次受理审计（含被拒收批次与原因） |

CSV 表头必须恰为：`sample_id,local_time,pass_count`
（`local_time` 为朴素本地时间，带 `Z` 或偏移后缀会被拒绝）。

## 6. 测试

容器内（含 HTTP 层，`test_api.py` 需要 fastapi/httpx）：

```bash
docker compose run --rm traffic-api python -m pytest -q
```

仅标准库环境（无 pip 也可运行，HTTP 测试自动 skip）：

```bash
python3 -I -m unittest discover -s tests -t . -v
```

测试内容：

- `test_timeline.py`：时区候选、边界钟面、贪心夹逼，并对回拨窗口内
  所有 4 格钟面组合做**暴力枚举对照**；
- `test_service.py`：整批原子、409/422 回滚、幂等、CSV 校验、审计留存；
- `test_purge.py`：清退数量与截断号、重复钟面/空仓边界保留、
  已清退号整批拒收、截断号只前移、失败不留部分清退、重启后边界仍生效，
  并对多个截断点与未清退参照库做**逐点判定对照**（含补传对等性校验）、
  清退-补传竞态、HTTP 端到端；
- `test_concurrency.py`：多线程抢锁，冲突批次不会同时落地、
  最终全表始终合法、读视图永不新旧拼接；
- `test_api.py`：FastAPI TestClient 端到端（含状态码与审计顺序）。
