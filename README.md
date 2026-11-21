# 示波器波形采集平台

实验室数据工程师向频谱分析交付示波器波形的采集服务：FastAPI + SQLite 采集网页，
附本地 SCPI/TCP 示波器模拟器。每次采集冻结捕获单通道 4096 个小端 16 位有符号样本，
读取采样间隔 / 电压比例 / 偏移，波形以 IEEE 488.2 二进制块传输，完成后可下载
电压-时序 CSV。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

* 网页：<http://localhost:8000> （选通道 → 开始采集 → 完成后下载 CSV）
* 模拟器 SCPI 端口：`localhost:5025`（可用 `nc` 或自写脚本直接发 SCPI 命令）

演示增量刷新（同一条记录 排队 → 完成，旧记录移出列表）：

```bash
# 放慢分段传输，让“排队中 / 采集中”持续数秒，便于观察状态原地更新
SIM_SEGMENT=256 SIM_SEGMENT_DELAY=0.4 docker compose up --build

# 浏览器打开带小窗口的波形页，最容易看到旧记录移出列表（默认 50 条）
#   http://localhost:8000/?limit=3
# 连续点几次「开始采集」：新记录从顶部进入，最旧的记录从底部移出。
```

## 本地开发启动（不用 Docker）

```bash
pip install fastapi 'uvicorn[standard]'
python3 simulator/scpi_simulator.py &                 # 监听 5025
cd web && SCOPE_PORT=5025 python3 -m uvicorn app.main:app --port 8000
```

## 组成

```
simulator/scpi_simulator.py   SCPI/TCP 示波器模拟器（纯标准库）
web/app/main.py               FastAPI 路由（采集/取消/状态/CSV）
web/app/manager.py            采集队列 worker：仪器串行访问、取消语义
web/app/instrument.py         仪器客户端：单轮单连接、5s 超时、IEEE488.2 块解析
web/app/db.py                 SQLite 持久化
web/app/static/index.html     操作网页（中文）
web/app/static/refresh.js     增量列表的客户端合并/防回退模型（浏览器 + Node 测试共用）
tests/selftest.py             端到端自测（启动两端真实进程）
tests/test_refresh.py         增量刷新 API 端到端自测
tests/test_refresh_model.js   增量合并模型单元测试（Node）
tests/test_refresh_dom.js     波形页 DOM 级自测（jsdom，可选依赖）
docker-compose.yml            一键启动两端
```

## 关键设计（对应需求）

* **参数与样本同次捕获**：每轮采集先 `:STOP` 冻结，再依次读取
  `:WAVeform:XINcr?/YMUlt?/YOFf?/POINts?` 与 `:WAVeform:DATA?`（同一连接内完成），
  参数与样本用**一条 SQL UPDATE** 原子写入，绝不同次错位。
* **整块收齐才供下载**：样本 BLOB 只在完整收齐 8192 字节后落库；
  未完成时 `GET /api/acquisitions/{id}/csv` 返回 409。
* **同仪器串行**：单个 asyncio worker 消费队列，仪器会话严格串行。
* **排队取消不发命令**：排队中的任务取消只改状态，worker 出队时跳过
  （自测用模拟器侧连接数 / DATA? 计数验证：被取消任务 0 命令）。
* **采集中取消**：取消请求立即受理（状态 `cancelling`），中止当前通信轮并关闭
  本轮连接后，才上报 `cancelled`。
* **单轮 5 秒超时**：每个 SCPI 命令-应答往返（含整个二进制块传输）限时 5 秒
  （`SCOPE_TIMEOUT` 可配）。
* **迟到字节不串单 / 旧收尾不误关新连接**：每轮采集使用**全新 TCP 连接**，
  结束（成功/超时/取消/断连）即关闭丢弃；迟到字节只会进入已死 socket；
  清理只触碰本轮自己的 writer，结构上不可能误关新连接。后续采集无需重启。
* **慢响应不挡网页**：仪器 I/O 全部在后台 worker，HTTP 接口只读写 SQLite。
* **二进制安全**：样本含 `0x0A/0x0D` 字节，客户端严格按块长度读取，绝不按行解析。
* **列表增量刷新**：波形页首次加载取最近 50 条（按 ID 倒序）；之后每秒带
  `since`（上次的版本号）+ `epoch` 轮询，只接收新增/变化的行与移出窗口的编号；
  无变化时响应不含 `records`/`removed`。列表中旧任务完成/取消/失败时原地更新
  状态与下载按钮。迟到响应按 (epoch, started, cursor) 判定并丢弃，不会把页面
  拉回旧状态。服务端刷新历史有固定上限（`REFRESH_HISTORY_CAP`，默认 1000 个
  版本号）；基准过旧、缺失或服务重启时自动回退为整页。
* **列表只取元数据**：列表与刷新查询只 SELECT 元数据列并以
  `samples IS NOT NULL` 派生 `has_samples`，不读取 8 KiB 样本 BLOB；
  原 `/api/acquisitions` 与 CSV 下载行为保持不变（CSV 仍单独读取 BLOB）。

## 模拟器故障注入（SCPI 扩展命令，运行时生效，无需重启）

```
:SIM:WAVE SINE|SQUARE|RAMP|NOISE   切换波形
:SIM:SEG <字节>                    波形分段的段大小
:SIM:SEGD <秒>                     段间延迟（分段发送）
:SIM:TAILD <秒>                    末段额外延迟（延迟尾部 -> 触发超时）
:SIM:DROP <字节>                   发送 N 字节块数据后断连（中途断连，0=关）
:SIM:RESPD <秒>                    每次应答延迟（慢响应）
:SIM:RESET                         恢复环境变量默认值
:SIM:STATS? / :SIM:CONFIG?         统计 / 当前配置
```

也可用环境变量预设（见 `docker-compose.yml` 注释）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 操作网页 |
| GET | `/api/health` | 仪器地址与超时配置 |
| POST | `/api/acquisitions` `{channel: 1\|2}` | 提交采集（排队） |
| GET | `/api/acquisitions` | 最近采集列表（元数据） |
| GET | `/api/acquisitions/changes?since=&epoch=&limit=` | 增量刷新：整页或增量（见下） |
| GET | `/api/acquisitions/{id}` | 单个采集状态 |
| POST | `/api/acquisitions/{id}/cancel` | 取消（排队/采集中均可） |
| GET | `/api/acquisitions/{id}/csv` | 下载电压-时序 CSV（仅完成后） |

状态机：`queued → running → done`，取消路径 `queued → cancelled`、
`running → cancelling → cancelled`，失败为 `error`（含错误信息）。

`GET /api/acquisitions/changes` 的响应：

```jsonc
// 整页（首次加载 / 无 since / epoch 不匹配 / 基准超出服务端历史）
{"mode":"full","epoch":"…","started":1730000000.0,"cursor":42,"records":[…]}
// 增量：只含新增或变化的行
{"mode":"delta","epoch":"…","started":1730000000.0,"cursor":45,"records":[…]}
// 增量：只含移出窗口的编号
{"mode":"delta","epoch":"…","started":1730000000.0,"cursor":46,"removed":[7]}
// 无变化：两个键都不出现
{"mode":"delta","epoch":"…","started":1730000000.0,"cursor":46}
```

`records` 的每一行与原列表相同（含 `has_samples`），不含样本 BLOB。

## 自测

```bash
python3 tests/selftest.py          # 采集链路端到端
python3 tests/test_refresh.py      # 增量刷新 API 端到端（含重启 / 基准过期）
node tests/test_refresh_model.js   # 客户端合并与防回退模型

# 可选：波形页 DOM 级自测（需要 jsdom）
npm install --no-save jsdom
NODE_PATH=$PWD/node_modules node tests/test_refresh_dom.js
```

`tests/selftest.py` 拉起真实模拟器与 Web 进程，覆盖：正常采集与 CSV 数值校验
（含每通道参数）、样本含换行字节、排队取消零命令、采集中取消的受理/上报时序、
5 秒超时、中途断连后恢复、断连后新采集拿到当前波形（无陈旧字节）、
慢响应下网页 API 延迟。

`tests/test_refresh.py` 额外覆盖：首屏整页、无变化轮询不带记录、旧任务完成
原地更新（状态 + 下载可用）、移出窗口的编号、基准超出固定历史回退整页、
服务重启后 epoch 变化回退整页、原列表接口与 CSV 行为不变。

## 已验证 / 未完成

见交付说明（对话总结）。Docker 镜像构建与 `docker compose up` 在本环境
未执行（无 docker CLI / 守护进程），Compose 与 Dockerfile 已做静态校验
（YAML 解析、健康检查命令）。
