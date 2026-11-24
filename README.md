# 受控发布的密级回收与索引一致性

原生 Windows、**仅 Python 3.12 标准库**实现的离线检索包发布器。它解决的问题：
一条已发布记录事后被补记为「受限」，需要在保留其它租户对同一词条合法引用的
前提下，把该受限来源从倒排索引与一切公开清单中彻底回收，并保证并发崩溃下
读者只能看到完整、一致的可见集合。

## 如何满足每一条要求

| 题面要求 | 实现位置 |
| --- | --- |
| 按原始来源 + 冻结水位判断每个索引项可见性 | `visibility.py`：密级规则（受限回溯生效，任何水位不可见）+ 水位规则（`freeze_wm > 读者水位` 不可见） |
| 保留另有授权来源的词条 | `VisibilityPolicy.public_view` 按词条聚合可见来源；共享词条仅剔除受限来源那一条 posting，独占词条整体移除 |
| 证明受限来源的页号/偏移/摘要不出现在公开清单任何变体 | `manifest.verify_no_leak`：字节级扫描（唯一摘要 + 完整受限记录字节）+ 结构化精确配对（来源 id、页号/偏移），覆盖全部全量快照与 delta；`test_leak.py` 还有「投毒反证」证明校验器确实能抓到泄漏 |
| 读者固定水位 | `SnapshotReader` / `public_view(ds, reader_wm)`，水位是显式入参 |
| 两个真实线程：持续读取 + 撤销更新 | `tests/test_concurrency.py`：`threading.Thread` 两个守护线程 |
| 屏障控制两种崩溃场景 | `Barrier` + `Event`：CP1（PREPARED 承诺 durable、指针切换前）、CP2（指针已切换、COMMITTED 回执丢失） |
| 恢复后只读到完整可见集合 | `index.recover`：CP1 前滚指针 + 补回执，CP2 幂等补回执；恢复后与规范视图逐字节比对 |
| 逐来源扫描的小参考工具 | `scanner.py`：只读 JSON、自带独立判定、不导入发布器逻辑；`cross_check` 与清单交叉核对 |
| 60000 条记录比较增量/全量 | `benchmark.py`：6000 来源 × 10 引用，真实计数 `pages_written / bytes_rewritten / pages_read` 与文件大小 |

## 目录结构

```
offline_publisher/
  model.py       密级、来源记录、PostingRef、页大小常量
  visibility.py  可见性策略（水位 + 回溯撤销 + 词条保留）
  synthdata.py   确定性合成数据（含受限来源、共享/独占词条、CANARY 摘要）+ JSON 落地
  paged.py       固定页文件：影子页 COW、CRC32、链式页表、原子指针、精确 I/O 计数
  journal.py     append-only WAL：PREPARED/COMMITTED 两阶段、CRC、撕裂识别
  index.py       词典桶链 + posting 链、增量/全量路径、两阶段事务、恢复、快照读取器
  manifest.py    全量/增量公开清单、delta 应用、字节级 + 结构化不泄漏证明
  scanner.py     独立逐来源扫描参考工具（CLI）
  publisher.py   门面，串联完整发布流程
  benchmark.py   60000 条增量 vs 全量基准（CLI）
tests/           unittest：可见性、双线程屏障崩溃恢复、不泄漏、参考扫描交叉核对
run_all.py       一键：全部测试 -> 参考扫描 -> 60000 基准
demo_small.py    小用例双崩溃点恢复演示
```

## 运行

```bash
python run_all.py                       # 全量测试 + 基准
python -m offline_publisher.scanner artifacts/.../sources.json --watermark 100 \
    --manifest-dir artifacts/.../manifests
python -m offline_publisher.benchmark --keep --out artifacts/benchmark
python demo_small.py
```

## 崩溃安全协议（关键设计）

事务严格按下列顺序，两个崩溃点都由测试用屏障确定性触发：

1. 影子页/新页 + 新链式页表落盘并 `fsync`（旧版本仍被旧指针完整引用）；
2. **PREPARED 日志** flush+`fsync` —— 撤销「承诺」durable；
   - **CP1 在此之后、指针切换之前**：崩溃后日志有承诺、指针仍旧 → 恢复时前滚；
3. 指针文件 `os.replace` 原子切换（Windows 上同目录原子替换）；
   - **CP2 在此之后、回执之前**：指针已指向新代次、回执丢失 → 恢复时仅幂等补回执；
4. **COMMITTED 日志** flush+`fsync`。

指针文件要么是旧内容要么是新内容，读者每次重新打开并加载指针，因此永远只
看到完整提交版本；撕裂指针由 CRC/长度校验检出并重试，绝不返回半成品。

## 增量为什么省

- posting 侧：词典项带 posting 内容 CRC，未变化词条的 posting 页**一次都不读**；
- 词典侧：词条散列到 256 个桶（桶可链式多页），增量只重写**脏桶**；
- 页表与数据文件只追加，旧影子页保留到显式离线 `compact()`，因此
  `bytes_rewritten` 严格等于真实落盘页数 × 页大小，不掺压缩写。
