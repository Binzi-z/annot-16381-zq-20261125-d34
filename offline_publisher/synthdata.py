# -*- coding: utf-8 -*-
"""确定性合成数据：合成日志、词典、倒排索引三合一的离线检索包。

不使用任何随机熵源（固定种子的 ``random.Random``），因此任何机器、任何时间
生成的数据集逐字节一致，便于回归与基准复现。

刻意构造的关键结构（用于证明撤销语义）：
- 存在一条被补记为受限的来源 ``REVOKED_ID``；
- 它引用的*共享词条*同时被其它公开/内部来源引用 -> 撤销后词条必须保留，
  仅受限来源自己的倒排项被剔除；
- 它还引用一个*独占词条*（含唯一 CANARY 摘要）-> 撤销后该词条整体消失；
- 每条倒排项的摘要都内嵌 source/page/offset，形成独一无二的泄漏锚点，
  可以对公开清单做“字节级”扫描而不存在整数巧合误判。
"""
from __future__ import annotations

import json
import random

from .model import Classification, Dataset, PostingRef, SourceRecord

#: 被补记为受限的来源 id（基准与小用例都固定，方便断言）。
REVOKED_ID = 4242
#: 保证“另有授权来源引用”的锚点词条，受限来源与来源 0 都引用它们。
ANCHOR_TERMS = ["term_anchor_alpha", "term_anchor_beta"]


def _make_posting(source_id: int, term: str, slot: int) -> PostingRef:
    # 每个来源占一条日志页，页内每条引用错开 128 字节；位置完全确定。
    page = 1000 + source_id
    offset = 64 + slot * 128
    summary = f"CANARY|s{source_id}|p{page}|o{offset}|{term}"
    return PostingRef(source_id, page, offset, summary)


def build_dataset(n_sources: int, terms_per_source: int, *,
                  shared_pool_size: int = 2000, seed: int = 20261125,
                  revoked_id: int = REVOKED_ID) -> Dataset:
    """构造数据集。基准默认 6000 来源 × 10 引用 = 60000 条倒排记录。"""
    if n_sources <= revoked_id:
        raise ValueError("n_sources 必须大于 revoked_id，以便构造撤销来源")
    rng = random.Random(seed)
    shared_terms = [f"term_shared_{i:05d}" for i in range(shared_pool_size)]

    sources: dict[int, SourceRecord] = {}
    dictionary: set[str] = set()

    for sid in range(n_sources):
        tenant = f"tenant-{sid % 7}"
        # 冻结水位单调递增；留少量水位间隔，供读者固定不同水位。
        freeze_wm = 1000 + sid * 2
        classification = (Classification.INTERNAL if sid % 3 == 0
                          else Classification.PUBLIC)

        picks: list[str] = []
        # 前两个槽位放锚点词条，保证受限来源的共享词条确有授权引用。
        if sid == revoked_id or sid == 0:
            picks.extend(ANCHOR_TERMS)
        while len(picks) < terms_per_source - 1:
            t = shared_terms[rng.randrange(shared_pool_size)]
            if t not in picks:
                picks.append(t)
        # 最后一个槽位是来源独占词条（含唯一 CANARY），撤销后应整体消失。
        unique_term = f"term_unique_s{sid:06d}"
        picks.append(unique_term)

        postings = {}
        for slot, term in enumerate(picks):
            postings[term] = _make_posting(sid, term, slot)
            dictionary.add(term)

        # 来源日志自身在合成日志卷中的位置（与倒排项页号区分开）。
        log_page = 200000 + sid
        log_offset = 16
        sources[sid] = SourceRecord(
            source_id=sid, tenant=tenant, freeze_wm=freeze_wm,
            classification=classification, postings=postings,
            log_page=log_page, log_offset=log_offset)

    # 撤销：把指定来源补记为受限。撤销发生在远晚于冻结水位的发布水位。
    sources[revoked_id].classification = Classification.RESTRICTED
    watermark = 1000 + n_sources * 2 + 500
    return Dataset(sources=sources, dictionary=sorted(dictionary),
                   watermark=watermark)


def build_small_case() -> Dataset:
    """手工语义小用例：3 个租户、4 条来源，覆盖保留/剔除/水位/撤销。"""
    def ref(sid: int, term: str, slot: int) -> PostingRef:
        return _make_posting(sid, term, slot)

    s0 = SourceRecord(
        0, "tenant-a", freeze_wm=10, classification=Classification.PUBLIC,
        postings={
            "word_shared": ref(0, "word_shared", 0),
            "word_alpha": ref(0, "word_alpha", 1),
            ANCHOR_TERMS[0]: ref(0, ANCHOR_TERMS[0], 2),
        },
        log_page=200000, log_offset=16)
    s1 = SourceRecord(
        1, "tenant-b", freeze_wm=20, classification=Classification.INTERNAL,
        postings={
            "word_shared": ref(1, "word_shared", 0),
            "word_beta": ref(1, "word_beta", 1),
        },
        log_page=200001, log_offset=16)
    # 被撤销来源：共享词条应保留，独占词条 word_secret 必须整体消失。
    s2 = SourceRecord(
        2, "tenant-c", freeze_wm=30, classification=Classification.RESTRICTED,
        postings={
            "word_shared": ref(2, "word_shared", 0),
            ANCHOR_TERMS[0]: ref(2, ANCHOR_TERMS[0], 1),
            "word_secret": ref(2, "word_secret", 2),
        },
        log_page=200002, log_offset=16)
    # 未来来源：冻结水位高于读者固定水位时应被水位规则隐藏。
    s3 = SourceRecord(
        3, "tenant-a", freeze_wm=999, classification=Classification.PUBLIC,
        postings={"word_future": ref(3, "word_future", 0)},
        log_page=200003, log_offset=16)

    dictionary = sorted({t for s in (s0, s1, s2, s3) for t in s.postings})
    return Dataset(sources={0: s0, 1: s1, 2: s2, 3: s3},
                   dictionary=dictionary, watermark=100)


# --------------------------------------------------------------------------- #
# JSON 落地：让参考扫描器可以完全脱离发布器代码，独立读取原始包进行核对。
# --------------------------------------------------------------------------- #
def export_sources_json(ds: Dataset, path: str) -> None:
    payload = {
        "watermark": ds.watermark,
        "dictionary": ds.dictionary,
        "sources": [
            {
                "source_id": s.source_id,
                "tenant": s.tenant,
                "freeze_wm": s.freeze_wm,
                "classification": int(s.classification),
                "log_page": s.log_page,
                "log_offset": s.log_offset,
                "postings": [
                    {"term": term, "source_id": r.source_id, "page": r.page,
                     "offset": r.offset, "summary": r.summary}
                    for term, r in sorted(s.postings.items())
                ],
            }
            for s in sorted(ds.sources.values(), key=lambda x: x.source_id)
        ],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)


def import_sources_json(path: str) -> Dataset:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    sources: dict[int, SourceRecord] = {}
    for item in payload["sources"]:
        postings = {
            p["term"]: PostingRef(p["source_id"], p["page"], p["offset"],
                                  p["summary"])
            for p in item["postings"]
        }
        sources[item["source_id"]] = SourceRecord(
            source_id=item["source_id"], tenant=item["tenant"],
            freeze_wm=item["freeze_wm"],
            classification=Classification(item["classification"]),
            postings=postings, log_page=item["log_page"],
            log_offset=item["log_offset"])
    return Dataset(sources=sources, dictionary=list(payload["dictionary"]),
                   watermark=payload["watermark"])
