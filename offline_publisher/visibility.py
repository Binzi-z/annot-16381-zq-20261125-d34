# -*- coding: utf-8 -*-
"""可见性判定：按原始来源与冻结水位判断每个索引项是否可见。

两条相互正交的规则：

1. 水位（生命周期）规则——读者可以“固定水位”。一条来源只有在
   ``freeze_wm <= 读者水位`` 时才已经发布、才可能被读到；未来水位的内容
   对当前读者不存在。

2. 密级（安全）规则——撤销是*回溯生效*的：一旦来源被补记为 RESTRICTED，
   它在**任何**读者水位、任何公开清单变体下都不再可见。安全规则不接受
   “固定在撤销前的水位就能继续读到受限内容”，因为离线包已实际发布，
   受限信息必须从一切公开产物中清除。

词条保留规则：词条本身不归属任何单一租户。若某词条仍被*另一个有授权的
（公开/内部且已发布的）来源*引用，则词条保留，仅剔除受限来源的那一条
倒排项；只有当词条的全部可见引用都消失时，词条才从公开词典移除。
"""
from __future__ import annotations

from dataclasses import dataclass

from .model import Classification, Dataset, PostingRef, SourceRecord


@dataclass(frozen=True)
class VisiblePosting:
    """公开侧可见的一条倒排项（已确认其来源有授权）。"""

    term: str
    ref: PostingRef


class VisibilityPolicy:
    """无状态判定器：给定数据集与读者固定水位，计算完整可见集合。"""

    def source_visible(self, src: SourceRecord, reader_wm: int) -> bool:
        """单条来源在给定读者水位下是否有授权可见。"""
        # 安全规则优先：受限来源在任何水位都不可见（回溯撤销）。
        if src.classification == Classification.RESTRICTED:
            return False
        # 水位规则：来源必须已经在读者水位之前冻结/发布。
        if src.freeze_wm > reader_wm:
            return False
        return True

    def visible_sources(self, ds: Dataset, reader_wm: int) -> list[SourceRecord]:
        return [s for s in ds.sources.values()
                if self.source_visible(s, reader_wm)]

    def public_view(self, ds: Dataset,
                    reader_wm: int) -> dict[str, list[PostingRef]]:
        """返回公开倒排视图：词条 -> 可见来源的倒排项列表（按 source_id 排序）。

        这是“完整的可见集合”的权威定义；恢复后读者读到的内容必须与此逐字节
        一致（顺序也固定，便于断言）。
        """
        view: dict[str, list[PostingRef]] = {}
        for src in ds.sources.values():
            if not self.source_visible(src, reader_wm):
                continue
            for term, ref in src.postings.items():
                view.setdefault(term, []).append(ref)
        for refs in view.values():
            refs.sort(key=lambda r: (r.source_id, r.page, r.offset))
        return dict(sorted(view.items()))

    def restricted_postings(self, ds: Dataset) -> list[VisiblePosting]:
        """所有受限来源的倒排项——这些锚点绝不允许出现在公开清单中。"""
        out: list[VisiblePosting] = []
        for src in ds.sources.values():
            if src.classification == Classification.RESTRICTED:
                for term, ref in src.postings.items():
                    out.append(VisiblePosting(term, ref))
        return out

    def retained_terms(self, ds: Dataset, reader_wm: int) -> list[str]:
        """因“另有授权来源引用”而得以保留的词条。"""
        return sorted(self.public_view(ds, reader_wm).keys())

    def dropped_terms(self, ds: Dataset, reader_wm: int) -> list[str]:
        """全部可见引用都消失、因而从公开词典移除的词条。"""
        alive = set(self.public_view(ds, reader_wm).keys())
        all_terms = set(ds.dictionary)
        return sorted(all_terms - alive)
