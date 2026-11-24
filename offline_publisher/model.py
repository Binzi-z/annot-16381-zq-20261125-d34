# -*- coding: utf-8 -*-
"""核心数据模型：密级、原始来源记录、倒排项。

整个系统只依赖 Python 3.12 标准库，可直接在原生 Windows 上运行。
术语约定：
- *来源记录 (source record)* 是一份合成日志，拥有唯一的 source_id、一个冻结水位
  (freeze_wm，提交时的逻辑水位) 与当前密级 (classification)。
- *倒排项 (posting)* 是某条来源记录对某个词条的一次引用，携带该词条在来源日志
  中的页号 page、页内偏移 offset 与摘要 summary。
- 密级撤销 (revoke) 只改变来源记录的密级，不改变它对词条的引用关系；因此同一
  词条可能被多个租户的来源记录合法引用，撤掉其中一个来源不应删除词条本身。
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import IntEnum

#: 所有页式文件统一使用的页大小（字节）。Windows 常见分配粒度的公约数。
PAGE_SIZE = 4096

#: 页头：魔数(1) + 页号(4) + 校验和(4) + 有效载荷长度(4) = 13 字节。
PAGE_HEADER = struct.Struct(">BII I")
MAGIC_PAGE = 0x5A


class Classification(IntEnum):
    """密级。数值越大越敏感，撤销即把来源提升到 RESTRICTED。"""

    PUBLIC = 0       # 公开
    INTERNAL = 1     # 内部
    RESTRICTED = 2   # 受限（补记密级后绝不可出现在任何公开清单变体中）


@dataclass(frozen=True)
class PostingRef:
    """一条倒排项：词条 term 在某条来源日志中的精确位置与摘要。"""

    source_id: int
    page: int
    offset: int
    summary: str

    def encode(self) -> bytes:
        """紧凑二进制编码，写入页式文件。"""
        summary_bytes = self.summary.encode("utf-8")
        head = struct.pack(">IIII", self.source_id, self.page, self.offset,
                           len(summary_bytes))
        return head + summary_bytes

    @staticmethod
    def decode(buf: bytes, pos: int) -> tuple["PostingRef", int]:
        source_id, page, offset, slen = struct.unpack_from(">IIII", buf, pos)
        pos += 16
        summary = buf[pos:pos + slen].decode("utf-8")
        return PostingRef(source_id, page, offset, summary), pos + slen


@dataclass
class SourceRecord:
    """一条原始来源（合成日志）。"""

    source_id: int
    tenant: str
    freeze_wm: int                 # 冻结水位：该来源被提交/冻结时的逻辑水位
    classification: Classification
    #: 该来源引用的全部词条 -> 倒排项（位置 + 摘要）
    postings: dict[str, PostingRef] = field(default_factory=dict)
    #: 该来源日志自身落盘的页号与页内偏移（用于泄漏校验的额外锚点）
    log_page: int = 0
    log_offset: int = 0

    def term_count(self) -> int:
        return len(self.postings)


@dataclass
class Dataset:
    """一份离线检索包的内存表示：合成日志、词典、倒排索引三合一。"""

    sources: dict[int, SourceRecord]
    dictionary: list[str]
    watermark: int                 # 当前发布水位（单调递增的逻辑时间）

    def all_postings(self) -> list[tuple[str, PostingRef]]:
        out: list[tuple[str, PostingRef]] = []
        for src in self.sources.values():
            for term, ref in src.postings.items():
                out.append((term, ref))
        return out

    def posting_count(self) -> int:
        return sum(s.term_count() for s in self.sources.values())
