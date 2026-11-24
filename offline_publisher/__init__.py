# -*- coding: utf-8 -*-
"""受控发布的密级回收与索引一致性（仅 Python 3.12 标准库，原生 Windows）。"""
from .model import Classification, Dataset, PostingRef, SourceRecord
from .visibility import VisibilityPolicy
from .journal import SimulatedCrash
from .index import InvertedIndex, SnapshotReader, view_digest
from .publisher import Publisher
from .manifest import verify_no_leak

__all__ = [
    "Classification", "Dataset", "PostingRef", "SourceRecord",
    "VisibilityPolicy", "SimulatedCrash", "InvertedIndex",
    "SnapshotReader", "view_digest", "Publisher", "verify_no_leak",
]
