"""Pydantic 请求/响应模型与 OpenAPI 示例。"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

ReplacementStatus = Literal["checking", "pending_switch", "switched", "failed"]


class LimitIn(BaseModel):
    project: str = Field(..., min_length=1, examples=["pH"])
    lower_int: int = Field(..., examples=[6])
    upper_int: int = Field(..., examples=[9])

    @field_validator("project")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("项目名不能为空")
        return v


class RuleIn(BaseModel):
    """首次规则或替换用新规则。范围为含端点的整数闭区间。"""
    limits: list[LimitIn] = Field(..., min_length=1)
    note: Optional[str] = Field(default=None, examples=["2026 年度质控范围"])

    @field_validator("limits")
    @classmethod
    def _validate(cls, v: list[LimitIn]) -> list[LimitIn]:
        projects = [x.project for x in v]
        if len(set(projects)) != len(projects):
            raise ValueError("同一规则内项目不能重复")
        for item in v:
            if item.lower_int > item.upper_int:
                raise ValueError(f"项目 {item.project} 的下限不能大于上限")
        return v


class LimitOut(LimitIn):
    pass


class RuleOut(BaseModel):
    rule_id: int
    is_active: bool
    note: Optional[str]
    created_at: str
    limits: list[LimitOut]


class BatchOut(BaseModel):
    batch_id: int
    rule_id: int
    filename: Optional[str]
    row_count: int
    created_at: str


class RecordOut(BaseModel):
    id: int
    batch_id: int
    rule_id: int
    sample_no: str
    project: str
    reading: int
    created_at: str


class ReplacementOut(BaseModel):
    id: int
    new_rule_id: int
    old_rule_id: Optional[int]
    status: ReplacementStatus
    failure_reason: Optional[str]
    snapshot_max_record: Optional[int]
    violation_count: int
    violations: list[dict]
    created_at: str
    finished_at: Optional[str]


class HealthOut(BaseModel):
    status: str
    active_rule_id: Optional[int]
    unfinished_replacements_recovered: list[int]
