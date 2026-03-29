from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional


@dataclass(frozen=True)
class IndustryProfile:
    raw_industry: str
    industry_type: str
    core_drivers: tuple[str, ...]
    forbidden_valuation_methods: tuple[str, ...]
    recommended_factors: tuple[str, ...]
    notes: str
    mapping_source: str
    needs_manual_confirm: bool


def _project_root() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))


def _read_json_file(path: str) -> Optional[dict[str, Any]]:
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _industry_attribute() -> dict[str, Any]:
    path = os.path.join(_project_root(), "config", "industry_attribute.json")
    data = _read_json_file(path)
    if not data:
        return {"types": {}, "industry_to_type": {}}
    return data


@lru_cache(maxsize=1)
def _custom_industry_mapping() -> dict[str, Any]:
    path = os.path.join(_project_root(), "config", "custom_industry_mapping.json")
    data = _read_json_file(path)
    if not data:
        return {"stock_code_to_type": {}}
    return data


def get_industry_profile(ts_code: str, raw_industry: str) -> IndustryProfile:
    raw_industry = (raw_industry or "").strip()
    attr = _industry_attribute()
    custom = _custom_industry_mapping()

    types = attr.get("types") or {}
    industry_to_type = attr.get("industry_to_type") or {}

    custom_type = (custom.get("stock_code_to_type") or {}).get(ts_code)
    mapping_source = "custom_industry_mapping.json" if custom_type else "industry_attribute.json"

    industry_type = (custom_type or industry_to_type.get(raw_industry) or "").strip()

    needs_manual_confirm = False
    if not industry_type:
        industry_type = "通用实体"
        mapping_source = "fallback"
        needs_manual_confirm = True

    t = types.get(industry_type) or types.get("通用实体") or {}

    def _to_tuple(v: Any) -> tuple[str, ...]:
        if not v:
            return tuple()
        if isinstance(v, (list, tuple)):
            return tuple(str(x) for x in v if str(x).strip())
        return (str(v),)

    return IndustryProfile(
        raw_industry=raw_industry,
        industry_type=industry_type,
        core_drivers=_to_tuple(t.get("core_drivers")),
        forbidden_valuation_methods=_to_tuple(t.get("forbidden_valuation_methods")),
        recommended_factors=_to_tuple(t.get("recommended_factors")),
        notes=str(t.get("notes") or ""),
        mapping_source=mapping_source,
        needs_manual_confirm=needs_manual_confirm,
    )

