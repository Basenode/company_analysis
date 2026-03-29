#!/usr/bin/env python3
"""Data Validation Module - Compare Tushare API data with PDF extracted data.

This module provides:
1. Cross-validation between Tushare API and PDF extracted financial data
2. Difference annotation with tolerance thresholds
3. Missing critical information flagging

Usage:
    from data_validator import DataValidator
    
    validator = DataValidator(tushare_data, pdf_sections)
    report = validator.validate()
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ValidationResult:
    section_id: str
    field_name: str
    tushare_value: Optional[Any] = None
    pdf_value: Optional[Any] = None
    status: str = "ok"
    diff_pct: Optional[float] = None
    message: str = ""
    is_critical: bool = False


@dataclass
class ValidationReport:
    ts_code: str
    results: List[ValidationResult] = field(default_factory=list)
    missing_critical: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_markdown(self) -> str:
        lines = []
        lines.append("## 数据校验报告")
        lines.append("")
        
        diff_results = [r for r in self.results if r.status == "diff"]
        missing_results = [r for r in self.results if r.status == "missing"]
        ok_results = [r for r in self.results if r.status == "ok"]
        
        if diff_results:
            lines.append("### ⚠️ 数据差异标注")
            lines.append("")
            lines.append("| 字段 | Tushare值 | PDF值 | 差异% | 说明 |")
            lines.append("|------|-----------|-------|-------|------|")
            for r in diff_results:
                critical_mark = "🔴" if r.is_critical else "🟡"
                lines.append(f"| {critical_mark} {r.field_name} | {r.tushare_value or '—'} | {r.pdf_value or '—'} | {r.diff_pct:.1f}% | {r.message} |")
            lines.append("")
        
        if missing_results:
            lines.append("### 🔴 缺失重点信息")
            lines.append("")
            for r in missing_results:
                critical_mark = "🔴 **必须提取**" if r.is_critical else "🟡 建议提取"
                lines.append(f"- {critical_mark}: {r.field_name} - {r.message}")
            lines.append("")
        
        if self.missing_critical:
            lines.append("### 🔴 必须提取但未找到的信息")
            lines.append("")
            for item in self.missing_critical:
                lines.append(f"- [ ] {item}")
            lines.append("")
        
        if self.warnings:
            lines.append("### ⚠️ 警告")
            lines.append("")
            for w in self.warnings:
                lines.append(f"- {w}")
            lines.append("")
        
        if ok_results:
            lines.append(f"### ✅ 校验通过 ({len(ok_results)}项)")
            lines.append("")
        
        return "\n".join(lines)


CRITICAL_FIELDS = {
    "AUDIT": ["审计意见类型"],
    "BIZ_OVERVIEW": ["主营业务", "行业地位", "核心竞争力"],
    "BIZ": ["营业收入", "营业成本", "毛利率"],
    "CAP": ["产能", "产量", "销量", "产能利用率"],
    "CUS": ["前五大客户占比", "前五大供应商占比"],
    "INV": ["存货余额", "存货跌价准备"],
    "AR": ["应收账款余额", "坏账准备"],
    "DEBT": ["短期借款", "长期借款", "有息负债总额"],
    "CIP": ["在建工程", "固定资产"],
    "P4": ["关联交易金额"],
    "P13": ["非经常性损益"],
    "MDA_RISK": ["风险因素"],
}

TOLERANCE_THRESHOLDS = {
    "营业收入": 0.01,
    "营业成本": 0.01,
    "净利润": 0.01,
    "总资产": 0.01,
    "净资产": 0.01,
    "毛利率": 0.02,
    "存货余额": 0.02,
    "应收账款": 0.02,
    "短期借款": 0.02,
    "长期借款": 0.02,
    "default": 0.05,
}


class DataValidator:
    """Validate PDF extracted data against Tushare API data."""
    
    def __init__(
        self,
        tushare_store: Dict[str, Any],
        pdf_sections: Dict[str, Optional[str]],
        ts_code: str = "",
    ):
        self.tushare_store = tushare_store
        self.pdf_sections = pdf_sections
        self.ts_code = ts_code
        self.report = ValidationReport(ts_code=ts_code)
    
    def validate(self) -> ValidationReport:
        self._validate_audit()
        self._validate_business_overview()
        self._validate_financial_statements()
        self._validate_segments()
        self._validate_critical_extractions()
        self._check_missing_critical()
        return self.report
    
    def _extract_number_from_text(self, text: str, keywords: List[str]) -> Optional[float]:
        if not text:
            return None
        for kw in keywords:
            patterns = [
                rf"{kw}[：:]\s*([\d,\.]+)",
                rf"{kw}\s*([\d,\.]+)",
                rf"{kw}.*?([\d,\.]+)\s*(?:万元|亿元|百万)",
            ]
            for pat in patterns:
                match = re.search(pat, text)
                if match:
                    try:
                        num_str = match.group(1).replace(",", "")
                        return float(num_str)
                    except ValueError:
                        continue
        return None
    
    def _compare_values(
        self,
        field_name: str,
        tushare_val: Optional[float],
        pdf_val: Optional[float],
        section_id: str,
        unit_factor: float = 1.0,
    ) -> Optional[ValidationResult]:
        is_critical = any(
            field_name in fields
            for fields in CRITICAL_FIELDS.get(section_id, [])
        )
        
        if tushare_val is None and pdf_val is None:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                status="missing",
                message="Tushare和PDF均无数据",
                is_critical=is_critical,
            )
        
        if tushare_val is None:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                pdf_value=pdf_val,
                status="ok",
                message="仅PDF有数据，以PDF为准",
                is_critical=is_critical,
            )
        
        if pdf_val is None:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                tushare_value=tushare_val,
                status="missing",
                message="PDF未提取到数据，以Tushare为准",
                is_critical=is_critical,
            )
        
        tushare_val_adj = tushare_val * unit_factor
        
        if tushare_val_adj == 0 and pdf_val == 0:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                tushare_value=tushare_val,
                pdf_value=pdf_val,
                status="ok",
                is_critical=is_critical,
            )
        
        base = max(abs(tushare_val_adj), abs(pdf_val))
        if base == 0:
            diff_pct = 0.0
        else:
            diff_pct = abs(tushare_val_adj - pdf_val) / base * 100
        
        threshold = TOLERANCE_THRESHOLDS.get(field_name, TOLERANCE_THRESHOLDS["default"])
        threshold_pct = threshold * 100
        
        if diff_pct <= threshold_pct:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                tushare_value=tushare_val,
                pdf_value=pdf_val,
                status="ok",
                diff_pct=diff_pct,
                message=f"差异在容忍范围内(≤{threshold_pct:.1f}%)",
                is_critical=is_critical,
            )
        else:
            return ValidationResult(
                section_id=section_id,
                field_name=field_name,
                tushare_value=tushare_val,
                pdf_value=pdf_val,
                status="diff",
                diff_pct=diff_pct,
                message=f"⚠️ 差异超过阈值(>{threshold_pct:.1f}%)，优先使用Tushare数据",
                is_critical=is_critical,
            )
    
    def _validate_audit(self):
        audit_text = self.pdf_sections.get("AUDIT") or ""
        biz_overview_text = self.pdf_sections.get("BIZ_OVERVIEW") or ""
        combined_text = audit_text + "\n" + biz_overview_text
        
        if not combined_text.strip():
            self.report.missing_critical.append("审计报告 - 审计意见类型")
            return
        
        opinion_types = [
            ("标准无保留意见", "standard"),
            ("带强调事项段的无保留意见", "emphasis"),
            ("带持续经营重大不确定性段落", "emphasis"),
            ("保留意见", "qualified"),
            ("否定意见", "adverse"),
            ("无法表示意见", "disclaimer"),
        ]
        
        found_opinion = None
        for opinion_cn, opinion_en in opinion_types:
            if opinion_cn in combined_text:
                found_opinion = opinion_cn
                break
        
        if not found_opinion:
            if "无保留意见" in combined_text:
                found_opinion = "无保留意见"
            elif "审计意见" in combined_text:
                found_opinion = "审计意见（需人工确认）"
        
        if found_opinion:
            self.report.results.append(ValidationResult(
                section_id="AUDIT",
                field_name="审计意见类型",
                tushare_value=None,
                pdf_value=found_opinion,
                status="ok",
                message="PDF提取成功",
                is_critical=True,
            ))
            
            if "标准无保留意见" not in found_opinion and "无保留意见" not in found_opinion:
                self.report.warnings.append(
                    f"⚠️ 审计意见为【{found_opinion}】，非标准无保留意见，需重点关注"
                )
        else:
            self.report.missing_critical.append("审计报告 - 审计意见类型")
        
        if "关键审计事项" in combined_text:
            self.report.results.append(ValidationResult(
                section_id="AUDIT",
                field_name="关键审计事项",
                pdf_value="已提取",
                status="ok",
                message="关键审计事项已提取",
                is_critical=True,
            ))
        else:
            self.report.results.append(ValidationResult(
                section_id="AUDIT",
                field_name="关键审计事项",
                status="missing",
                message="未找到关键审计事项",
                is_critical=True,
            ))
    
    def _validate_business_overview(self):
        overview_text = self.pdf_sections.get("BIZ_OVERVIEW")
        if not overview_text:
            self.report.missing_critical.append("公司业务概要 - 主营业务描述")
            return
        
        critical_items = [
            ("主营业务", ["主营业务", "主要产品", "核心业务"]),
            ("经营模式", ["经营模式", "盈利模式", "商业模式"]),
            ("行业地位", ["行业地位", "市场地位", "市场份额", "领先"]),
            ("核心竞争力", ["核心竞争力", "竞争优势", "竞争壁垒"]),
        ]
        
        for item_name, keywords in critical_items:
            found = any(kw in overview_text for kw in keywords)
            is_critical = item_name in ["主营业务", "核心竞争力"]
            
            if found:
                self.report.results.append(ValidationResult(
                    section_id="BIZ_OVERVIEW",
                    field_name=item_name,
                    pdf_value="已提取",
                    status="ok",
                    message=f"{item_name}信息已提取",
                    is_critical=is_critical,
                ))
            else:
                self.report.results.append(ValidationResult(
                    section_id="BIZ_OVERVIEW",
                    field_name=item_name,
                    status="missing",
                    message=f"未找到{item_name}相关信息",
                    is_critical=is_critical,
                ))
                if is_critical:
                    self.report.missing_critical.append(f"公司业务概要 - {item_name}")
    
    def _validate_financial_statements(self):
        income_df = self.tushare_store.get("income")
        biz_text = self.pdf_sections.get("BIZ")
        
        if income_df is not None and not income_df.empty and biz_text:
            latest = income_df.iloc[0]
            
            revenue = self._safe_float(latest.get("revenue"))
            pdf_revenue = self._extract_number_from_text(biz_text, ["营业收入", "营收"])
            if revenue or pdf_revenue:
                result = self._compare_values("营业收入", revenue, pdf_revenue, "BIZ", unit_factor=0.0001)
                if result:
                    self.report.results.append(result)
            
            cost = self._safe_float(latest.get("oper_cost"))
            pdf_cost = self._extract_number_from_text(biz_text, ["营业成本", "成本"])
            if cost or pdf_cost:
                result = self._compare_values("营业成本", cost, pdf_cost, "BIZ", unit_factor=0.0001)
                if result:
                    self.report.results.append(result)
        
        balance_df = self.tushare_store.get("balance")
        inv_text = self.pdf_sections.get("INV")
        
        if balance_df is not None and not balance_df.empty and inv_text:
            latest = balance_df.iloc[0]
            
            inventory = self._safe_float(latest.get("inventory"))
            pdf_inventory = self._extract_number_from_text(inv_text, ["存货余额", "存货账面"])
            if inventory or pdf_inventory:
                result = self._compare_values("存货余额", inventory, pdf_inventory, "INV", unit_factor=0.0001)
                if result:
                    self.report.results.append(result)
        
        ar_text = self.pdf_sections.get("AR")
        if balance_df is not None and not balance_df.empty and ar_text:
            latest = balance_df.iloc[0]
            
            ar = self._safe_float(latest.get("accounts_receiv"))
            pdf_ar = self._extract_number_from_text(ar_text, ["应收账款余额", "应收账款账面"])
            if ar or pdf_ar:
                result = self._compare_values("应收账款", ar, pdf_ar, "AR", unit_factor=0.0001)
                if result:
                    self.report.results.append(result)
    
    def _validate_segments(self):
        segments_df = self.tushare_store.get("segments")
        biz_text = self.pdf_sections.get("BIZ")
        
        if segments_df is not None and not segments_df.empty:
            self.report.results.append(ValidationResult(
                section_id="BIZ",
                field_name="主营业务构成",
                tushare_value="已获取",
                pdf_value="已提取" if biz_text else None,
                status="ok",
                message="Tushare已提供分部数据",
                is_critical=True,
            ))
        elif biz_text:
            self.report.results.append(ValidationResult(
                section_id="BIZ",
                field_name="主营业务构成",
                pdf_value="已提取",
                status="ok",
                message="仅PDF有数据",
                is_critical=True,
            ))
        else:
            self.report.missing_critical.append("主营业务构成 - 分产品/分地区营收")
    
    def _validate_critical_extractions(self):
        critical_sections = {
            "CAP": ["产能", "产量", "销量"],
            "CUS": ["前五大客户", "前五大供应商"],
            "DEBT": ["短期借款", "长期借款"],
            "CIP": ["在建工程"],
            "P4": ["关联交易"],
            "P13": ["非经常性损益"],
            "MDA_RISK": ["风险因素"],
        }
        
        for section_id, field_names in critical_sections.items():
            section_text = self.pdf_sections.get(section_id)
            
            for field_name in field_names:
                is_critical = field_name in CRITICAL_FIELDS.get(section_id, [])
                
                if section_text and any(kw in section_text for kw in [field_name, field_name.replace("前五大", "前五名")]):
                    self.report.results.append(ValidationResult(
                        section_id=section_id,
                        field_name=field_name,
                        pdf_value="已提取",
                        status="ok",
                        message=f"{field_name}信息已提取",
                        is_critical=is_critical,
                    ))
                else:
                    self.report.results.append(ValidationResult(
                        section_id=section_id,
                        field_name=field_name,
                        status="missing",
                        message=f"未找到{field_name}相关信息",
                        is_critical=is_critical,
                    ))
                    if is_critical:
                        self.report.missing_critical.append(f"{section_id} - {field_name}")
    
    def _check_missing_critical(self):
        required_sections = [
            "AUDIT", "BIZ_OVERVIEW", "BIZ", "CAP", "CUS", 
            "INV", "AR", "DEBT", "CIP", "P4", "P13", "MDA_RISK"
        ]
        
        for section_id in required_sections:
            text = self.pdf_sections.get(section_id)
            if not text or len(text.strip()) < 100:
                section_names = {
                    "AUDIT": "审计报告",
                    "BIZ_OVERVIEW": "公司业务概要",
                    "BIZ": "主营业务分拆",
                    "CAP": "产能产量",
                    "CUS": "客户供应商集中度",
                    "INV": "存货明细",
                    "AR": "应收账款账龄",
                    "DEBT": "有息负债",
                    "CIP": "在建工程",
                    "P4": "关联方交易",
                    "P13": "非经常性损益",
                    "MDA_RISK": "风险因素",
                }
                section_name = section_names.get(section_id, section_id)
                if f"{section_id} -" not in str(self.report.missing_critical):
                    self.report.warnings.append(f"⚠️ {section_name}提取内容不足，可能需要人工补充")
    
    def _safe_float(self, val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None


def validate_and_annotate(
    tushare_store: Dict[str, Any],
    pdf_sections: Dict[str, Optional[str]],
    ts_code: str = "",
) -> Tuple[str, List[str]]:
    """Validate data and return markdown report + missing items list.
    
    Args:
        tushare_store: Data store from TushareCollector
        pdf_sections: Extracted sections from PDF
        ts_code: Stock code for reference
    
    Returns:
        Tuple of (markdown_report, missing_critical_list)
    """
    validator = DataValidator(tushare_store, pdf_sections, ts_code)
    report = validator.validate()
    return report.to_markdown(), report.missing_critical
