#!/usr/bin/env python3
"""Turtle Investment Framework - PDF Preprocessor Configuration.

配置加载策略：
1. 优先从 analysis_modules.yaml（统一配置文件）读取
2. 如果读取失败，使用内置默认配置

这样确保配置的单一真相来源，避免双配置源不一致问题。
"""

import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Encoding constants
DEFAULT_ENCODING = 'utf-8'

# Garbled text detection thresholds
GARBLED_THRESHOLD = 0.30
GARBLED_PAGE_RATIO = 0.30

# Extraction defaults
DEFAULT_BUFFER_PAGES = 1
DEFAULT_MAX_CHARS = 4000

def _load_yaml_config() -> Optional[Dict]:
    """从 analysis_modules.yaml 加载配置"""
    config_path = Path(__file__).parent.parent / "config" / "analysis_modules.yaml"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"⚠️ 无法加载 analysis_modules.yaml: {e}")
        return None

def _build_section_keywords(yaml_config: Optional[Dict]) -> Dict[str, Dict[str, List[str]]]:
    """从 YAML 配置构建 SECTION_KEYWORDS"""
    if not yaml_config:
        return _get_default_section_keywords()
    
    keywords = {}
    pdf_sections = yaml_config.get("pdf_sections", {})
    
    for section_id, section_data in pdf_sections.items():
        keywords[section_id] = {
            "include": section_data.get("keywords", {}).get("include", []),
            "exclude": section_data.get("keywords", {}).get("exclude", []),
            "mandatory": section_data.get("keywords", {}).get("mandatory", [])
        }
    
    return keywords

def _build_section_extract_config(yaml_config: Optional[Dict]) -> Dict[str, Dict[str, int]]:
    """从 YAML 配置构建 SECTION_EXTRACT_CONFIG"""
    if not yaml_config:
        return _get_default_extract_config()
    
    config = {}
    pdf_sections = yaml_config.get("pdf_sections", {})
    
    for section_id, section_data in pdf_sections.items():
        extract_cfg = section_data.get("extract_config", {})
        config[section_id] = {
            "buffer_pages": extract_cfg.get("buffer_pages", DEFAULT_BUFFER_PAGES),
            "max_chars": extract_cfg.get("max_chars", DEFAULT_MAX_CHARS)
        }
    
    return config

def _build_section_order(yaml_config: Optional[Dict]) -> List[str]:
    """从 YAML 配置构建 SECTION_ORDER"""
    if not yaml_config:
        return _get_default_section_order()
    
    return list(yaml_config.get("pdf_sections", {}).keys())

def _build_toc_hints(yaml_config: Optional[Dict]) -> Dict[str, List[str]]:
    """从 YAML 配置构建 TOC_HINTS"""
    if not yaml_config:
        return _get_default_toc_hints()
    
    hints = {}
    pdf_sections = yaml_config.get("pdf_sections", {})
    
    for section_id, section_data in pdf_sections.items():
        location = section_data.get("location", "")
        if location:
            hints[section_id] = [location]
    
    return hints

# 加载 YAML 配置
_YAML_CONFIG = _load_yaml_config()

# 动态生成配置（单一真相来源：analysis_modules.yaml）
SECTION_KEYWORDS: Dict[str, Dict[str, List[str]]] = _build_section_keywords(_YAML_CONFIG)
SECTION_EXTRACT_CONFIG: Dict[str, Dict[str, int]] = _build_section_extract_config(_YAML_CONFIG)
SECTION_ORDER: List[str] = _build_section_order(_YAML_CONFIG)
EXTRACT_PRIORITY: List[str] = SECTION_ORDER  # 与 SECTION_ORDER 保持一致
TOC_HINTS: Dict[str, List[str]] = _build_toc_hints(_YAML_CONFIG)

# Zone markers for section identification
ZONE_MARKERS: List[Tuple[str, str]] = [
    (r"第[一二三四五六七八九十百]+节\s*重要提示", "INTRO_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*公司简介", "INTRO_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*管理层讨论与分析", "MDA_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*经营情况讨论与分析", "MDA_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*公司治理", "GOVERNANCE_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*财务报告", "FIN_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*会计数据", "FIN_ZONE"),
    (r"[四五六]\s*[、.．]\s*重要会计政策", "POLICY_ZONE"),
    (r"七\s*[、.．]\s*合并财务报表项目注释", "NOTES_ZONE"),
    (r"[一二三四五六七八九十]+[、.．]\s*补充资料", "SUPPLEMENT_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\ss]*重要提示", "INTRO_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*公司简介", "INTRO_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*管理层讨论与分析", "MDA_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*经营情况讨论与分析", "MDA_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*财务报告", "FIN_ZONE"),
]

# Section title binding for precise extraction
SECTION_TITLE_BINDING: Dict[str, List[str]] = {
    "MDA_INDUSTRY": ["行业发展概况", "行业分析", "行业格局", "市场环境"],
    "MDA_OPERATION": ["主营业务分析", "经营情况讨论与分析", "经营成果分析"],
    "MDA_OUTLOOK": ["未来发展展望", "经营计划", "发展战略", "未来规划"],
    "MDA_RISK": ["风险因素", "风险提示", "可能面对的风险"],
}

# Section zone preferences
SECTION_ZONE_PREFERENCES: Dict[str, Dict[str, List[str]]] = {
    "AUDIT": {"prefer": ["FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "AUDIT_OPINION": {"prefer": ["FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "ACCOUNTING_POLICY": {"prefer": ["POLICY_ZONE", "FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "P1": {"prefer": ["FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "P2": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P3": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P4": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P5": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P6": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P7": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P8": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P9": {"prefer": ["POLICY_ZONE", "FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "P13": {"prefer": ["SUPPLEMENT_ZONE", "NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "BIZ_OVERVIEW": {"prefer": ["MDA_ZONE", "INTRO_ZONE"], "avoid": ["NOTES_ZONE"]},
    "GLOSSARY": {"prefer": ["INTRO_ZONE"], "avoid": ["NOTES_ZONE", "FIN_ZONE"]},
    "IMPORTANT": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": []},
    "BIZ": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CAP": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CUS": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "INV": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "AR":  {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "DEBT": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CIP": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "MDA_INDUSTRY": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_OPERATION": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_OUTLOOK": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_RISK": {"prefer": ["MDA_ZONE"], "avoid": []},
    "SUB": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "INDUSTRY_EXTRA": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": []},
}

# AR section exclusion terms
AR_EXCLUDE_TERMS = ["递延所得税", "预付款项", "预付账款", "其他应收"]

# Directory names for output
OUTPUT_DIR_NAME = "pdf_sections"
JSON_FILENAME = "pdf_sections.json"
MD_FILENAME = "pdf_sections.md"


# ============================================================
# 默认配置（仅在 YAML 配置加载失败时使用）
# ============================================================

def _get_default_section_keywords() -> Dict[str, Dict[str, List[str]]]:
    """默认 SECTION_KEYWORDS（备用）"""
    return {
        "AUDIT": {
            "include": ["审计报告", "审计意见", "标准无保留意见", "关键审计事项"],
            "exclude": ["主营业务", "经营情况", "行业分析"],
            "mandatory": ["审计意见", "关键审计事项", "会计师事务所"]
        },
        "BIZ": {
            "include": ["营业收入分产品", "主营业务分产品", "营业成本构成", "分产品毛利率"],
            "exclude": ["产能", "产量", "销量", "在建工程"],
            "mandatory": ["分产品营收", "分地区营收", "营业成本", "毛利率"]
        },
    }

def _get_default_extract_config() -> Dict[str, Dict[str, int]]:
    """默认 SECTION_EXTRACT_CONFIG（备用）"""
    return {
        "AUDIT": {"buffer_pages": 3, "max_chars": 8000},
        "BIZ": {"buffer_pages": 3, "max_chars": 8000},
    }

def _get_default_section_order() -> List[str]:
    """默认 SECTION_ORDER（备用）"""
    return ["AUDIT", "AUDIT_OPINION", "ACCOUNTING_POLICY", "BIZ"]

def _get_default_toc_hints() -> Dict[str, List[str]]:
    """默认 TOC_HINTS（备用）"""
    return {
        "AUDIT": ["审计报告"],
        "BIZ": ["经营情况讨论与分析", "主营业务分析"],
    }


# ============================================================
# 配置验证函数
# ============================================================

def validate_config_loaded() -> bool:
    """验证配置是否成功从 YAML 加载"""
    if _YAML_CONFIG is None:
        print("⚠️ 配置加载自默认值，请检查 analysis_modules.yaml")
        return False
    
    print(f"✅ 配置已从 analysis_modules.yaml 加载")
    print(f"   - PDF 章节: {len(SECTION_KEYWORDS)} 个")
    print(f"   - 提取配置: {len(SECTION_EXTRACT_CONFIG)} 个")
    return True


if __name__ == "__main__":
    # 测试配置加载
    print("=" * 60)
    print("PDF 配置加载测试")
    print("=" * 60)
    
    validate_config_loaded()
    
    print("\n章节列表:")
    for i, section in enumerate(SECTION_ORDER, 1):
        print(f"  {i:2d}. {section}")
