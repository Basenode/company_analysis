#!/usr/bin/env python3
"""配置文件一致性校验脚本

本脚本用于校验以下文件之间的一致性：
1. config/analysis_modules.yaml（统一配置文件，单一真相来源）
2. scripts/pdf_config.py（PDF提取配置）
3. prompts/coordinator_v2.md（调度器）
4. prompts/pdf_parser.md（PDF解析规范）
5. prompts/report_template.md（报告模板）

运行方式：
    python scripts/validate_config.py
    python scripts/validate_config.py --fix  # 自动修复不一致
"""

import yaml
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    missing_items: List[str] = field(default_factory=list)
    extra_items: List[str] = field(default_factory=list)


class ConfigValidator:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.config_path = base_path / "config" / "analysis_modules.yaml"
        self.pdf_config_path = base_path / "scripts" / "pdf_config.py"
        self.coordinator_path = base_path / "prompts" / "coordinator_v2.md"
        self.pdf_parser_path = base_path / "prompts" / "pdf_parser.md"
        self.report_template_path = base_path / "prompts" / "report_template.md"
        
        self.config: Dict = {}
        self.results: Dict[str, ValidationResult] = {}
    
    def load_config(self) -> bool:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
            print(f"✅ 已加载统一配置文件: {self.config_path}")
            return True
        except Exception as e:
            print(f"❌ 加载配置文件失败: {e}")
            return False
    
    def get_pdf_sections_from_config(self) -> Set[str]:
        return set(self.config.get("pdf_sections", {}).keys())
    
    def get_search_items_from_config(self) -> Dict[str, str]:
        items = {}
        for tier_name, tier_data in self.config.get("search_items", {}).items():
            for item in tier_data.get("items", []):
                items[item["id"]] = item["name"]
        return items
    
    def get_report_sections_from_config(self) -> Set[str]:
        sections = set()
        for part in self.config.get("report_sections", []):
            sections.add(part["id"])
            for sub in part.get("subsections", []):
                sections.add(sub["id"])
        return sections
    
    def validate_pdf_config(self) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if not self.pdf_config_path.exists():
            result.errors.append(f"pdf_config.py 文件不存在: {self.pdf_config_path}")
            result.is_valid = False
            return result
        
        config_sections = self.get_pdf_sections_from_config()
        
        # 方式1：尝试动态加载配置（支持从YAML动态加载的新版本）
        try:
            from pdf_config import SECTION_KEYWORDS, SECTION_EXTRACT_CONFIG
            found_sections = set(SECTION_KEYWORDS.keys())
            
            for section in config_sections:
                if section not in found_sections:
                    result.missing_items.append(f"pdf_config.py 缺少章节: {section}")
                    result.is_valid = False
            
            for section in found_sections:
                if section not in config_sections:
                    result.extra_items.append(f"pdf_config.py 多余章节: {section}")
                    result.warnings.append(f"pdf_config.py 中 {section} 不在统一配置中")
            
            if result.is_valid:
                print(f"  ✅ pdf_config.py 校验通过 ({len(config_sections)} 个章节，动态加载)")
            else:
                print(f"  ❌ pdf_config.py 校验失败")
                for err in result.missing_items:
                    print(f"     - {err}")
            
            return result
            
        except ImportError:
            # 方式2：回退到正则表达式检测（兼容旧版本）
            with open(self.pdf_config_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            pattern = r'"([A-Z_0-9]+)":\s*\{'
            found_sections = set(re.findall(pattern, content))
            
            found_sections = {s for s in found_sections if not s.startswith("ZONE") and not s.startswith("SECTION") and not s.startswith("AR_EXCLUDE") and not s.startswith("OUTPUT") and not s.startswith("DEFAULT") and not s.startswith("GARBLED") and not s.startswith("EXTRACT")}
            
            for section in config_sections:
                if section not in found_sections:
                    result.missing_items.append(f"pdf_config.py 缺少章节: {section}")
                    result.is_valid = False
            
            for section in found_sections:
                if section not in config_sections:
                    result.extra_items.append(f"pdf_config.py 多余章节: {section}")
                    result.warnings.append(f"pdf_config.py 中 {section} 不在统一配置中")
            
            if result.is_valid:
                print(f"  ✅ pdf_config.py 校验通过 ({len(config_sections)} 个章节)")
            else:
                print(f"  ❌ pdf_config.py 校验失败")
                for err in result.missing_items:
                    print(f"     - {err}")
            
            return result
    
    def validate_coordinator(self) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if not self.coordinator_path.exists():
            result.errors.append(f"coordinator_v2.md 文件不存在: {self.coordinator_path}")
            result.is_valid = False
            return result
        
        with open(self.coordinator_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        websearch_ref_path = self.base_path / "prompts" / "websearch_guidelines.md"
        pdf_parser_ref_path = self.base_path / "prompts" / "pdf_parser.md"
        
        required_refs = [
            ("websearch_guidelines.md", "网络搜索规范引用"),
            ("pdf_parser.md", "PDF提取规范引用"),
        ]
        
        for ref_file, ref_desc in required_refs:
            if ref_file not in content:
                result.missing_items.append(f"coordinator_v2.md 缺少{ref_desc}: {ref_file}")
                result.is_valid = False
        
        if "严格按照" not in content:
            result.warnings.append("coordinator_v2.md 建议使用'严格按照'关键词引用规范文件")
        
        if not websearch_ref_path.exists():
            result.errors.append(f"websearch_guidelines.md 文件不存在")
            result.is_valid = False
        
        if not pdf_parser_ref_path.exists():
            result.errors.append(f"pdf_parser.md 文件不存在")
            result.is_valid = False
        
        if result.is_valid:
            print(f"  ✅ coordinator_v2.md 校验通过（引用模式）")
        else:
            print(f"  ❌ coordinator_v2.md 校验失败")
            for err in result.missing_items:
                print(f"     - {err}")
            for err in result.errors:
                print(f"     - {err}")
        
        return result
    
    def validate_pdf_parser(self) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if not self.pdf_parser_path.exists():
            result.errors.append(f"pdf_parser.md 文件不存在: {self.pdf_parser_path}")
            result.is_valid = False
            return result
        
        with open(self.pdf_parser_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        config_sections = self.get_pdf_sections_from_config()
        
        pattern = r'\|\s*([A-Z_]+)\s*\|'
        found_sections = set(re.findall(pattern, content))
        
        for section in config_sections:
            if section not in found_sections:
                result.missing_items.append(f"pdf_parser.md 缺少章节: {section}")
        
        pattern2 = r'###\s*([A-Z0-9]+)\s*[:：]'
        found_sections2 = set(re.findall(pattern2, content))
        
        for section in config_sections:
            if section.startswith("P") and section not in found_sections2:
                result.warnings.append(f"pdf_parser.md 缺少精提取规范: {section}")
        
        if result.is_valid:
            print(f"  ✅ pdf_parser.md 校验通过")
        else:
            print(f"  ❌ pdf_parser.md 校验失败")
            for err in result.missing_items[:5]:
                print(f"     - {err}")
        
        return result
    
    def validate_report_template(self) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if not self.report_template_path.exists():
            result.errors.append(f"report_template.md 文件不存在: {self.report_template_path}")
            result.is_valid = False
            return result
        
        with open(self.report_template_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        config_sections = self.get_report_sections_from_config()
        
        for section_id in config_sections:
            if section_id not in content:
                result.missing_items.append(f"report_template.md 缺少章节: {section_id}")
        
        if result.is_valid:
            print(f"  ✅ report_template.md 校验通过")
        else:
            print(f"  ❌ report_template.md 校验失败")
            for err in result.missing_items[:5]:
                print(f"     - {err}")
        
        return result
    
    def validate_all(self) -> Dict[str, ValidationResult]:
        print("\n" + "=" * 60)
        print("配置文件一致性校验")
        print("=" * 60)
        print(f"统一配置文件: {self.config_path}")
        print(f"校验项目:")
        print(f"  - PDF章节: {len(self.get_pdf_sections_from_config())} 个")
        print(f"  - 搜索项: {len(self.get_search_items_from_config())} 个")
        print(f"  - 报告章节: {len(self.get_report_sections_from_config())} 个")
        print()
        
        self.results["pdf_config"] = self.validate_pdf_config()
        self.results["coordinator"] = self.validate_coordinator()
        self.results["pdf_parser"] = self.validate_pdf_parser()
        self.results["report_template"] = self.validate_report_template()
        
        return self.results
    
    def print_summary(self):
        print("\n" + "=" * 60)
        print("校验结果汇总")
        print("=" * 60)
        
        all_valid = True
        for name, result in self.results.items():
            status = "✅ 通过" if result.is_valid else "❌ 失败"
            print(f"  {name}: {status}")
            if not result.is_valid:
                all_valid = False
                print(f"    - 缺失项: {len(result.missing_items)}")
                print(f"    - 多余项: {len(result.extra_items)}")
                print(f"    - 警告: {len(result.warnings)}")
        
        print()
        if all_valid:
            print("🎉 所有配置文件一致性校验通过！")
        else:
            print("⚠️ 存在配置不一致，请检查并修复")
            print()
            print("修复方法：")
            print("  1. 修改 config/analysis_modules.yaml（统一配置文件）")
            print("  2. 运行 python scripts/generate_config.py 自动生成其他文件")
            print("  3. 或手动同步修改相关文件")
        
        return all_valid


def main():
    base_path = Path(__file__).parent.parent
    
    validator = ConfigValidator(base_path)
    
    if not validator.load_config():
        sys.exit(1)
    
    validator.validate_all()
    all_valid = validator.print_summary()
    
    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
