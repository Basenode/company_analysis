#!/usr/bin/env python3
"""配置文件自动生成脚本

本脚本从 config/analysis_modules.yaml 读取统一配置
自动生成以下文件：
1. scripts/pdf_config.py - PDF提取配置

运行方式：
    python scripts/generate_config.py              # 生成所有文件
    python scripts/generate_config.py --check     # 检查并提示差异
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class ConfigGenerator:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.config_path = base_path / "config" / "analysis_modules.yaml"
        self.pdf_config_path = base_path / "scripts" / "pdf_config.py"
        self.config: Dict = {}
    
    def load_config(self) -> bool:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
            print(f"✅ 已加载统一配置文件: {self.config_path}")
            return True
        except Exception as e:
            print(f"❌ 加载配置文件失败: {e}")
            return False
    
    def generate_pdf_config(self) -> bool:
        """生成 pdf_config.py 文件"""
        pdf_sections = self.config.get("pdf_sections", {})
        
        lines = []
        lines.append('#!/usr/bin/env python3')
        lines.append('"""PDF提取配置文件')
        lines.append('')
        lines.append('本文件由 scripts/generate_config.py 自动生成')
        lines.append(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        lines.append('请勿手动修改，修改 config/analysis_modules.yaml 后重新运行生成脚本')
        lines.append('"""')
        lines.append('')
        lines.append('from typing import Dict, List, Tuple')
        lines.append('')
        lines.append('# Encoding constants')
        lines.append("DEFAULT_ENCODING = 'utf-8'")
        lines.append('')
        lines.append('# Garbled text detection thresholds')
        lines.append('GARBLED_THRESHOLD = 0.30')
        lines.append('GARBLED_PAGE_RATIO = 0.30')
        lines.append('')
        lines.append('# Extraction defaults')
        lines.append('DEFAULT_BUFFER_PAGES = 1')
        lines.append('DEFAULT_MAX_CHARS = 4000')
        lines.append('')
        lines.append('# Section keywords for extraction')
        lines.append('SECTION_KEYWORDS: Dict[str, Dict[str, List[str]]] = {')
        
        for section_id in sorted(pdf_sections.keys()):
            section = pdf_sections[section_id]
            keywords = section.get("keywords", {})
            include_list = keywords.get("include", [])
            exclude_list = keywords.get("exclude", [])
            mandatory_list = keywords.get("mandatory", [])
            lines.append(f'    "{section_id}": {{')
            lines.append(f'        "include": {repr(include_list)},')
            lines.append(f'        "exclude": {repr(exclude_list)},')
            lines.append(f'        "mandatory": {repr(mandatory_list)}')
            lines.append('    },')
        
        lines.append('}')
        lines.append('')
        lines.append('# Section extraction configuration')
        lines.append('SECTION_EXTRACT_CONFIG: Dict[str, Dict[str, int]] = {')
        
        for section_id in sorted(pdf_sections.keys()):
            section = pdf_sections[section_id]
            extract_config = section.get("extract_config", {})
            buffer_pages = extract_config.get("buffer_pages", 1)
            max_chars = extract_config.get("max_chars", 4000)
            lines.append(f'    "{section_id}": {{"buffer_pages": {buffer_pages}, "max_chars": {max_chars}}},')
        
        lines.append('}')
        lines.append('')
        lines.append('# Section zone preferences')
        lines.append('SECTION_ZONE_PREFERENCES: Dict[str, Dict[str, List[str]]] = {')
        
        zone_prefs = self.config.get("zone_preferences", {})
        for section_id in sorted(pdf_sections.keys()):
            prefs = zone_prefs.get(section_id, {"prefer": [], "avoid": []})
            prefer_list = prefs.get("prefer", [])
            avoid_list = prefs.get("avoid", [])
            lines.append(f'    "{section_id}": {{"prefer": {repr(prefer_list)}, "avoid": {repr(avoid_list)}}},')
        
        lines.append('}')
        lines.append('')
        lines.append('# Section order for output')
        lines.append('SECTION_ORDER = [')
        
        section_order = self.config.get("section_order", list(pdf_sections.keys()))
        for section_id in section_order:
            lines.append(f'    "{section_id}",')
        
        lines.append(']')
        lines.append('')
        lines.append('# Module extraction priority')
        lines.append('EXTRACT_PRIORITY = [')
        
        extract_priority = self.config.get("extract_priority", section_order)
        for section_id in extract_priority:
            lines.append(f'    "{section_id}",')
        
        lines.append(']')
        lines.append('')
        lines.append('# TOC hints mapping')
        lines.append('TOC_HINTS = {')
        
        toc_hints = self.config.get("toc_hints", {})
        for section_id in sorted(toc_hints.keys()):
            hints = toc_hints.get(section_id, [])
            lines.append(f'    "{section_id}": {repr(hints)},')
        
        lines.append('}')
        lines.append('')
        lines.append('# AR section exclusion terms')
        lines.append('AR_EXCLUDE_TERMS = ["递延所得税", "预付款项", "预付账款", "其他应收"]')
        lines.append('')
        lines.append('# Directory names for output')
        lines.append('OUTPUT_DIR_NAME = "pdf_sections"')
        lines.append('JSON_FILENAME = "pdf_sections.json"')
        lines.append('MD_FILENAME = "pdf_sections.md"')
        
        content = '\n'.join(lines)
        
        with open(self.pdf_config_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        print(f"✅ 已生成: {self.pdf_config_path}")
        return True
    
    def generate_all(self) -> bool:
        """生成所有配置文件"""
        print("\n" + "=" * 60)
        print("配置文件自动生成")
        print("=" * 60)
        
        success = True
        success = success and self.generate_pdf_config()
        
        if success:
            print("\n🎉 所有配置文件生成成功！")
            print("\n下一步：")
            print("1. 运行 python scripts/validate_config.py 校验一致性")
            print("2. 继续执行分析流程")
        else:
            print("\n❌ 部分文件生成失败")
        
        return success


    def check_drift(self) -> Dict[str, List[str]]:
        """检查配置漂移（统一配置与实际文件的差异)"""
        drift_report = {}
        
        pdf_sections = self.config.get("pdf_sections", {})
        
        with open(self.pdf_config_path, "r", encoding="utf-8") as f:
            actual_content = f.read()
        
        for section_id in pdf_sections.keys():
            if f'"{section_id}"' not in actual_content:
                if section_id not in drift_report:
                    drift_report[section_id] = []
                drift_report[section_id].append("缺失")
        
        return drift_report


    
    def print_drift_report(self, drift_report: Dict[str, List[str]]):
        """打印漂移报告"""
        if not drift_report:
            print("✅ 未发现配置漂移")
        else:
            print("⚠️ 发现配置漂移:")
            for section_id, issues in drift_report.items():
                print(f"  {section_id}: {', '.join(issues)}")

def main():
    base_path = Path(__file__).parent.parent
    
    generator = ConfigGenerator(base_path)
    
    if not generator.load_config():
        sys.exit(1)
    
    import argparse
    parser = argparse.ArgumentParser(description="配置文件自动生成脚本")
    parser.add_argument("--pdf-only", action="store_true", help="只生成pdf_config.py")
    parser.add_argument("--check", action="store_true", help="检查配置漂移")
    args = parser.parse_args()
    
    if args.check:
        drift_report = generator.check_drift()
        generator.print_drift_report(drift_report)
    elif args.pdf_only:
        generator.generate_pdf_config()
    else:
        generator.generate_all()


if __name__ == "__main__":
    main()
