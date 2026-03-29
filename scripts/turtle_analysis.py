#!/usr/bin/env python3
"""Turtle Investment Framework - Data Collection Pipeline.

Core capabilities:
  - Tushare API data collection and storage
  - Local PDF file management
  - PDF preprocessing

Usage:
    python scripts/turtle_analysis.py --code 600887
    python scripts/turtle_analysis.py --code 600887.SH --pdf path/to/report.pdf
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import tushare as ts

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from config import get_token, get_api_url, validate_stock_code, check_local_pdf

REPORT_PERIODS = {
    "年报": "年报",
    "半年报": "半年报", 
    "一季报": "一季报",
    "三季度报": "三季度报",
    "一季度报": "一季报",
    "季报": "季报",
}

PERIOD_ORDER = ["年报", "半年报", "一季报", "三季度报"]


def parse_report_period(period_str: str) -> str:
    """Normalize report period string.
    
    Args:
        period_str: Raw period string (e.g., '年报', '一季度报', '一季报')
    
    Returns:
        Normalized period name (e.g., '年报', '一季报')
    """
    period_str = period_str.strip()
    for key, value in REPORT_PERIODS.items():
        if key in period_str:
            return value
    return "年报"


def infer_report_period_from_filename(filename: str) -> tuple[int, str]:
    """Extract year and period from PDF filename.
    
    Args:
        filename: PDF filename (e.g., '云天化2025年年度报告.pdf')
    
    Returns:
        (year, period) tuple, e.g., (2025, '年报')
    """
    year_match = re.search(r'20\d{2}', filename)
    year = int(year_match.group()) if year_match else datetime.now().year - 1
    
    if "年度报告" in filename or "年报" in filename:
        period = "年报"
    elif "半年度" in filename or "半年报" in filename:
        period = "半年报"
    elif "第一季度" in filename or "一季报" in filename or "一季度" in filename:
        period = "一季报"
    elif "第三季度" in filename or "三季度" in filename or "三季度报" in filename:
        period = "三季度报"
    else:
        period = "年报"
    
    return year, period


def get_company_name(ts_code: str) -> str:
    """Fetch company name from Tushare stock_basic API.
    
    Args:
        ts_code: Stock code (e.g., '600096.SH', '00700.HK')
    
    Returns:
        Company name string, or empty string if not found
    """
    try:
        token = get_token()
        api_url = get_api_url()
        ts.set_token(token)
        pro = ts.pro_api(timeout=30)
        if api_url:
            pro._DataApi__token = token
            pro._DataApi__http_url = api_url
        
        if ts_code.endswith('.HK'):
            df = pro.hk_basic(ts_code=ts_code, fields='ts_code,name')
        else:
            df = pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
        
        if not df.empty and 'name' in df.columns:
            return str(df.iloc[0]['name'])
    except Exception as e:
        print(f"  [WARN] 获取公司名称失败: {e}")
    
    return ""


def copy_local_pdf(local_path: str, output_dir: Path, ts_code: str, 
                   company_name: str = None, year: int = None, 
                   period: str = "年报") -> tuple[bool, str, int, str]:
    """Copy local PDF file to output directory with standardized naming.
    
    Naming convention: {代码}_{年份}_{公司名}_{报告期}.pdf
    Example: 600989_2024_宝丰能源_年报.pdf
    
    Args:
        local_path: Path to local PDF file
        output_dir: Output directory to copy to
        ts_code: Stock code for naming
        company_name: Company name (optional)
        year: Year for naming (default: inferred from filename)
        period: Report period (default: inferred from filename)
    
    Returns:
        (success, new_path or error_message, year, period)
    """
    filename = os.path.basename(local_path)
    
    if year is None or period == "年报":
        inferred_year, inferred_period = infer_report_period_from_filename(filename)
        if year is None:
            year = inferred_year
        if period == "年报":
            period = inferred_period
    
    code = ts_code.split('.')[0]
    
    if not os.path.exists(local_path):
        return False, f"本地 PDF 文件不存在: {local_path}", year, period
    
    if company_name:
        pdf_filename = f"{code}_{year}_{company_name}_{period}.pdf"
    else:
        pdf_filename = f"{code}_{year}_{period}.pdf"
    
    dest_path = output_dir / pdf_filename
    
    try:
        shutil.copy2(local_path, str(dest_path))
        filesize = os.path.getsize(dest_path)
        print(f"  PDF 已保存: {pdf_filename} ({filesize:,} bytes)")
        return True, str(dest_path), year, period
    except Exception as e:
        return False, f"复制 PDF 文件失败: {e}", year, period


def run_phase1a(ts_code: str, output_file: Path, pdf_json_path: Path = None) -> tuple[bool, str]:
    """Run Tushare data collection (Phase 1A).
    
    Args:
        ts_code: Stock code
        output_file: Output markdown file path
        pdf_json_path: Optional path to pdf_sections.json for validation
    """
    
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "tushare_collector.py"),
        "--code", ts_code,
        "--output", str(output_file),
    ]
    
    if pdf_json_path and pdf_json_path.exists():
        cmd.extend(["--pdf-json", str(pdf_json_path)])
    
    print(f"\n{'='*60}")
    print(f"[Phase 1A] Tushare 数据采集")
    print(f"{'='*60}")
    print(f"股票代码: {ts_code}")
    print(f"输出文件: {output_file}")
    if pdf_json_path:
        print(f"PDF 验证: {pdf_json_path}")
    print()
    
    try:
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode == 0:
            return True, str(output_file)
        else:
            return False, f"tushare_collector.py exited with code {result.returncode}"
    except Exception as e:
        return False, str(e)


def run_phase2a(pdf_path: str, output_dir: Path) -> tuple[bool, str]:
    """Run PDF preprocessing (Phase 2A)."""
    output_file = output_dir / "pdf_sections.json"
    
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "pdf_preprocessor.py"),
        "--pdf", pdf_path,
        "--output", str(output_file),
    ]
    
    print(f"\n{'='*60}")
    print(f"[Phase 2A] PDF 预处理")
    print(f"{'='*60}")
    print(f"PDF 文件: {pdf_path}")
    print(f"输出文件: {output_file}")
    print()
    
    try:
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode == 0:
            return True, str(output_file)
        else:
            return False, f"pdf_preprocessor.py exited with code {result.returncode}"
    except Exception as e:
        return False, str(e)


def find_existing_output_dir(ts_code: str, company_name: str = None, 
                             year: int = None, period: str = None) -> Path:
    """Find existing output directory for the given stock and report period.
    
    Directory structure:
    output/{代码}_{公司}/{年份}_{报告期}/
    Example: output/600989SH_宝丰能源/2024_年报/
    
    Returns the first existing directory matching the criteria, or None if not found.
    """
    code = ts_code.replace(".", "")
    output_root = PROJECT_ROOT / "output"
    
    company_dir_name = f"{code}_{company_name}" if company_name else code
    
    if year and period:
        period_dir = output_root / company_dir_name / f"{year}_{period}"
        if period_dir.exists() and period_dir.is_dir():
            return period_dir
    
    company_dir = output_root / company_dir_name
    if company_dir.exists() and company_dir.is_dir():
        return None
    
    return None


def create_output_dir(ts_code: str, company_name: str = None, 
                      year: int = None, period: str = "年报",
                      reuse_existing: bool = True) -> Path:
    """Create output directory for analysis results.
    
    Directory structure:
    output/{代码}_{公司}/{年份}_{报告期}/
    Example: output/600989SH_宝丰能源/2024_年报/
    
    Args:
        ts_code: Stock code (e.g., '600989.SH')
        company_name: Company name (e.g., '宝丰能源')
        year: Report year (e.g., 2024)
        period: Report period (e.g., '年报', '一季报')
        reuse_existing: Whether to reuse existing directory for same period
    """
    code = ts_code.replace(".", "")
    output_root = PROJECT_ROOT / "output"
    
    company_dir_name = f"{code}_{company_name}" if company_name else code
    company_dir = output_root / company_dir_name
    
    if reuse_existing:
        existing_dir = find_existing_output_dir(ts_code, company_name, year, period)
        if existing_dir:
            print(f"  [INFO] 复用已有目录: {existing_dir}")
            return existing_dir
    
    if year and period:
        period_dir_name = f"{year}_{period}"
        period_dir = company_dir / period_dir_name
        period_dir.mkdir(parents=True, exist_ok=True)
        return period_dir
    else:
        company_dir.mkdir(parents=True, exist_ok=True)
        return company_dir


def main():
    parser = argparse.ArgumentParser(
        description="Turtle Investment Framework - Data Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis without PDF
  python scripts/turtle_analysis.py --code 600887
  
  # Use local PDF file (will be copied to output directory)
  python scripts/turtle_analysis.py --code 600887 --pdf /path/to/report.pdf
  
  # Specify year for PDF
  python scripts/turtle_analysis.py --code 600887 --pdf report.pdf --year 2023
        """
    )
    parser.add_argument(
        "--code", "-c",
        required=True,
        help="Stock code (e.g., 600887, 600887.SH, 00700.HK)"
    )
    parser.add_argument(
        "--pdf", "-p",
        help="Path to local annual report PDF file (will be copied to output directory)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Target year for report (default: inferred from PDF filename or latest available)"
    )
    parser.add_argument(
        "--period",
        choices=["年报", "半年报", "一季报", "三季度报"],
        default="年报",
        help="Report period type (default: 年报)"
    )
    parser.add_argument(
        "--channel",
        choices=["direct", "hkconnect", "us"],
        default="direct",
        help="Holding channel for HK stocks (default: direct)"
    )
    parser.add_argument(
        "--company",
        help="Company name (optional, for output directory naming)"
    )
    parser.add_argument(
        "--skip-phase1a",
        action="store_true",
        help="Skip Phase 1A if data_pack_market.md already exists"
    )
    
    args = parser.parse_args()
    
    ts_code = validate_stock_code(args.code)
    print(f"\n🐢 龟龟投资策略分析 - 数据采集")
    print(f"   股票代码: {ts_code}")
    print(f"   持股渠道: {args.channel}")
    
    company_name = args.company
    if not company_name:
        print("  [INFO] 正在获取公司名称...")
        company_name = get_company_name(ts_code)
        if company_name:
            print(f"  [INFO] 公司名称: {company_name}")
        else:
            print("  [WARN] 未能获取公司名称，将使用代码作为目录名")
    
    report_year = args.year
    report_period = args.period
    
    if args.pdf and (report_year is None or report_period == "年报"):
        inferred_year, inferred_period = infer_report_period_from_filename(args.pdf)
        if report_year is None:
            report_year = inferred_year
        if report_period == "年报" and inferred_period != "年报":
            report_period = inferred_period
    
    if report_year is None:
        report_year = datetime.now().year - 1
        if datetime.now().month < 4:
            report_year -= 1
    
    print(f"   报告期: {report_year}年{report_period}")
    
    output_dir = create_output_dir(ts_code, company_name, report_year, report_period)
    
    print(f"   输出目录: {output_dir}")
    
    results = {
        "ts_code": ts_code,
        "channel": args.channel,
        "year": report_year,
        "period": report_period,
        "output_dir": str(output_dir),
        "phases": {}
    }
    
    has_pdf = False
    pdf_path = None
    
    if args.pdf:
        print(f"\n{'='*60}")
        print(f"[Phase 0] 本地 PDF 文件处理")
        print(f"{'='*60}")
        print(f"本地文件: {args.pdf}")
        
        success, msg, actual_year, actual_period = copy_local_pdf(
            args.pdf, output_dir, ts_code, company_name, report_year, report_period
        )
        results["phases"]["phase0"] = {"success": success, "output": msg, "source": "local"}
        
        if success:
            pdf_path = msg
            has_pdf = True
            if actual_year != report_year:
                report_year = actual_year
                results["year"] = report_year
            if actual_period != report_period:
                report_period = actual_period
                results["period"] = report_period
        else:
            print(f"\n⚠️ 本地 PDF 处理失败: {msg}")
    
    code = ts_code.replace(".", "")
    market_data_filename = "data_pack_market.md"
    market_data_path = output_dir / market_data_filename
    pdf_json_path = output_dir / "pdf_sections.json"
    
    if has_pdf and pdf_path:
        success, msg = run_phase2a(pdf_path, output_dir)
        results["phases"]["phase2a"] = {"success": success, "output": msg}
        if not success:
            print(f"\n⚠️ Phase 2A 失败: {msg}")
            has_pdf = False
    
    if not args.skip_phase1a:
        pdf_validation_path = pdf_json_path if (has_pdf and pdf_json_path.exists()) else None
        success, msg = run_phase1a(ts_code, market_data_path, pdf_validation_path)
        results["phases"]["phase1a"] = {"success": success, "output": msg}
        if not success:
            print(f"\n❌ Phase 1A 失败: {msg}")
            sys.exit(1)
    else:
        print(f"\n⏭️ 跳过 Phase 1A（使用现有数据）")
    
    results_file = output_dir / "analysis_status.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"数据采集完成")
    print(f"{'='*60}")
    print(f"状态文件: {results_file}")
    if has_pdf:
        print(f"PDF 预处理: {pdf_json_path}")
        print(f"数据验证: 已整合 PDF 数据与 Tushare 数据")
    print(f"市场数据: {market_data_path}")


if __name__ == "__main__":
    main()
