#!/usr/bin/env python3
"""Turtle Investment Framework - Data Collection Pipeline.

Core capabilities:
  - Tushare API data collection and storage
  - Local PDF file management
  - PDF preprocessing with parallel extraction
  - Checkpoint/resume support
  - Unified configuration management

Usage:
    python scripts/turtle_analysis.py --code 600887
    python scripts/turtle_analysis.py --code 600887.SH --pdf path/to/report.pdf
    python scripts/turtle_analysis.py --code 600887 --resume
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import tushare as ts

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from config import get_token, get_api_url, validate_stock_code, check_local_pdf

try:
    from config_loader import get_config, get_output_dir as get_config_output_dir
    from task_status import (
        TaskStatusManager,
        SubtaskStatus,
        create_status_manager,
        load_status_manager,
        SUBTASK_DEFINITIONS,
    )
    from pdf_parallel_extractor import ParallelPDFExtractor
    _new_modules_available = True
except ImportError as e:
    print(f"[WARN] New modules not available: {e}")
    _new_modules_available = False

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
    """Normalize report period string."""
    period_str = period_str.strip()
    for key, value in REPORT_PERIODS.items():
        if key in period_str:
            return value
    return "年报"


def infer_report_period_from_filename(filename: str) -> tuple[int, str]:
    """Extract year and period from PDF filename."""
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
    """Fetch company name from Tushare stock_basic API."""
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
    """Copy local PDF file to output directory with standardized naming."""
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


def run_phase1a(ts_code: str, output_file: Path, pdf_json_path: Path = None,
                status_manager: Optional['TaskStatusManager'] = None) -> tuple[bool, str]:
    """Run Tushare data collection (Phase 1A)."""
    
    if status_manager:
        status_manager.start_subtask("tushare_collect", metadata={"ts_code": ts_code})
    
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
            if status_manager:
                status_manager.complete_subtask("tushare_collect", output_file=str(output_file))
            return True, str(output_file)
        else:
            error_msg = f"tushare_collector.py exited with code {result.returncode}"
            if status_manager:
                status_manager.fail_subtask("tushare_collect", error_msg)
            return False, error_msg
    except Exception as e:
        if status_manager:
            status_manager.fail_subtask("tushare_collect", str(e))
        return False, str(e)


def run_phase2a(pdf_path: str, output_dir: Path, 
                status_manager: Optional['TaskStatusManager'] = None,
                use_parallel: bool = True,
                max_workers: int = 4) -> tuple[bool, str]:
    """Run PDF preprocessing (Phase 2A) with optional parallel extraction."""
    
    if status_manager:
        status_manager.start_subtask("pdf_extract", metadata={"pdf_path": pdf_path})
    
    output_file = output_dir / "pdf_sections.json"
    
    if use_parallel and _new_modules_available:
        print(f"\n{'='*60}")
        print(f"[Phase 2A] PDF 预处理 (并行模式, {max_workers} workers)")
        print(f"{'='*60}")
        print(f"PDF 文件: {pdf_path}")
        print(f"输出文件: {output_file}")
        print()
        
        try:
            extractor = ParallelPDFExtractor(max_workers=max_workers)
            result = extractor.extract(pdf_path, output_dir)
            
            if "error" in result:
                if status_manager:
                    status_manager.fail_subtask("pdf_extract", result["error"])
                return False, result["error"]
            
            summary = extractor.get_extraction_summary(result)
            print(f"\n  提取完成: {summary['found_sections']}/{summary['total_sections']} 章节")
            print(f"  耗时: {summary['total_duration_sec']:.1f}s")
            
            if status_manager:
                status_manager.complete_subtask(
                    "pdf_extract", 
                    output_file=str(output_file),
                    metadata={"sections_found": summary['found_sections']}
                )
            return True, str(output_file)
        
        except Exception as e:
            if status_manager:
                status_manager.fail_subtask("pdf_extract", str(e))
            return False, str(e)
    
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
            if status_manager:
                status_manager.complete_subtask("pdf_extract", output_file=str(output_file))
            return True, str(output_file)
        else:
            error_msg = f"pdf_preprocessor.py exited with code {result.returncode}"
            if status_manager:
                status_manager.fail_subtask("pdf_extract", error_msg)
            return False, error_msg
    except Exception as e:
        if status_manager:
            status_manager.fail_subtask("pdf_extract", str(e))
        return False, str(e)


def find_existing_output_dir(ts_code: str, company_name: str = None, 
                             year: int = None, period: str = None) -> Optional[Path]:
    """Find existing output directory for the given stock and report period."""
    code = ts_code.replace(".", "")
    output_root = PROJECT_ROOT / "output"
    
    company_dir_name = f"{code}_{company_name}" if company_name else code
    
    if year and period:
        period_dir = output_root / company_dir_name / f"{year}_{period}"
        if period_dir.exists() and period_dir.is_dir():
            return period_dir
    
    return None


def create_output_dir(ts_code: str, company_name: str = None, 
                      year: int = None, period: str = "年报",
                      reuse_existing: bool = True) -> Path:
    """Create output directory for analysis results."""
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


def check_existing_data(output_dir: Path) -> dict:
    """Check existing data files in output directory."""
    checks = {
        "data_pack_market.md": (output_dir / "data_pack_market.md").exists(),
        "pdf_sections.json": (output_dir / "pdf_sections.json").exists(),
        "web_search_result.md": (output_dir / "web_search_result.md").exists(),
        "analysis_status.json": (output_dir / "analysis_status.json").exists(),
    }
    
    completeness = sum(checks.values()) / len(checks) * 100
    
    return {
        "files": checks,
        "completeness": completeness,
        "has_all_data": all(checks.values()),
    }


def print_progress(status_manager: 'TaskStatusManager') -> None:
    """Print current task progress."""
    if not status_manager or not status_manager.checkpoint:
        return
    
    progress = status_manager.get_progress()
    
    print(f"\n{'='*50}")
    print(f"📊 任务进度: {progress['task_id']}")
    print(f"{'='*50}")
    print(f"状态: {progress['status']}")
    print(f"进度: {progress['completed']}/{progress['total']} ({progress['percentage']}%)")
    print()
    
    for subtask_id, subtask in status_manager.checkpoint.subtasks.items():
        definition = SUBTASK_DEFINITIONS.get(subtask_id, {})
        display_name = definition.get("display_name", subtask_id)
        
        status_icons = {
            SubtaskStatus.PENDING: "⏳",
            SubtaskStatus.RUNNING: "🔄",
            SubtaskStatus.COMPLETED: "✅",
            SubtaskStatus.FAILED: "❌",
            SubtaskStatus.SKIPPED: "⏭️",
        }
        icon = status_icons.get(subtask.status, "❓")
        
        duration_str = ""
        if subtask.duration_sec:
            duration_str = f" ({subtask.duration_sec:.1f}s)"
        
        print(f"  {icon} {display_name}{duration_str}")
        
        if subtask.error_message:
            print(f"      错误: {subtask.error_message}")
    
    print(f"{'='*50}\n")


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
  
  # Resume from checkpoint
  python scripts/turtle_analysis.py --code 600887 --resume
  
  # Use parallel PDF extraction
  python scripts/turtle_analysis.py --code 600887 --pdf report.pdf --parallel --workers 4
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
    parser.add_argument(
        "--parallel",
        action="store_true",
        default=True,
        help="Use parallel PDF extraction (default: True)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers for PDF extraction (default: 4)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show status of existing analysis"
    )
    
    args = parser.parse_args()
    
    ts_code = validate_stock_code(args.code)
    
    if args.status:
        output_dir = find_existing_output_dir(ts_code, args.company, args.year, args.period)
        if output_dir:
            status_manager = load_status_manager(output_dir)
            if status_manager:
                print_progress(status_manager)
            else:
                print(f"未找到分析状态文件: {output_dir}")
        else:
            print(f"未找到输出目录")
        return
    
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
    
    status_manager = None
    if _new_modules_available:
        if args.resume:
            existing_manager = load_status_manager(output_dir)
            if existing_manager and existing_manager.can_resume():
                status_manager = existing_manager
                print(f"\n  [INFO] 从检查点恢复: {status_manager.checkpoint.task_id}")
                print_progress(status_manager)
            else:
                status_manager = create_status_manager(
                    output_dir, ts_code, company_name, report_year, report_period, args.channel
                )
        else:
            status_manager = create_status_manager(
                output_dir, ts_code, company_name, report_year, report_period, args.channel
            )
    
    existing_data = check_existing_data(output_dir)
    if existing_data["completeness"] >= 90 and not args.resume:
        print(f"\n  [INFO] 检测到现有数据 (完整度: {existing_data['completeness']:.0f}%)")
        print(f"  [INFO] 使用 --resume 参数可从检查点恢复")
    
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
    
    pdf_extract_status = status_manager.get_subtask_status("pdf_extract") if status_manager else None
    if pdf_extract_status == SubtaskStatus.COMPLETED:
        print(f"\n  [INFO] PDF提取已完成，跳过")
        has_pdf = True
        pdf_path = str(output_dir / f"{ts_code.split('.')[0]}_{report_year}_{company_name}_{report_period}.pdf")
        if not os.path.exists(pdf_path):
            pdf_files = list(output_dir.glob("*.pdf"))
            if pdf_files:
                pdf_path = str(pdf_files[0])
    
    if args.pdf and pdf_extract_status != SubtaskStatus.COMPLETED:
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
            if status_manager:
                status_manager.skip_subtask("pdf_extract", msg)
    
    code = ts_code.replace(".", "")
    market_data_filename = "data_pack_market.md"
    market_data_path = output_dir / market_data_filename
    pdf_json_path = output_dir / "pdf_sections.json"
    
    if has_pdf and pdf_path and pdf_extract_status != SubtaskStatus.COMPLETED:
        success, msg = run_phase2a(
            pdf_path, output_dir, status_manager, 
            use_parallel=args.parallel, 
            max_workers=args.workers
        )
        results["phases"]["phase2a"] = {"success": success, "output": msg}
        if not success:
            print(f"\n⚠️ Phase 2A 失败: {msg}")
            has_pdf = False
    
    tushare_status = status_manager.get_subtask_status("tushare_collect") if status_manager else None
    if tushare_status == SubtaskStatus.COMPLETED:
        print(f"\n  [INFO] Tushare采集已完成，跳过")
    elif not args.skip_phase1a:
        pdf_validation_path = pdf_json_path if (has_pdf and pdf_json_path.exists()) else None
        success, msg = run_phase1a(ts_code, market_data_path, pdf_validation_path, status_manager)
        results["phases"]["phase1a"] = {"success": success, "output": msg}
        if not success:
            print(f"\n❌ Phase 1A 失败: {msg}")
            sys.exit(1)
    else:
        print(f"\n⏭️ 跳过 Phase 1A（使用现有数据）")
    
    if status_manager:
        status_manager.save_checkpoint()
        print_progress(status_manager)
    
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
