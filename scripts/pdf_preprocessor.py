#!/usr/bin/env python3
"""Turtle Investment Framework - PDF Preprocessor (Phase 2A).

Scans annual report PDFs for target sections using keyword matching
and outputs structured JSON for Agent fine-extraction.

Requires: Python 3.7+
Dependencies: pdfplumber (required), PyMuPDF/fitz (optional fallback)

Usage:
    python3 scripts/pdf_preprocessor.py --pdf report.pdf
    python3 scripts/pdf_preprocessor.py --pdf report.pdf --output output/sections.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

if sys.version_info < (3, 7):
    raise RuntimeError("Python 3.7+ is required. Current version: " + sys.version)

try:
    import pdfplumber
except ImportError:
    raise ImportError(
        "pdfplumber is required. Install with: pip install pdfplumber"
    )

from pdf_config import (
    GARBLED_THRESHOLD,
    GARBLED_PAGE_RATIO,
    DEFAULT_BUFFER_PAGES,
    DEFAULT_MAX_CHARS,
    SECTION_KEYWORDS,
    SECTION_EXTRACT_CONFIG,
    ZONE_MARKERS,
    SECTION_ZONE_PREFERENCES,
    SECTION_ORDER,
    TOC_HINTS,
    AR_EXCLUDE_TERMS,
    OUTPUT_DIR_NAME,
    JSON_FILENAME,
    MD_FILENAME,
    DEFAULT_ENCODING,
)

from pdf_processor import (
    is_garbled,
    extract_all_pages,
    get_zone_for_page,
    _score_match,
    _center_truncate,
    _truncate_at_boundary,
    _extract_page_content,
)

_logger = logging.getLogger(__name__)


def find_section_pages(section_id: str, page_texts: Dict[int, str], 
                     page_tables: Dict[int, list], 
                     zone_markers: List[Tuple[str, str]],
                     zone_preferences: Dict[str, Dict[str, List[str]]]) -> List[Tuple[int, float]]:
    """Find pages that match a section.
    
    Args:
        section_id: Section ID
        page_texts: Page texts
        page_tables: Page tables
        zone_markers: Zone markers
        zone_preferences: Zone preferences
        
    Returns:
        List of (page_num, score) tuples
    """
    keywords = SECTION_KEYWORDS.get(section_id, [])
    if not keywords:
        return []
    
    scores: List[Tuple[int, float]] = []
    page_zones: Dict[int, Optional[str]] = {}
    
    for page_num, text in page_texts.items():
        page_zone = get_zone_for_page(text, zone_markers)
        page_zones[page_num] = page_zone
        
        score = _score_match(
            section_id,
            text,
            keywords,
            page_zone,
            zone_preferences,
            AR_EXCLUDE_TERMS
        )
        
        if score > 0:
            scores.append((page_num, score))
    
    scores.sort(key=lambda x: x[1], reverse=True)
    
    return scores


def extract_section_content(section_id: str, page_texts: Dict[int, str], 
                          page_tables: Dict[int, list], 
                          scores: List[Tuple[int, float]],
                          total_pages: int) -> Optional[str]:
    """Extract content for a section with paragraph-level deduplication.
    
    Args:
        section_id: Section ID
        page_texts: Page texts
        page_tables: Page tables
        scores: Page scores
        total_pages: Total pages
        
    Returns:
        Extracted content or None
    """
    from pdf_config import SECTION_KEYWORDS, EXTRACT_PRIORITY
    
    if not scores:
        return None
    
    config = SECTION_EXTRACT_CONFIG.get(section_id, {})
    buffer_pages = config.get("buffer_pages", DEFAULT_BUFFER_PAGES)
    max_chars = config.get("max_chars", DEFAULT_MAX_CHARS)
    
    # Get section keywords configuration
    section_config = SECTION_KEYWORDS.get(section_id, {})
    include_kw = section_config.get("include", [])
    exclude_kw = section_config.get("exclude", [])
    mandatory_kw = section_config.get("mandatory", [])
    
    best_page = scores[0][0]
    
    parts = []
    # 全局已提取段落记录，只过滤完全重复的段落，不屏蔽整页
    extracted_paragraphs: set = set()
    
    # 只提取目标buffer页，不做整页屏蔽
    for offset in range(-buffer_pages, buffer_pages + 1):
        target = best_page + offset
        if 0 <= target < total_pages:
            raw_text = page_texts.get(target, "")
            if not raw_text:
                continue
            
            # 按段落拆分，精准过滤，只保留当前模块需要的内容
            paragraphs = raw_text.split("\n")
            valid_paragraphs = []
            for para in paragraphs:
                para_stripped = para.strip()
                # 空段落、排他关键词段落直接跳过
                if not para_stripped or any(kw in para_stripped for kw in exclude_kw):
                    continue
                
                # 【核心规则】命中强制关键词，直接保留，优先级最高
                if any(kw in para_stripped for kw in mandatory_kw):
                    valid_paragraphs.append(para)
                    continue
                
                # 【核心规则】命中当前模块关键词，直接保留，哪怕被其他模块提取过也不屏蔽
                if any(kw in para_stripped for kw in include_kw):
                    valid_paragraphs.append(para)
                # 【去重规则】未命中关键词的通用冗余段落，重复的才过滤
                else:
                    if para_stripped not in extracted_paragraphs:
                        valid_paragraphs.append(para)
                        extracted_paragraphs.add(para_stripped)
            
            if valid_paragraphs:
                parts.append(f"--- p.{target} ---")
                parts.append("\n".join(valid_paragraphs))
    
    # 合并内容，按标题层级精准提取，截断超长内容
    combined = "\n\n".join(parts)
    combined = _extract_by_title_level(combined, section_id, include_kw)
    if len(combined) > max_chars:
        combined = _center_truncate(combined, include_kw[0] if include_kw else "", max_chars)
    
    # 【兜底机制】如果提取内容为空，强制取最佳页的关键词核心内容，绝不出现空模块
    if not combined or len(combined.strip()) < 100:
        raw_text = page_texts.get(best_page, "")
        combined = _center_truncate(raw_text, include_kw[0] if include_kw else "", max_chars)
    
    return combined.strip() if combined else None


def write_output(sections: Dict[str, Optional[str]], output_dir: str, 
                metadata: Dict[str, Any]) -> Tuple[str, str]:
    """Write extracted sections to output files.
    
    Args:
        sections: Extracted sections
        output_dir: Output directory
        metadata: Metadata
        
    Returns:
        Tuple of (json_path, md_path)
    """
    os.makedirs(output_dir, exist_ok=True)
    
    json_path = os.path.join(output_dir, JSON_FILENAME)
    md_path = os.path.join(output_dir, MD_FILENAME)
    
    output_data = {
        "metadata": metadata,
        "sections": sections
    }
    
    try:
        with open(json_path, 'w', encoding=DEFAULT_ENCODING) as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        _logger.info(f"Wrote JSON output to {json_path}")
    except Exception as e:
        _logger.error(f"Failed to write JSON: {e}")
        json_path = ""
    
    md_content = f"# PDF 财报提取内容\n\n"
    md_content += f"## 元数据\n"
    for key, value in metadata.items():
        md_content += f"- **{key}**: {value}\n"
    md_content += "\n"
    
    for section_id in SECTION_ORDER:
        content = sections.get(section_id)
        if content:
            section_name = section_id.replace("_", " ").title()
            md_content += f"## {section_name} ({section_id})\n"
            md_content += f"\n{content}\n\n"
    
    try:
        with open(md_path, 'w', encoding=DEFAULT_ENCODING) as f:
            f.write(md_content)
        _logger.info(f"Wrote Markdown output to {md_path}")
    except Exception as e:
        _logger.error(f"Failed to write Markdown: {e}")
        md_path = ""
    
    return json_path, md_path


def run_pipeline(pdf_path: str, output_path: Optional[str] = None, 
                verbose: bool = False) -> Dict[str, Any]:
    """Run the PDF preprocessing pipeline.
    
    Args:
        pdf_path: Path to PDF file
        output_path: Output path (directory or file)
        verbose: Enable verbose logging
        
    Returns:
        Dict with results
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG, 
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    _logger.info(f"Processing PDF: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        _logger.error(f"PDF file not found: {pdf_path}")
        return {"error": "PDF file not found"}
    
    try:
        page_texts, page_tables, garbled_ratio, used_fallback = extract_all_pages(pdf_path)
        total_pages = len(page_texts)
        
        metadata = {
            "pdf_path": pdf_path,
            "total_pages": total_pages,
            "garbled_ratio": f"{garbled_ratio:.1%}",
            "used_fallback": used_fallback,
            "extraction_time": datetime.now().isoformat(),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }
        
        from pdf_config import EXTRACT_PRIORITY
        
        sections: Dict[str, Optional[str]] = {}
        found_sections = 0
        
        # 按优先级遍历模块，不再按字典乱序遍历
        for section_id in EXTRACT_PRIORITY:
            _logger.info(f"Processing section: {section_id}")
            scores = find_section_pages(
                section_id,
                page_texts,
                page_tables,
                ZONE_MARKERS,
                SECTION_ZONE_PREFERENCES
            )
            
            if scores:
                content = extract_section_content(
                    section_id,
                    page_texts,
                    page_tables,
                    scores,
                    total_pages
                )
                sections[section_id] = content
                if content:
                    found_sections += 1
                    _logger.info(f"Found section {section_id} (score: {scores[0][1]:.1f})")
                else:
                    _logger.warning(f"Section {section_id} found but no content extracted")
            else:
                sections[section_id] = None
                _logger.warning(f"Section {section_id} not found")
        
        metadata["found_sections"] = found_sections
        metadata["total_sections"] = len(SECTION_KEYWORDS)
        
        if output_path:
            if os.path.isdir(output_path):
                output_dir = output_path
            else:
                output_dir = os.path.dirname(output_path)
                if not output_dir:
                    output_dir = OUTPUT_DIR_NAME
            
            json_path, md_path = write_output(sections, output_dir, metadata)
            metadata["json_output"] = json_path
            metadata["md_output"] = md_path
        
        _logger.info(f"Processing completed: {found_sections}/{len(SECTION_KEYWORDS)} sections found")
        
        return {
            "metadata": metadata,
            "sections": sections
        }
        
    except Exception as e:
        _logger.error(f"Pipeline failed: {e}", exc_info=True)
        return {"error": str(e)}


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='PDF Preprocessor for annual reports')
    parser.add_argument('--pdf', required=True, help='Path to PDF file')
    parser.add_argument('--output', help='Output directory or file path')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    result = run_pipeline(args.pdf, args.output, args.verbose)
    
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    else:
        metadata = result.get("metadata", {})
        print(f"\nProcessing completed!")
        print(f"Total pages: {metadata.get('total_pages', 0)}")
        print(f"Found sections: {metadata.get('found_sections', 0)}/{metadata.get('total_sections', 0)}")
        print(f"Garbled ratio: {metadata.get('garbled_ratio', '0%')}")
        if 'json_output' in metadata:
            print(f"JSON output: {metadata['json_output']}")
        if 'md_output' in metadata:
            print(f"Markdown output: {metadata['md_output']}")


if __name__ == "__main__":
    main()