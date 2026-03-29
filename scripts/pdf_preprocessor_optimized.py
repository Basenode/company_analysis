#!/usr/bin/env python3
"""Optimized PDF preprocessor with performance improvements."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set

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
    EXTRACT_PRIORITY,
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
    _extract_by_title_level,
)

_logger = logging.getLogger(__name__)


def find_section_pages(section_id: str, page_texts: Dict[int, str], 
                     page_tables: Dict[int, list], 
                     zone_markers: List[Tuple[str, str]],
                     zone_preferences: Dict[str, Dict[str, List[str]]]) -> List[Tuple[int, float]]:
    """Find pages that match a section (optimized)."""
    keywords = SECTION_KEYWORDS.get(section_id, {}).get("include", [])
    if not keywords:
        return []
    
    scores: List[Tuple[int, float]] = []
    page_zones: Dict[int, Optional[str]] = {}
    
    # Precompile regex patterns for zone markers
    compiled_markers = [(re.compile(pat), zone) for pat, zone in zone_markers]
    
    # Preprocess page zones to avoid repeated calculations
    for page_num, text in page_texts.items():
        if page_num not in page_zones:
            zone = None
            for pattern, z in compiled_markers:
                if pattern.search(text):
                    zone = z
                    break
            page_zones[page_num] = zone
    
    # Batch process pages
    for page_num, text in page_texts.items():
        page_zone = page_zones[page_num]
        
        # Early exit for very short text
        if len(text) < 100:
            continue
        
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
    
    # Sort by score (descending)
    scores.sort(key=lambda x: x[1], reverse=True)
    
    # Limit to top 5 pages to reduce processing
    return scores[:5]


def extract_section_content(section_id: str, page_texts: Dict[int, str], 
                          page_tables: Dict[int, list], 
                          scores: List[Tuple[int, float]],
                          total_pages: int, 
                          extracted_paragraphs: Set[str]) -> Optional[str]:
    """Extract content for a section with performance optimizations."""
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
    
    # Process buffer pages
    processed_pages = set()
    for offset in range(-buffer_pages, buffer_pages + 1):
        target = best_page + offset
        if 0 <= target < total_pages and target not in processed_pages:
            raw_text = page_texts.get(target, "")
            if not raw_text:
                continue
            
            # Process paragraphs
            paragraphs = raw_text.split("\n")
            valid_paragraphs = []
            
            for para in paragraphs:
                para_stripped = para.strip()
                # Skip empty paragraphs and excluded content
                if not para_stripped or any(kw in para_stripped for kw in exclude_kw):
                    continue
                
                # Mandatory keywords - highest priority
                if any(kw in para_stripped for kw in mandatory_kw):
                    valid_paragraphs.append(para)
                # Include keywords
                elif any(kw in para_stripped for kw in include_kw):
                    valid_paragraphs.append(para)
                # General content - deduplicate
                else:
                    if para_stripped not in extracted_paragraphs:
                        valid_paragraphs.append(para)
                        extracted_paragraphs.add(para_stripped)
            
            if valid_paragraphs:
                parts.append(f"--- p.{target} ---")
                parts.append("\n".join(valid_paragraphs))
            
            processed_pages.add(target)
    
    # Merge and process content
    combined = "\n\n".join(parts)
    
    # Extract by title level (if applicable)
    if combined:
        combined = _extract_by_title_level(combined, section_id, include_kw)
        
        # Truncate if too long
        if len(combined) > max_chars:
            keyword = include_kw[0] if include_kw else ""
            combined = _center_truncate(combined, keyword, max_chars)
    
    # Fallback if no content
    if not combined or len(combined.strip()) < 100:
        raw_text = page_texts.get(best_page, "")
        keyword = include_kw[0] if include_kw else ""
        combined = _center_truncate(raw_text, keyword, max_chars)
    
    return combined.strip() if combined else None


def write_output(sections: Dict[str, Optional[str]], output_dir: str, 
                metadata: Dict[str, Any]) -> Tuple[str, str]:
    """Write extracted sections to output files."""
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
                verbose: bool = False, 
                max_pages: Optional[int] = None) -> Dict[str, Any]:
    """Run the PDF preprocessing pipeline with performance optimizations."""
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
        # Extract pages with optional max_pages limit
        start_extract = time.time()
        page_texts, page_tables, garbled_ratio, used_fallback = extract_all_pages(pdf_path, max_pages)
        extract_time = time.time() - start_extract
        total_pages = len(page_texts)
        
        _logger.info(f"PDF extraction completed in {extract_time:.2f} seconds")
        
        metadata = {
            "pdf_path": pdf_path,
            "total_pages": total_pages,
            "garbled_ratio": f"{garbled_ratio:.1%}",
            "used_fallback": used_fallback,
            "extraction_time": datetime.now().isoformat(),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }
        
        sections: Dict[str, Optional[str]] = {}
        found_sections = 0
        
        # Global extracted paragraphs set for deduplication
        extracted_paragraphs: Set[str] = set()
        
        # Process sections in priority order
        start_process = time.time()
        for section_id in EXTRACT_PRIORITY:
            _logger.info(f"Processing section: {section_id}")
            
            # Find matching pages
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
                    total_pages,
                    extracted_paragraphs
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
        
        process_time = time.time() - start_process
        _logger.info(f"Section processing completed in {process_time:.2f} seconds")
        
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
    parser.add_argument('--max-pages', type=int, help='Maximum number of pages to process')
    
    args = parser.parse_args()
    
    result = run_pipeline(args.pdf, args.output, args.verbose, args.max_pages)
    
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