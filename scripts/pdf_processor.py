#!/usr/bin/env python3
"""Turtle Investment Framework - PDF Text Processor.

Handles PDF text extraction, garbled detection, and table processing.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

from pdf_config import GARBLED_THRESHOLD

_logger = logging.getLogger(__name__)


def is_garbled(text: str, threshold: float = GARBLED_THRESHOLD) -> bool:
    """Check if text is garbled.
    
    Args:
        text: Text to check
        threshold: Threshold for garbled detection (0.0-1.0)
        
    Returns:
        True if text is garbled, False otherwise
    """
    if not text or len(text.strip()) < 10:
        return True
    
    table_markers = ["|", "---", "[TABLE]"]
    in_table = sum(1 for m in table_markers if m in text) >= 2
    
    if in_table:
        text_without_table = re.sub(r'\[TABLE\].*?(?=\n---|\Z)', '', text, flags=re.DOTALL)
        text_without_table = re.sub(r'\|.*?\|', '', text_without_table)
        if text_without_table.strip():
            text = text_without_table
    
    normal = 0
    total_chars = 0
    for ch in text:
        if ch in "\n\r\t ":
            continue
        total_chars += 1
        cp = ord(ch)
        if (
            0x20 <= cp <= 0x7E  # ASCII
            or 0x4E00 <= cp <= 0x9FFF  # Common Chinese
            or 0x3400 <= cp <= 0x4DBF  # Ext Chinese
            or 0x3000 <= cp <= 0x303F  # Punctuation
            or 0xFF00 <= cp <= 0xFFEF  # Fullwidth
        ):
            normal += 1
    
    if total_chars == 0:
        return True
    
    ratio = normal / total_chars
    return ratio < (1 - threshold)


def _tables_to_markdown(tables: list) -> str:
    """Convert tables to Markdown format.
    
    Args:
        tables: List of tables from pdfplumber
        
    Returns:
        Markdown formatted tables
    """
    parts = []
    for table in tables:
        if not table or len(table) < 2:
            continue
        cleaned = []
        max_cols = 0
        for row in table:
            cleaned_row = [(cell or "").replace("\n", " ").strip() for cell in row]
            cleaned.append(cleaned_row)
            max_cols = max(max_cols, len(cleaned_row))
        
        if max_cols == 0:
            continue
            
        for row in cleaned:
            while len(row) < max_cols:
                row.append("")
        
        header = cleaned[0]
        md = "| " + " | ".join(header[:max_cols]) + " |\n"
        md += "| " + " | ".join(["---"] * max_cols) + " |\n"
        for row in cleaned[1:]:
            md += "| " + " | ".join(row[:max_cols]) + " |\n"
        parts.append(md)
    return "\n\n".join(parts)


def fallback_extract_pymupdf(pdf_path: str) -> Optional[str]:
    """Fallback text extraction using PyMuPDF (fitz).
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Extracted text, or None if PyMuPDF is not available
    """
    try:
        import fitz
        _logger.info("Using PyMuPDF for fallback extraction")
        doc = fitz.open(pdf_path)
        text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            page_text = page.get_text()
            text += f"\n=== PAGE {page_num + 1} ===\n"
            text += page_text
        doc.close()
        return text
    except ImportError:
        _logger.warning("PyMuPDF not installed, skipping fallback extraction")
        return None
    except Exception as e:
        _logger.error(f"PyMuPDF extraction failed: {e}")
        return None


def extract_all_pages(pdf_path: str, max_pages: Optional[int] = None) -> Tuple[Dict[int, str], Dict[int, list], float, bool]:
    """Extract text and tables from all PDF pages.
    
    Args:
        pdf_path: Path to PDF file
        max_pages: Maximum number of pages to process
        
    Returns:
        Tuple of (page_texts, page_tables, garbled_ratio, used_fallback)
    """
    page_texts: Dict[int, str] = {}
    page_tables: Dict[int, list] = {}
    garbled_pages = 0
    total_pages = 0
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            process_pages = min(total_pages, max_pages) if max_pages else total_pages
            
            for page_num in range(process_pages):
                page = pdf.pages[page_num]
                try:
                    text = page.extract_text() or ""
                    tables = page.extract_tables() or []
                    
                    page_texts[page_num] = text
                    page_tables[page_num] = tables
                    
                    if is_garbled(text):
                        garbled_pages += 1
                        _logger.debug(f"Page {page_num + 1} is garbled")
                    
                except Exception as e:
                    _logger.error(f"Error processing page {page_num + 1}: {e}")
                    page_texts[page_num] = ""
                    page_tables[page_num] = []
    
    except Exception as e:
        _logger.error(f"PDF extraction failed: {e}")
        fallback_text = fallback_extract_pymupdf(pdf_path)
        if fallback_text:
            _logger.info("Using fallback extraction")
            lines = fallback_text.split("\n")
            current_page = 0
            current_text = []
            
            for line in lines:
                if line.startswith("=== PAGE "):
                    if current_text:
                        page_texts[current_page] = "\n".join(current_text)
                        current_text = []
                    current_page = int(line.split()[2]) - 1
                else:
                    current_text.append(line)
            
            if current_text:
                page_texts[current_page] = "\n".join(current_text)
            
            garbled_ratio = 0.0
            used_fallback = True
            return page_texts, page_tables, garbled_ratio, used_fallback
    
    garbled_ratio = garbled_pages / total_pages if total_pages > 0 else 0.0
    used_fallback = False
    
    _logger.info(f"Extracted {total_pages} pages, {garbled_pages} garbled ({garbled_ratio:.1%})")
    return page_texts, page_tables, garbled_ratio, used_fallback


def get_zone_for_page(page_text: str, zone_markers: List[Tuple[str, str]]) -> Optional[str]:
    """Determine zone for a page based on markers.
    
    Args:
        page_text: Page text
        zone_markers: List of (regex, zone) tuples
        
    Returns:
        Zone name or None
    """
    for pattern, zone in zone_markers:
        if re.search(pattern, page_text, re.IGNORECASE):
            return zone
    return None


def _score_match(section_id: str, page_text: str, keywords: List[str], 
                page_zone: Optional[str], zone_preferences: Dict[str, Dict[str, List[str]]],
                ar_exclude_terms: List[str]) -> float:
    """Score how well a page matches a section.
    
    Args:
        section_id: Section ID
        page_text: Page text
        keywords: Section keywords
        page_zone: Page zone
        zone_preferences: Zone preferences
        ar_exclude_terms: AR section exclusion terms
        
    Returns:
        Match score (higher is better)
    """
    score = 0.0
    
    for keyword in keywords:
        if keyword in page_text:
            keyword_pos = page_text.find(keyword)
            score += 1.0
            
            if keyword_pos < 1000:
                score += 0.5
            
            if section_id == "AR" and keyword_pos >= 0:
                context_window = page_text[max(0, keyword_pos - 200):min(len(page_text), keyword_pos + 200)]
                if any(term in context_window for term in ar_exclude_terms):
                    score -= 3.0
    
    if page_zone:
        prefs = zone_preferences.get(section_id, {})
        if page_zone in prefs.get("prefer", []):
            score += 2.0
        elif page_zone in prefs.get("avoid", []):
            score -= 2.0
    
    if "目录" in page_text or "Contents" in page_text:
        score -= 0.5
    
    return score


def _extract_by_title_level(text: str, section_id: str, keywords: List[str]) -> str:
    """Extract content by title level for precise section boundaries.
    
    Args:
        text: Text to extract from
        section_id: Section ID
        keywords: Section keywords
        
    Returns:
        Extracted content within title boundaries
    """
    from pdf_config import SECTION_TITLE_BINDING
    
    title_patterns = [
        r"[一二三四五六七八九十]+、\s*.*",
        r"\d+\.\s*.*",
        r"\([一二三四五六七八九十]+\)\s*.*",
    ]
    # 优先取绑定的专属标题，没有再用通用关键词
    bind_titles = SECTION_TITLE_BINDING.get(section_id, keywords)
    
    lines = text.split("\n")
    target_content = []
    in_target_section = False
    current_level = 0

    for line in lines:
        line_stripped = line.strip()
        is_title = any(re.match(pat, line_stripped) for pat in title_patterns)
        if is_title:
            # 匹配绑定的专属标题，进入提取状态
            if any(kw in line_stripped for kw in bind_titles):
                in_target_section = True
                for i, pat in enumerate(title_patterns):
                    if re.match(pat, line_stripped):
                        current_level = i
                        break
                target_content.append(line)
                continue
            # 遇到同级/上级标题，退出提取，绝不越界提取其他模块的内容
            if in_target_section:
                for i, pat in enumerate(title_patterns):
                    if re.match(pat, line_stripped) and i <= current_level:
                        in_target_section = False
                        break
        if in_target_section and line_stripped:
            target_content.append(line)
    
    return "\n".join(target_content) if target_content else text


def _center_truncate(text: str, keyword: str, max_chars: int = 4000) -> str:
    """Truncate text around a keyword.
    
    Args:
        text: Text to truncate
        keyword: Keyword to center around
        max_chars: Maximum characters to return
        
    Returns:
        Truncated text
    """
    if not text:
        return ""
    
    pos = text.find(keyword)
    if pos == -1:
        return text[:max_chars]
    
    half = max_chars // 2
    start = max(0, pos - half)
    end = min(len(text), pos + len(keyword) + half)
    
    truncated = text[start:end]
    if start > 0:
        truncated = "... " + truncated
    if end < len(text):
        truncated = truncated + " ..."
    
    return truncated


def _truncate_at_boundary(text: str, max_chars: int) -> str:
    """Truncate text at sentence/paragraph boundary.
    
    Args:
        text: Text to truncate
        max_chars: Maximum characters
        
    Returns:
        Truncated text
    """
    if len(text) <= max_chars:
        return text
    
    truncate_at = max_chars
    boundaries = ["。", "！", "？", "\n\n", ".", "!", "?", "\n"]
    
    for boundary in boundaries:
        pos = text.rfind(boundary, 0, max_chars)
        if pos != -1:
            truncate_at = pos + len(boundary)
            break
    
    truncated = text[:truncate_at]
    if truncate_at < len(text):
        truncated += " ..."
    
    return truncated


def _extract_page_content(page_text: str, page_tables: list, 
                        buffer_pages: List[Tuple[int, str, list]], 
                        max_chars: int = 4000) -> str:
    """Extract content from a page and buffer pages.
    
    Args:
        page_text: Main page text
        page_tables: Main page tables
        buffer_pages: Buffer pages (page_num, text, tables)
        max_chars: Maximum characters
        
    Returns:
        Extracted content
    """
    all_text = [page_text]
    all_tables = [page_tables]
    
    for _, buffer_text, buffer_tables in buffer_pages:
        all_text.append(buffer_text)
        all_tables.append(buffer_tables)
    
    text_content = "\n\n".join(all_text)
    table_content = _tables_to_markdown([table for tables in all_tables for table in tables])
    
    if table_content:
        full_content = text_content + "\n\n" + table_content
    else:
        full_content = text_content
    
    truncated = _truncate_at_boundary(full_content, max_chars)
    
    return truncated