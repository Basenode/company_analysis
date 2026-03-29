#!/usr/bin/env python3
"""Parallel PDF Section Extractor for Turtle Investment Framework.

Provides multiprocessing-based PDF section extraction with:
1. Parallel processing of multiple sections
2. Incremental extraction (compare with existing results)
3. Resource management and timeout handling
4. Integration with config_loader

Usage:
    from pdf_parallel_extractor import ParallelPDFExtractor
    
    extractor = ParallelPDFExtractor(config)
    result = extractor.extract(pdf_path, output_dir)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import Manager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import pdfplumber
except ImportError:
    raise ImportError("pdfplumber is required. Install with: pip install pdfplumber")

_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

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
    AR_EXCLUDE_TERMS,
    JSON_FILENAME,
    MD_FILENAME,
    DEFAULT_ENCODING,
)

try:
    from config_loader import get_config
    _config_available = True
except ImportError:
    _config_available = False

_logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    section_id: str
    content: Optional[str] = None
    score: float = 0.0
    pages: List[int] = field(default_factory=list)
    duration_sec: float = 0.0
    error: Optional[str] = None
    from_cache: bool = False


@dataclass
class PDFMetadata:
    pdf_path: str
    total_pages: int = 0
    file_size_mb: float = 0.0
    file_hash: str = ""
    garbled_ratio: float = 0.0
    used_fallback: bool = False
    extraction_time: str = ""
    python_version: str = ""


def compute_file_hash(pdf_path: str) -> str:
    """Compute MD5 hash of PDF file for change detection."""
    hash_md5 = hashlib.md5()
    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def is_garbled(text: str, threshold: float = GARBLED_THRESHOLD) -> bool:
    """Check if text is garbled."""
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
            0x20 <= cp <= 0x7E
            or 0x4E00 <= cp <= 0x9FFF
            or 0x3400 <= cp <= 0x4DBF
            or 0x3000 <= cp <= 0x303F
            or 0xFF00 <= cp <= 0xFFEF
        ):
            normal += 1
    
    if total_chars == 0:
        return True
    
    ratio = normal / total_chars
    return ratio < (1 - threshold)


def _tables_to_markdown(tables: list) -> str:
    """Convert tables to Markdown format."""
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


def extract_all_pages(pdf_path: str, max_pages: Optional[int] = None) -> Tuple[Dict[int, str], Dict[int, list], float, bool]:
    """Extract text and tables from all PDF pages."""
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
                text = page.extract_text() or ""
                tables = page.extract_tables()
                
                page_texts[page_num] = text
                page_tables[page_num] = tables
                
                if is_garbled(text):
                    garbled_pages += 1
    
    except Exception as e:
        _logger.error(f"PDF extraction error: {e}")
        return {}, {}, 1.0, False
    
    garbled_ratio = garbled_pages / total_pages if total_pages > 0 else 0.0
    used_fallback = False
    
    if garbled_ratio > GARBLED_PAGE_RATIO:
        fallback_text = _fallback_extract_pymupdf(pdf_path)
        if fallback_text:
            lines = fallback_text.split("=== PAGE")
            page_texts = {}
            for i, line in enumerate(lines[1:], 0):
                page_texts[i] = line.split("===")[-1].strip() if "===" in line else line.strip()
            used_fallback = True
            garbled_ratio = 0.0
    
    return page_texts, page_tables, garbled_ratio, used_fallback


def _fallback_extract_pymupdf(pdf_path: str) -> Optional[str]:
    """Fallback text extraction using PyMuPDF."""
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


def _score_match(
    section_id: str,
    text: str,
    keywords: List[str],
    page_zone: Optional[str],
    zone_preferences: Dict[str, Dict[str, List[str]]],
    ar_exclude_terms: List[str],
) -> float:
    """Score how well a page matches a section."""
    if not text or len(text.strip()) < 100:
        return 0.0
    
    score = 0.0
    
    for kw in keywords:
        if kw in text:
            score += 10.0
    
    section_config = SECTION_KEYWORDS.get(section_id, {})
    mandatory_kw = section_config.get("mandatory", [])
    for kw in mandatory_kw:
        if kw in text:
            score += 20.0
    
    exclude_kw = section_config.get("exclude", [])
    for kw in exclude_kw:
        if kw in text:
            score -= 5.0
    
    if section_id == "AR":
        for term in ar_exclude_terms:
            if term in text:
                score -= 15.0
    
    prefs = zone_preferences.get(section_id, {})
    if page_zone:
        if page_zone in prefs.get("prefer", []):
            score += 5.0
        if page_zone in prefs.get("avoid", []):
            score -= 10.0
    
    return max(score, 0.0)


def _get_zone_for_page(text: str, zone_markers: List[Tuple[str, str]]) -> Optional[str]:
    """Determine which zone a page belongs to."""
    for pattern, zone in zone_markers:
        if re.search(pattern, text):
            return zone
    return None


def _center_truncate(text: str, keyword: str, max_chars: int) -> str:
    """Truncate text around a keyword."""
    if len(text) <= max_chars:
        return text
    
    if keyword and keyword in text:
        idx = text.find(keyword)
        start = max(0, idx - max_chars // 2)
        end = min(len(text), start + max_chars)
        return "..." + text[start:end] + "..."
    
    return text[:max_chars] + "..."


def _truncate_at_boundary(text: str, max_chars: int) -> str:
    """Truncate text at sentence boundary."""
    if len(text) <= max_chars:
        return text
    
    truncated = text[:max_chars]
    last_period = truncated.rfind("。")
    last_newline = truncated.rfind("\n")
    
    boundary = max(last_period, last_newline)
    if boundary > max_chars * 0.7:
        return truncated[:boundary + 1]
    
    return truncated + "..."


def _extract_by_title_level(text: str, section_id: str, keywords: List[str]) -> str:
    """Extract content by title level."""
    lines = text.split("\n")
    result_lines = []
    capturing = False
    
    for line in lines:
        stripped = line.strip()
        
        if any(kw in stripped for kw in keywords):
            capturing = True
            result_lines.append(line)
            continue
        
        if capturing:
            if stripped and (stripped[0].isdigit() or stripped.startswith("第")):
                if any(kw in stripped for kw in keywords):
                    continue
                break
            result_lines.append(line)
    
    return "\n".join(result_lines)


def find_section_pages(
    section_id: str,
    page_texts: Dict[int, str],
    page_tables: Dict[int, list],
    zone_markers: List[Tuple[str, str]],
    zone_preferences: Dict[str, Dict[str, List[str]]],
) -> List[Tuple[int, float]]:
    """Find pages that match a section."""
    keywords = SECTION_KEYWORDS.get(section_id, {}).get("include", [])
    if not keywords:
        return []
    
    scores: List[Tuple[int, float]] = []
    page_zones: Dict[int, Optional[str]] = {}
    
    compiled_markers = [(re.compile(pat), zone) for pat, zone in zone_markers]
    
    for page_num, text in page_texts.items():
        if page_num not in page_zones:
            zone = None
            for pattern, z in compiled_markers:
                if pattern.search(text):
                    zone = z
                    break
            page_zones[page_num] = zone
    
    for page_num, text in page_texts.items():
        page_zone = page_zones[page_num]
        
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
    
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:5]


def extract_section_content(
    section_id: str,
    page_texts: Dict[int, str],
    page_tables: Dict[int, list],
    scores: List[Tuple[int, float]],
    total_pages: int,
    extracted_paragraphs: Set[str],
) -> Optional[str]:
    """Extract content for a section."""
    if not scores:
        return None
    
    config = SECTION_EXTRACT_CONFIG.get(section_id, {})
    buffer_pages = config.get("buffer_pages", DEFAULT_BUFFER_PAGES)
    max_chars = config.get("max_chars", DEFAULT_MAX_CHARS)
    
    section_config = SECTION_KEYWORDS.get(section_id, {})
    include_kw = section_config.get("include", [])
    exclude_kw = section_config.get("exclude", [])
    mandatory_kw = section_config.get("mandatory", [])
    
    best_page = scores[0][0]
    parts = []
    processed_pages = set()
    
    for offset in range(-buffer_pages, buffer_pages + 1):
        target = best_page + offset
        if 0 <= target < total_pages and target not in processed_pages:
            raw_text = page_texts.get(target, "")
            if not raw_text:
                continue
            
            paragraphs = raw_text.split("\n")
            valid_paragraphs = []
            
            for para in paragraphs:
                para_stripped = para.strip()
                if not para_stripped or any(kw in para_stripped for kw in exclude_kw):
                    continue
                
                if any(kw in para_stripped for kw in mandatory_kw):
                    valid_paragraphs.append(para)
                elif any(kw in para_stripped for kw in include_kw):
                    valid_paragraphs.append(para)
                else:
                    if para_stripped not in extracted_paragraphs:
                        valid_paragraphs.append(para)
                        extracted_paragraphs.add(para_stripped)
            
            if valid_paragraphs:
                parts.append(f"--- p.{target} ---")
                parts.append("\n".join(valid_paragraphs))
            
            processed_pages.add(target)
    
    combined = "\n\n".join(parts)
    
    if combined:
        combined = _extract_by_title_level(combined, section_id, include_kw)
        
        if len(combined) > max_chars:
            keyword = include_kw[0] if include_kw else ""
            combined = _center_truncate(combined, keyword, max_chars)
    
    if not combined or len(combined.strip()) < 100:
        raw_text = page_texts.get(best_page, "")
        keyword = include_kw[0] if include_kw else ""
        combined = _center_truncate(raw_text, keyword, max_chars)
    
    return combined.strip() if combined else None


def _extract_section_worker(
    section_id: str,
    page_texts: Dict[int, str],
    page_tables: Dict[int, list],
    total_pages: int,
    zone_markers: List[Tuple[str, str]],
    zone_preferences: Dict[str, Dict[str, List[str]]],
) -> ExtractionResult:
    """Worker function for extracting a single section (used in multiprocessing)."""
    start_time = time.time()
    
    try:
        scores = find_section_pages(
            section_id,
            page_texts,
            page_tables,
            zone_markers,
            zone_preferences,
        )
        
        if not scores:
            return ExtractionResult(
                section_id=section_id,
                error="No matching pages found",
                duration_sec=time.time() - start_time,
            )
        
        extracted_paragraphs: Set[str] = set()
        content = extract_section_content(
            section_id,
            page_texts,
            page_tables,
            scores,
            total_pages,
            extracted_paragraphs,
        )
        
        pages = [s[0] for s in scores]
        
        return ExtractionResult(
            section_id=section_id,
            content=content,
            score=scores[0][1] if scores else 0.0,
            pages=pages,
            duration_sec=time.time() - start_time,
        )
    
    except Exception as e:
        return ExtractionResult(
            section_id=section_id,
            error=str(e),
            duration_sec=time.time() - start_time,
        )


class ParallelPDFExtractor:
    """Parallel PDF section extractor with caching and incremental extraction."""
    
    def __init__(
        self,
        max_workers: int = 4,
        timeout_sec: int = 600,
        use_cache: bool = True,
        large_file_threshold_mb: float = 100.0,
    ):
        self.max_workers = max_workers
        self.timeout_sec = timeout_sec
        self.use_cache = use_cache
        self.large_file_threshold_mb = large_file_threshold_mb
        
        if _config_available:
            try:
                config = get_config()
                self.max_workers = config.pdf.parallel.max_workers
                self.timeout_sec = config.pdf.timeout_sec
                self.large_file_threshold_mb = config.pdf.large_file_threshold_mb
            except Exception:
                pass
    
    def _check_existing_result(self, output_dir: Path, pdf_path: str) -> Tuple[bool, Optional[Dict]]:
        """Check if existing extraction result can be reused."""
        if not self.use_cache:
            return False, None
        
        json_path = output_dir / JSON_FILENAME
        if not json_path.exists():
            return False, None
        
        try:
            with open(json_path, "r", encoding=DEFAULT_ENCODING) as f:
                existing = json.load(f)
            
            metadata = existing.get("metadata", {})
            stored_hash = metadata.get("file_hash", "")
            current_hash = compute_file_hash(pdf_path)
            
            if stored_hash and stored_hash == current_hash:
                return True, existing
        
        except Exception as e:
            _logger.warning(f"Failed to check existing result: {e}")
        
        return False, None
    
    def _is_large_file(self, pdf_path: str) -> bool:
        """Check if PDF file is large."""
        file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
        return file_size_mb > self.large_file_threshold_mb
    
    def extract(
        self,
        pdf_path: str,
        output_dir: Path,
        sections: Optional[List[str]] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Extract sections from PDF file.
        
        Args:
            pdf_path: Path to PDF file
            output_dir: Output directory for results
            sections: List of section IDs to extract (default: all)
            force: Force re-extraction even if cache exists
        
        Returns:
            Dictionary with metadata and extracted sections
        """
        start_time = time.time()
        
        if not os.path.exists(pdf_path):
            return {"error": f"PDF file not found: {pdf_path}"}
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if not force:
            can_reuse, existing = self._check_existing_result(output_dir, pdf_path)
            if can_reuse and existing:
                _logger.info("Reusing cached extraction result")
                existing["metadata"]["from_cache"] = True
                return existing
        
        if self._is_large_file(pdf_path):
            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
            _logger.warning(f"Large PDF detected ({file_size_mb:.1f}MB), extraction may take longer")
        
        page_texts, page_tables, garbled_ratio, used_fallback = extract_all_pages(pdf_path)
        total_pages = len(page_texts)
        
        if total_pages == 0:
            return {"error": "No pages extracted from PDF"}
        
        metadata = PDFMetadata(
            pdf_path=pdf_path,
            total_pages=total_pages,
            file_size_mb=os.path.getsize(pdf_path) / (1024 * 1024),
            file_hash=compute_file_hash(pdf_path),
            garbled_ratio=garbled_ratio,
            used_fallback=used_fallback,
            extraction_time=datetime.now().isoformat(),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        )
        
        target_sections = sections or EXTRACT_PRIORITY
        results: Dict[str, ExtractionResult] = {}
        
        if self.max_workers > 1 and len(target_sections) > 1:
            results = self._extract_parallel(
                target_sections,
                page_texts,
                page_tables,
                total_pages,
            )
        else:
            results = self._extract_sequential(
                target_sections,
                page_texts,
                page_tables,
                total_pages,
            )
        
        sections_output: Dict[str, Optional[str]] = {}
        for section_id in SECTION_ORDER:
            if section_id in results:
                sections_output[section_id] = results[section_id].content
            else:
                sections_output[section_id] = None
        
        output_data = {
            "metadata": {
                "pdf_path": metadata.pdf_path,
                "total_pages": metadata.total_pages,
                "file_size_mb": round(metadata.file_size_mb, 2),
                "file_hash": metadata.file_hash,
                "garbled_ratio": f"{metadata.garbled_ratio:.1%}",
                "used_fallback": metadata.used_fallback,
                "extraction_time": metadata.extraction_time,
                "python_version": metadata.python_version,
                "extraction_mode": "parallel" if self.max_workers > 1 else "sequential",
                "max_workers": self.max_workers,
                "total_duration_sec": round(time.time() - start_time, 2),
            },
            "sections": sections_output,
        }
        
        self._write_output(output_data, output_dir)
        
        return output_data
    
    def _extract_parallel(
        self,
        sections: List[str],
        page_texts: Dict[int, str],
        page_tables: Dict[int, list],
        total_pages: int,
    ) -> Dict[str, ExtractionResult]:
        """Extract sections in parallel using multiprocessing."""
        results: Dict[str, ExtractionResult] = {}
        
        chunk_size = max(1, len(sections) // self.max_workers)
        section_chunks = [
            sections[i:i + chunk_size]
            for i in range(0, len(sections), chunk_size)
        ]
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            
            for chunk in section_chunks:
                for section_id in chunk:
                    future = executor.submit(
                        _extract_section_worker,
                        section_id,
                        page_texts,
                        page_tables,
                        total_pages,
                        ZONE_MARKERS,
                        SECTION_ZONE_PREFERENCES,
                    )
                    futures[future] = section_id
            
            for future in as_completed(futures, timeout=self.timeout_sec):
                section_id = futures[future]
                try:
                    result = future.result(timeout=60)
                    results[section_id] = result
                    if result.content:
                        _logger.info(f"Extracted {section_id}: {len(result.content)} chars")
                except Exception as e:
                    results[section_id] = ExtractionResult(
                        section_id=section_id,
                        error=str(e),
                    )
                    _logger.error(f"Failed to extract {section_id}: {e}")
        
        return results
    
    def _extract_sequential(
        self,
        sections: List[str],
        page_texts: Dict[int, str],
        page_tables: Dict[int, list],
        total_pages: int,
    ) -> Dict[str, ExtractionResult]:
        """Extract sections sequentially."""
        results: Dict[str, ExtractionResult] = {}
        extracted_paragraphs: Set[str] = set()
        
        for section_id in sections:
            start_time = time.time()
            
            try:
                scores = find_section_pages(
                    section_id,
                    page_texts,
                    page_tables,
                    ZONE_MARKERS,
                    SECTION_ZONE_PREFERENCES,
                )
                
                if scores:
                    content = extract_section_content(
                        section_id,
                        page_texts,
                        page_tables,
                        scores,
                        total_pages,
                        extracted_paragraphs,
                    )
                    
                    results[section_id] = ExtractionResult(
                        section_id=section_id,
                        content=content,
                        score=scores[0][1],
                        pages=[s[0] for s in scores],
                        duration_sec=time.time() - start_time,
                    )
                else:
                    results[section_id] = ExtractionResult(
                        section_id=section_id,
                        error="No matching pages found",
                        duration_sec=time.time() - start_time,
                    )
            
            except Exception as e:
                results[section_id] = ExtractionResult(
                    section_id=section_id,
                    error=str(e),
                    duration_sec=time.time() - start_time,
                )
        
        return results
    
    def _write_output(self, data: Dict[str, Any], output_dir: Path) -> Tuple[str, str]:
        """Write extraction results to output files."""
        json_path = output_dir / JSON_FILENAME
        md_path = output_dir / MD_FILENAME
        
        with open(json_path, 'w', encoding=DEFAULT_ENCODING) as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        metadata = data.get("metadata", {})
        md_content = f"# PDF 财报提取内容\n\n"
        md_content += f"## 元数据\n"
        for key, value in metadata.items():
            md_content += f"- **{key}**: {value}\n"
        md_content += "\n"
        
        sections = data.get("sections", {})
        for section_id in SECTION_ORDER:
            content = sections.get(section_id)
            if content:
                section_name = section_id.replace("_", " ").title()
                md_content += f"## {section_name} ({section_id})\n"
                md_content += f"\n{content}\n\n"
        
        with open(md_path, 'w', encoding=DEFAULT_ENCODING) as f:
            f.write(md_content)
        
        return str(json_path), str(md_path)
    
    def get_extraction_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Get summary of extraction results."""
        sections = data.get("sections", {})
        metadata = data.get("metadata", {})
        
        found = sum(1 for v in sections.values() if v)
        total = len(sections)
        
        return {
            "total_sections": total,
            "found_sections": found,
            "missing_sections": [k for k, v in sections.items() if not v],
            "completeness": f"{found / total * 100:.1f}%" if total > 0 else "0%",
            "total_pages": metadata.get("total_pages", 0),
            "file_size_mb": metadata.get("file_size_mb", 0),
            "extraction_mode": metadata.get("extraction_mode", "unknown"),
            "total_duration_sec": metadata.get("total_duration_sec", 0),
        }


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Parallel PDF Section Extractor')
    parser.add_argument('--pdf', required=True, help='Path to PDF file')
    parser.add_argument('--output', required=True, help='Output directory')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--timeout', type=int, default=600, help='Timeout in seconds')
    parser.add_argument('--force', action='store_true', help='Force re-extraction')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    extractor = ParallelPDFExtractor(
        max_workers=args.workers,
        timeout_sec=args.timeout,
    )
    
    result = extractor.extract(
        pdf_path=args.pdf,
        output_dir=Path(args.output),
        force=args.force,
    )
    
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    
    summary = extractor.get_extraction_summary(result)
    print(f"\n{'='*50}")
    print(f"PDF Extraction Complete")
    print(f"{'='*50}")
    print(f"Sections found: {summary['found_sections']}/{summary['total_sections']} ({summary['completeness']})")
    print(f"Total pages: {summary['total_pages']}")
    print(f"Duration: {summary['total_duration_sec']:.1f}s")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
