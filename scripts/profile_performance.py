#!/usr/bin/env python3
"""Performance analysis script for PDF preprocessor."""

import time
import sys
sys.path.insert(0, 'scripts')

from pdf_preprocessor import run_pipeline
from pdf_processor import extract_all_pages


def test_extraction_performance():
    """Test PDF extraction performance."""
    pdf_path = '../output/600989SH_宝丰能源/2025_年报/600989_2025_宝丰能源_年报.pdf'
    output_path = '../output/test_perf'
    
    print(f"Testing PDF extraction for: {pdf_path}")
    print("=" * 60)
    
    # Test 1: Raw PDF extraction
    print("\n1. Testing raw PDF extraction...")
    start = time.time()
    page_texts, page_tables, garbled_ratio, used_fallback = extract_all_pages(pdf_path)
    end = time.time()
    print(f"Raw extraction time: {end - start:.2f} seconds")
    print(f"Total pages: {len(page_texts)}")
    print(f"Garbled ratio: {garbled_ratio:.1%}")
    
    # Test 2: Full pipeline
    print("\n2. Testing full pipeline...")
    start = time.time()
    result = run_pipeline(pdf_path, output_path, verbose=False)
    end = time.time()
    
    print(f"\nPipeline Results:")
    print(f"Total processing time: {end - start:.2f} seconds")
    print(f"Total pages: {result.get('metadata', {}).get('total_pages', 0)}")
    print(f"Found sections: {result.get('metadata', {}).get('found_sections', 0)}/{result.get('metadata', {}).get('total_sections', 0)}")


if __name__ == '__main__':
    test_extraction_performance()