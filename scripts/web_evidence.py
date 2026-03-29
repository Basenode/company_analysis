from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class EvidenceItem:
    source_level: str
    title: str
    date: str
    url: str
    snippet: str
    caliber: str
    retrieved_at: str
    used_in_conclusion: bool


class _MetaTitleParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_title = False
        self.title = ""
        self.meta: dict[str, str] = {}
        self.first_p = ""
        self._capture_p = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]):
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "title":
            self.in_title = True
            return

        if tag.lower() == "meta":
            name = (attrs_dict.get("name") or attrs_dict.get("property") or "").strip().lower()
            content = (attrs_dict.get("content") or "").strip()
            if name and content and name not in self.meta:
                self.meta[name] = content
            return

        if tag.lower() == "p" and not self.first_p:
            self._capture_p = True

    def handle_endtag(self, tag: str):
        if tag.lower() == "title":
            self.in_title = False
        if tag.lower() == "p":
            self._capture_p = False

    def handle_data(self, data: str):
        txt = (data or "").strip()
        if not txt:
            return
        if self.in_title and not self.title:
            self.title = txt
        if self._capture_p and not self.first_p:
            self.first_p = txt


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


_DATE_META_KEYS = (
    "article:published_time",
    "article:modified_time",
    "og:updated_time",
    "pubdate",
    "publishdate",
    "publish_date",
    "date",
    "dc.date",
    "dc.date.issued",
)


def _extract_date_from_meta(meta: dict[str, str]) -> str:
    for k in _DATE_META_KEYS:
        v = (meta.get(k) or "").strip()
        if not v:
            continue
        m = re.search(r"\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}", v)
        if m:
            return m.group(0).replace(".", "-").replace("/", "-")
        m = re.search(r"\d{4}\d{2}\d{2}", v)
        if m:
            s = m.group(0)
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return ""


def _extract_date_from_html(html: str) -> str:
    m = re.search(r"\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}", html)
    if m:
        return m.group(0).replace(".", "-").replace("/", "-")
    return ""


def fetch_evidence(url: str, source_level: str, caliber: str, timeout_sec: int = 12) -> EvidenceItem:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TurtleInvestmentFramework/1.0; +https://example.invalid)"
    }
    r = requests.get(url, headers=headers, timeout=timeout_sec)
    r.raise_for_status()

    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding
    html = r.text or ""
    p = _MetaTitleParser()
    p.feed(html)

    title = (p.meta.get("og:title") or p.title or "").strip()
    if not title:
        title = url

    date = _extract_date_from_meta(p.meta) or _extract_date_from_html(html)
    if not date:
        date = ""

    snippet = (p.meta.get("description") or p.meta.get("og:description") or p.first_p or "").strip()
    snippet = re.sub(r"\s+", " ", snippet)[:240]

    return EvidenceItem(
        source_level=source_level,
        title=title,
        date=date,
        url=url,
        snippet=snippet,
        caliber=caliber,
        retrieved_at=_now_iso(),
        used_in_conclusion=False,
    )


def _read_urls_file(path: str) -> list[str]:
    urls: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            urls.append(u)
    return urls


def _write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch lightweight web evidence (title+date+snippet).")
    ap.add_argument("--urls", nargs="*", default=[], help="One or more URLs to fetch.")
    ap.add_argument("--urls-file", default="", help="File with one URL per line.")
    ap.add_argument("--output", required=True, help="Output JSON file path.")
    ap.add_argument("--source-level", default="Level C", help="Source level: Level A/B/C/D.")
    ap.add_argument("--caliber", default="", help="Caliber note, e.g. 含税/不含税、地区、频率。")
    args = ap.parse_args()

    urls: list[str] = []
    urls.extend([u for u in (args.urls or []) if u])
    if args.urls_file:
        urls.extend(_read_urls_file(args.urls_file))
    urls = list(dict.fromkeys(urls))

    items: list[EvidenceItem] = []
    errors: list[dict[str, str]] = []

    for url in urls:
        try:
            items.append(fetch_evidence(url=url, source_level=args.source_level, caliber=args.caliber))
        except Exception as e:
            errors.append({"url": url, "error": str(e)})

    payload = {
        "version": 1,
        "retrieved_at": _now_iso(),
        "items": [asdict(x) for x in items],
        "errors": errors,
    }
    _write_json(args.output, payload)
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
