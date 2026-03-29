#!/usr/bin/env python3
"""Configuration loader for Turtle Investment Framework.

Loads configuration from:
1. config.yaml (base configuration)
2. Environment variables (override)
3. Command-line arguments (highest priority)

Usage:
    from config_loader import get_config
    
    config = get_config()
    timeout = config.tushare.timeout
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None

_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 10.0
    exponential_base: float = 2.0


@dataclass
class CacheConfig:
    enabled: bool = True
    ttl_days: int = 7


@dataclass
class TushareConfig:
    timeout: int = 30
    retry: RetryConfig = field(default_factory=RetryConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    rate_limit_delay: float = 0.5


@dataclass
class ParallelConfig:
    enabled: bool = True
    max_workers: int = 4
    chunk_size: int = 3


@dataclass
class PDFConfig:
    max_file_size_mb: int = 200
    timeout_sec: int = 600
    extract_mode: str = "full"
    parallel: ParallelConfig = field(default_factory=ParallelConfig)
    large_file_threshold_mb: int = 100
    large_file_options: List[str] = field(default_factory=lambda: ["full", "core", "custom"])
    fallback_encoding: str = "utf-8"


@dataclass
class WebSearchConfig:
    timeout_sec: int = 300
    cache_days: int = 7
    max_results_per_query: int = 10
    trusted_sources: List[str] = field(default_factory=list)


@dataclass
class ValidationConfig:
    tolerance_thresholds: Dict[str, float] = field(default_factory=dict)
    completeness_thresholds: Dict[str, float] = field(default_factory=lambda: {"full": 0.9, "partial": 0.7})


@dataclass
class TaskTimeoutConfig:
    pdf_extract: int = 600
    tushare_collect: int = 300
    web_search: int = 300
    report_generate: int = 600


@dataclass
class SchedulerConfig:
    parallel_execution: bool = True
    max_concurrent_tasks: int = 5
    task_timeout: TaskTimeoutConfig = field(default_factory=TaskTimeoutConfig)


@dataclass
class ResourceLimitsConfig:
    max_cpu: int = 2
    max_memory_gb: int = 2


@dataclass
class AutoPauseConfig:
    enabled: bool = True
    cpu_threshold: int = 90
    mem_threshold: int = 85


@dataclass
class BatchAnalysisConfig:
    max_concurrent: int = 5


@dataclass
class ResourceConfig:
    pdf_extract: ResourceLimitsConfig = field(default_factory=ResourceLimitsConfig)
    batch_analysis: BatchAnalysisConfig = field(default_factory=BatchAnalysisConfig)
    auto_pause: AutoPauseConfig = field(default_factory=AutoPauseConfig)


@dataclass
class OutputConfig:
    formats: List[str] = field(default_factory=lambda: ["md", "xlsx"])
    include_summary: bool = True
    include_source_appendix: bool = True
    include_validation_report: bool = True
    timestamp_in_filename: bool = True


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s | %(task_id)s | %(module)s | %(levelname)s | %(message)s"
    file_enabled: bool = True
    console_enabled: bool = True
    max_file_size_mb: int = 10
    backup_count: int = 5


@dataclass
class CheckpointConfig:
    enabled: bool = True
    auto_save_interval_sec: int = 30
    resume_on_failure: bool = True
    max_retry_attempts: int = 3


@dataclass
class DataSourceEntry:
    name: str
    priority: int = 1
    enabled: bool = True


@dataclass
class DataSourcesConfig:
    financial_data: List[DataSourceEntry] = field(default_factory=list)
    company_info: List[DataSourceEntry] = field(default_factory=list)
    market_data: List[DataSourceEntry] = field(default_factory=list)


@dataclass
class WorkspaceConfig:
    root: str = "."
    output_dir: str = "output"
    cache_dir: str = ".cache"


@dataclass
class AppConfig:
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    tushare: TushareConfig = field(default_factory=TushareConfig)
    pdf: PDFConfig = field(default_factory=PDFConfig)
    web_search: WebSearchConfig = field(default_factory=WebSearchConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    resource: ResourceConfig = field(default_factory=ResourceConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    data_sources: DataSourcesConfig = field(default_factory=DataSourcesConfig)


def _deep_get(d: Dict, *keys, default=None) -> Any:
    """Safely get nested dictionary value."""
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return default
    return d


def _parse_retry_config(data: Dict) -> RetryConfig:
    return RetryConfig(
        max_retries=_deep_get(data, "max_retries", default=3),
        base_delay=_deep_get(data, "base_delay", default=1.0),
        max_delay=_deep_get(data, "max_delay", default=10.0),
        exponential_base=_deep_get(data, "exponential_base", default=2.0),
    )


def _parse_cache_config(data: Dict) -> CacheConfig:
    return CacheConfig(
        enabled=_deep_get(data, "enabled", default=True),
        ttl_days=_deep_get(data, "ttl_days", default=7),
    )


def _parse_tushare_config(data: Dict) -> TushareConfig:
    return TushareConfig(
        timeout=_deep_get(data, "timeout", default=30),
        retry=_parse_retry_config(_deep_get(data, "retry", default={})),
        cache=_parse_cache_config(_deep_get(data, "cache", default={})),
        rate_limit_delay=_deep_get(data, "rate_limit", "delay_between_calls", default=0.5),
    )


def _parse_parallel_config(data: Dict) -> ParallelConfig:
    return ParallelConfig(
        enabled=_deep_get(data, "enabled", default=True),
        max_workers=_deep_get(data, "max_workers", default=4),
        chunk_size=_deep_get(data, "chunk_size", default=3),
    )


def _parse_pdf_config(data: Dict) -> PDFConfig:
    return PDFConfig(
        max_file_size_mb=_deep_get(data, "max_file_size_mb", default=200),
        timeout_sec=_deep_get(data, "timeout_sec", default=600),
        extract_mode=_deep_get(data, "extract_mode", default="full"),
        parallel=_parse_parallel_config(_deep_get(data, "parallel", default={})),
        large_file_threshold_mb=_deep_get(data, "large_file_threshold_mb", default=100),
        large_file_options=_deep_get(data, "large_file_options", default=["full", "core", "custom"]),
        fallback_encoding=_deep_get(data, "fallback_encoding", default="utf-8"),
    )


def _parse_web_search_config(data: Dict) -> WebSearchConfig:
    return WebSearchConfig(
        timeout_sec=_deep_get(data, "timeout_sec", default=300),
        cache_days=_deep_get(data, "cache_days", default=7),
        max_results_per_query=_deep_get(data, "max_results_per_query", default=10),
        trusted_sources=_deep_get(data, "trusted_sources", default=[]),
    )


def _parse_validation_config(data: Dict) -> ValidationConfig:
    return ValidationConfig(
        tolerance_thresholds=_deep_get(data, "tolerance_thresholds", default={}),
        completeness_thresholds=_deep_get(data, "completeness_thresholds", default={"full": 0.9, "partial": 0.7}),
    )


def _parse_task_timeout_config(data: Dict) -> TaskTimeoutConfig:
    return TaskTimeoutConfig(
        pdf_extract=_deep_get(data, "pdf_extract", default=600),
        tushare_collect=_deep_get(data, "tushare_collect", default=300),
        web_search=_deep_get(data, "web_search", default=300),
        report_generate=_deep_get(data, "report_generate", default=600),
    )


def _parse_scheduler_config(data: Dict) -> SchedulerConfig:
    return SchedulerConfig(
        parallel_execution=_deep_get(data, "parallel_execution", default=True),
        max_concurrent_tasks=_deep_get(data, "max_concurrent_tasks", default=5),
        task_timeout=_parse_task_timeout_config(_deep_get(data, "task_timeout", default={})),
    )


def _parse_resource_config(data: Dict) -> ResourceConfig:
    pdf_data = _deep_get(data, "pdf_extract", default={})
    batch_data = _deep_get(data, "batch_analysis", default={})
    auto_pause_data = _deep_get(data, "auto_pause", default={})
    
    return ResourceConfig(
        pdf_extract=ResourceLimitsConfig(
            max_cpu=_deep_get(pdf_data, "max_cpu", default=2),
            max_memory_gb=_deep_get(pdf_data, "max_memory_gb", default=2),
        ),
        batch_analysis=BatchAnalysisConfig(
            max_concurrent=_deep_get(batch_data, "max_concurrent", default=5),
        ),
        auto_pause=AutoPauseConfig(
            enabled=_deep_get(auto_pause_data, "enabled", default=True),
            cpu_threshold=_deep_get(auto_pause_data, "cpu_threshold", default=90),
            mem_threshold=_deep_get(auto_pause_data, "mem_threshold", default=85),
        ),
    )


def _parse_output_config(data: Dict) -> OutputConfig:
    return OutputConfig(
        formats=_deep_get(data, "formats", default=["md", "xlsx"]),
        include_summary=_deep_get(data, "include_summary", default=True),
        include_source_appendix=_deep_get(data, "include_source_appendix", default=True),
        include_validation_report=_deep_get(data, "include_validation_report", default=True),
        timestamp_in_filename=_deep_get(data, "timestamp_in_filename", default=True),
    )


def _parse_logging_config(data: Dict) -> LoggingConfig:
    return LoggingConfig(
        level=_deep_get(data, "level", default="INFO"),
        format=_deep_get(data, "format", default="%(asctime)s | %(task_id)s | %(module)s | %(levelname)s | %(message)s"),
        file_enabled=_deep_get(data, "file_enabled", default=True),
        console_enabled=_deep_get(data, "console_enabled", default=True),
        max_file_size_mb=_deep_get(data, "max_file_size_mb", default=10),
        backup_count=_deep_get(data, "backup_count", default=5),
    )


def _parse_checkpoint_config(data: Dict) -> CheckpointConfig:
    return CheckpointConfig(
        enabled=_deep_get(data, "enabled", default=True),
        auto_save_interval_sec=_deep_get(data, "auto_save_interval_sec", default=30),
        resume_on_failure=_deep_get(data, "resume_on_failure", default=True),
        max_retry_attempts=_deep_get(data, "max_retry_attempts", default=3),
    )


def _parse_data_source_entry(data: Dict) -> DataSourceEntry:
    return DataSourceEntry(
        name=_deep_get(data, "name", default=""),
        priority=_deep_get(data, "priority", default=1),
        enabled=_deep_get(data, "enabled", default=True),
    )


def _parse_data_sources_config(data: Dict) -> DataSourcesConfig:
    def parse_list(items: List) -> List[DataSourceEntry]:
        return [_parse_data_source_entry(item) for item in items if isinstance(item, dict)]
    
    return DataSourcesConfig(
        financial_data=parse_list(_deep_get(data, "financial_data", default=[])),
        company_info=parse_list(_deep_get(data, "company_info", default=[])),
        market_data=parse_list(_deep_get(data, "market_data", default=[])),
    )


def _parse_workspace_config(data: Dict) -> WorkspaceConfig:
    return WorkspaceConfig(
        root=_deep_get(data, "root", default="."),
        output_dir=_deep_get(data, "output_dir", default="output"),
        cache_dir=_deep_get(data, "cache_dir", default=".cache"),
    )


def load_yaml_config(config_path: Path) -> Dict:
    """Load configuration from YAML file."""
    if yaml is None:
        print("[WARN] PyYAML not installed, using default configuration")
        return {}
    
    if not config_path.exists():
        print(f"[WARN] Config file not found: {config_path}, using defaults")
        return {}
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"[ERROR] Failed to load config: {e}")
        return {}


def parse_config(data: Dict) -> AppConfig:
    """Parse raw configuration dict into typed config object."""
    return AppConfig(
        workspace=_parse_workspace_config(data.get("workspace", {})),
        tushare=_parse_tushare_config(data.get("tushare", {})),
        pdf=_parse_pdf_config(data.get("pdf", {})),
        web_search=_parse_web_search_config(data.get("web_search", {})),
        validation=_parse_validation_config(data.get("validation", {})),
        scheduler=_parse_scheduler_config(data.get("scheduler", {})),
        resource=_parse_resource_config(data.get("resource_limits", {})),
        output=_parse_output_config(data.get("output", {})),
        logging=_parse_logging_config(data.get("logging", {})),
        checkpoint=_parse_checkpoint_config(data.get("checkpoint", {})),
        data_sources=_parse_data_sources_config(data.get("data_sources", {})),
    )


def apply_env_overrides(config: AppConfig) -> AppConfig:
    """Apply environment variable overrides to configuration."""
    
    if os.environ.get("TUSHARE_TIMEOUT"):
        try:
            config.tushare.timeout = int(os.environ["TUSHARE_TIMEOUT"])
        except ValueError:
            pass
    
    if os.environ.get("TUSHARE_MAX_RETRIES"):
        try:
            config.tushare.retry.max_retries = int(os.environ["TUSHARE_MAX_RETRIES"])
        except ValueError:
            pass
    
    if os.environ.get("PDF_TIMEOUT"):
        try:
            config.pdf.timeout_sec = int(os.environ["PDF_TIMEOUT"])
        except ValueError:
            pass
    
    if os.environ.get("PDF_MAX_WORKERS"):
        try:
            config.pdf.parallel.max_workers = int(os.environ["PDF_MAX_WORKERS"])
        except ValueError:
            pass
    
    if os.environ.get("LOG_LEVEL"):
        config.logging.level = os.environ["LOG_LEVEL"].upper()
    
    if os.environ.get("PARALLEL_ENABLED"):
        config.pdf.parallel.enabled = os.environ["PARALLEL_ENABLED"].lower() in ("true", "1", "yes")
    
    return config


_config_instance: Optional[AppConfig] = None


def get_config(config_path: Optional[Path] = None, reload: bool = False) -> AppConfig:
    """Get application configuration (singleton pattern).
    
    Args:
        config_path: Path to config.yaml file
        reload: Force reload configuration
    
    Returns:
        AppConfig instance
    """
    global _config_instance
    
    if _config_instance is not None and not reload:
        return _config_instance
    
    path = config_path or _DEFAULT_CONFIG_PATH
    
    raw_config = load_yaml_config(path)
    config = parse_config(raw_config)
    config = apply_env_overrides(config)
    
    _config_instance = config
    return config


def get_project_root() -> Path:
    """Get project root directory."""
    return _PROJECT_ROOT


def get_output_dir() -> Path:
    """Get output directory path."""
    config = get_config()
    root = Path(config.workspace.root)
    if not root.is_absolute():
        root = _PROJECT_ROOT / root
    return root / config.workspace.output_dir


def get_cache_dir() -> Path:
    """Get cache directory path."""
    config = get_config()
    root = Path(config.workspace.root)
    if not root.is_absolute():
        root = _PROJECT_ROOT / root
    return root / config.workspace.cache_dir
