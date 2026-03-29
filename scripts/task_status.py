#!/usr/bin/env python3
"""Task Status Manager for Turtle Investment Framework.

Provides:
1. Task status tracking with subtask granularity
2. Checkpoint/resume capability
3. Execution metrics collection

Usage:
    from task_status import TaskStatusManager, SubtaskStatus
    
    manager = TaskStatusManager(output_dir)
    manager.start_subtask("pdf_extract")
    # ... do work ...
    manager.complete_subtask("pdf_extract", output_file="pdf_sections.json")
    
    # Resume from checkpoint
    if manager.can_resume():
        manager.resume()
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class SubtaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class TaskPriority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class SubtaskResult:
    status: SubtaskStatus = SubtaskStatus.PENDING
    output_file: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    retry_count: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_sec: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SubtaskResult":
        if "status" in data and isinstance(data["status"], str):
            data["status"] = SubtaskStatus(data["status"])
        return cls(**data)


@dataclass
class TaskCheckpoint:
    task_id: str
    ts_code: str
    company_name: str
    year: int
    period: str
    channel: str
    created_at: str
    updated_at: str
    subtasks: Dict[str, SubtaskResult] = field(default_factory=dict)
    version: str = "2.1"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "ts_code": self.ts_code,
            "company_name": self.company_name,
            "year": self.year,
            "period": self.period,
            "channel": self.channel,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "subtasks": {k: v.to_dict() for k, v in self.subtasks.items()},
            "version": self.version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskCheckpoint":
        subtasks = {}
        for k, v in data.get("subtasks", {}).items():
            subtasks[k] = SubtaskResult.from_dict(v)
        
        return cls(
            task_id=data.get("task_id", ""),
            ts_code=data.get("ts_code", ""),
            company_name=data.get("company_name", ""),
            year=data.get("year", 0),
            period=data.get("period", "年报"),
            channel=data.get("channel", "direct"),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            subtasks=subtasks,
            version=data.get("version", "2.1"),
        )


SUBTASK_DEFINITIONS = {
    "pdf_extract": {
        "display_name": "PDF年报提取",
        "phase": 0,
        "priority": TaskPriority.HIGH,
        "dependencies": [],
        "output_files": ["pdf_sections.json"],
        "timeout_sec": 600,
    },
    "tushare_collect": {
        "display_name": "Tushare数据采集",
        "phase": 0,
        "priority": TaskPriority.HIGH,
        "dependencies": [],
        "output_files": ["data_pack_market.md"],
        "timeout_sec": 300,
    },
    "web_search": {
        "display_name": "网络搜索",
        "phase": 1,
        "priority": TaskPriority.MEDIUM,
        "dependencies": [],
        "output_files": ["web_search_result.md"],
        "timeout_sec": 300,
    },
    "report_generate": {
        "display_name": "报告生成",
        "phase": 2,
        "priority": TaskPriority.HIGH,
        "dependencies": ["pdf_extract", "tushare_collect", "web_search"],
        "output_files": ["{code}_{year}_{company}_分析报告.md"],
        "timeout_sec": 600,
    },
}


class TaskStatusManager:
    """Manages task execution status and checkpoint/resume capability."""
    
    STATUS_FILENAME = "analysis_status.json"
    
    def __init__(
        self,
        output_dir: Path,
        ts_code: str = "",
        company_name: str = "",
        year: int = 0,
        period: str = "年报",
        channel: str = "direct",
    ):
        self.output_dir = Path(output_dir)
        self.status_file = self.output_dir / self.STATUS_FILENAME
        self.checkpoint: Optional[TaskCheckpoint] = None
        
        if ts_code:
            self._init_checkpoint(ts_code, company_name, year, period, channel)
        else:
            self._load_checkpoint()
    
    def _generate_task_id(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        short_uuid = uuid.uuid4().hex[:8]
        return f"task_{timestamp}_{short_uuid}"
    
    def _init_checkpoint(
        self,
        ts_code: str,
        company_name: str,
        year: int,
        period: str,
        channel: str,
    ) -> None:
        now = datetime.now().isoformat()
        self.checkpoint = TaskCheckpoint(
            task_id=self._generate_task_id(),
            ts_code=ts_code,
            company_name=company_name,
            year=year,
            period=period,
            channel=channel,
            created_at=now,
            updated_at=now,
        )
        self._init_subtasks()
    
    def _init_subtasks(self) -> None:
        if not self.checkpoint:
            return
        
        for subtask_id in SUBTASK_DEFINITIONS:
            if subtask_id not in self.checkpoint.subtasks:
                self.checkpoint.subtasks[subtask_id] = SubtaskResult()
    
    def _load_checkpoint(self) -> None:
        if not self.status_file.exists():
            return
        
        try:
            with open(self.status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.checkpoint = TaskCheckpoint.from_dict(data)
        except Exception as e:
            print(f"[WARN] Failed to load checkpoint: {e}")
            self.checkpoint = None
    
    def save_checkpoint(self) -> None:
        if not self.checkpoint:
            return
        
        self.checkpoint.updated_at = datetime.now().isoformat()
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.status_file, "w", encoding="utf-8") as f:
            json.dump(self.checkpoint.to_dict(), f, ensure_ascii=False, indent=2)
    
    def start_subtask(
        self,
        subtask_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.checkpoint:
            return False
        
        if subtask_id not in self.checkpoint.subtasks:
            self.checkpoint.subtasks[subtask_id] = SubtaskResult()
        
        subtask = self.checkpoint.subtasks[subtask_id]
        
        if subtask.status == SubtaskStatus.COMPLETED:
            return False
        
        subtask.status = SubtaskStatus.RUNNING
        subtask.started_at = datetime.now().isoformat()
        if metadata:
            subtask.metadata.update(metadata)
        
        self.save_checkpoint()
        return True
    
    def complete_subtask(
        self,
        subtask_id: str,
        output_file: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.checkpoint:
            return False
        
        if subtask_id not in self.checkpoint.subtasks:
            return False
        
        subtask = self.checkpoint.subtasks[subtask_id]
        subtask.status = SubtaskStatus.COMPLETED
        subtask.completed_at = datetime.now().isoformat()
        
        if subtask.started_at:
            start = datetime.fromisoformat(subtask.started_at)
            end = datetime.fromisoformat(subtask.completed_at)
            subtask.duration_sec = (end - start).total_seconds()
        
        if output_file:
            subtask.output_file = output_file
        if metadata:
            subtask.metadata.update(metadata)
        
        self.save_checkpoint()
        return True
    
    def fail_subtask(
        self,
        subtask_id: str,
        error_message: str,
        error_type: Optional[str] = None,
        increment_retry: bool = True,
    ) -> bool:
        if not self.checkpoint:
            return False
        
        if subtask_id not in self.checkpoint.subtasks:
            return False
        
        subtask = self.checkpoint.subtasks[subtask_id]
        subtask.status = SubtaskStatus.FAILED
        subtask.error_message = error_message
        subtask.error_type = error_type
        subtask.completed_at = datetime.now().isoformat()
        
        if subtask.started_at:
            start = datetime.fromisoformat(subtask.started_at)
            end = datetime.fromisoformat(subtask.completed_at)
            subtask.duration_sec = (end - start).total_seconds()
        
        if increment_retry:
            subtask.retry_count += 1
        
        self.save_checkpoint()
        return True
    
    def skip_subtask(
        self,
        subtask_id: str,
        reason: str,
    ) -> bool:
        if not self.checkpoint:
            return False
        
        if subtask_id not in self.checkpoint.subtasks:
            return False
        
        subtask = self.checkpoint.subtasks[subtask_id]
        subtask.status = SubtaskStatus.SKIPPED
        subtask.error_message = reason
        subtask.completed_at = datetime.now().isoformat()
        
        self.save_checkpoint()
        return True
    
    def get_subtask_status(self, subtask_id: str) -> Optional[SubtaskStatus]:
        if not self.checkpoint:
            return None
        
        subtask = self.checkpoint.subtasks.get(subtask_id)
        return subtask.status if subtask else None
    
    def get_pending_subtasks(self) -> List[str]:
        if not self.checkpoint:
            return []
        
        pending = []
        for subtask_id, subtask in self.checkpoint.subtasks.items():
            if subtask.status in (SubtaskStatus.PENDING, SubtaskStatus.FAILED):
                pending.append(subtask_id)
        
        return pending
    
    def get_completed_subtasks(self) -> List[str]:
        if not self.checkpoint:
            return []
        
        return [
            subtask_id
            for subtask_id, subtask in self.checkpoint.subtasks.items()
            if subtask.status == SubtaskStatus.COMPLETED
        ]
    
    def can_resume(self) -> bool:
        if not self.checkpoint:
            return False
        
        pending = self.get_pending_subtasks()
        return len(pending) > 0
    
    def get_resumable_subtasks(self) -> List[str]:
        if not self.checkpoint:
            return []
        
        resumable = []
        for subtask_id in SUBTASK_DEFINITIONS:
            subtask = self.checkpoint.subtasks.get(subtask_id)
            if not subtask:
                continue
            
            if subtask.status == SubtaskStatus.COMPLETED:
                continue
            
            if subtask.status == SubtaskStatus.FAILED:
                if subtask.retry_count < 3:
                    resumable.append(subtask_id)
                continue
            
            if subtask.status in (SubtaskStatus.PENDING, SubtaskStatus.RUNNING):
                resumable.append(subtask_id)
        
        return resumable
    
    def get_next_subtask(self) -> Optional[str]:
        resumable = self.get_resumable_subtasks()
        if not resumable:
            return None
        
        for subtask_id in resumable:
            definition = SUBTASK_DEFINITIONS.get(subtask_id, {})
            dependencies = definition.get("dependencies", [])
            
            all_deps_completed = all(
                self.get_subtask_status(dep) == SubtaskStatus.COMPLETED
                for dep in dependencies
            )
            
            if all_deps_completed:
                return subtask_id
        
        return None
    
    def get_progress(self) -> Dict[str, Any]:
        if not self.checkpoint:
            return {"total": 0, "completed": 0, "percentage": 0}
        
        total = len(SUBTASK_DEFINITIONS)
        completed = len(self.get_completed_subtasks())
        
        return {
            "total": total,
            "completed": completed,
            "pending": len(self.get_pending_subtasks()),
            "percentage": round(completed / total * 100, 1) if total > 0 else 0,
            "task_id": self.checkpoint.task_id,
            "status": self._get_overall_status(),
        }
    
    def _get_overall_status(self) -> str:
        if not self.checkpoint:
            return "unknown"
        
        statuses = [s.status for s in self.checkpoint.subtasks.values()]
        
        if all(s == SubtaskStatus.COMPLETED for s in statuses):
            return "completed"
        if any(s == SubtaskStatus.RUNNING for s in statuses):
            return "running"
        if any(s == SubtaskStatus.FAILED for s in statuses):
            return "failed"
        return "pending"
    
    def get_execution_summary(self) -> Dict[str, Any]:
        if not self.checkpoint:
            return {}
        
        summary = {
            "task_id": self.checkpoint.task_id,
            "ts_code": self.checkpoint.ts_code,
            "company_name": self.checkpoint.company_name,
            "year": self.checkpoint.year,
            "period": self.checkpoint.period,
            "created_at": self.checkpoint.created_at,
            "updated_at": self.checkpoint.updated_at,
            "overall_status": self._get_overall_status(),
            "subtasks": {},
        }
        
        total_duration = 0.0
        for subtask_id, subtask in self.checkpoint.subtasks.items():
            definition = SUBTASK_DEFINITIONS.get(subtask_id, {})
            summary["subtasks"][subtask_id] = {
                "display_name": definition.get("display_name", subtask_id),
                "status": subtask.status.value,
                "duration_sec": subtask.duration_sec,
                "output_file": subtask.output_file,
                "error_message": subtask.error_message,
                "retry_count": subtask.retry_count,
            }
            if subtask.duration_sec:
                total_duration += subtask.duration_sec
        
        summary["total_duration_sec"] = total_duration
        summary["total_duration_min"] = round(total_duration / 60, 1)
        
        return summary
    
    def print_progress(self) -> None:
        progress = self.get_progress()
        
        print(f"\n{'='*50}")
        print(f"📊 任务进度: {progress['task_id']}")
        print(f"{'='*50}")
        print(f"状态: {progress['status']}")
        print(f"进度: {progress['completed']}/{progress['total']} ({progress['percentage']}%)")
        print()
        
        if self.checkpoint:
            for subtask_id, subtask in self.checkpoint.subtasks.items():
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


def create_status_manager(
    output_dir: Path,
    ts_code: str,
    company_name: str,
    year: int,
    period: str = "年报",
    channel: str = "direct",
) -> TaskStatusManager:
    """Factory function to create a TaskStatusManager."""
    return TaskStatusManager(
        output_dir=output_dir,
        ts_code=ts_code,
        company_name=company_name,
        year=year,
        period=period,
        channel=channel,
    )


def load_status_manager(output_dir: Path) -> Optional[TaskStatusManager]:
    """Load existing TaskStatusManager from output directory."""
    manager = TaskStatusManager(output_dir)
    return manager if manager.checkpoint else None
