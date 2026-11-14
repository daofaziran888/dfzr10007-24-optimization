"""数据采样管理业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services.sample_run import engine, parse_layers
from app.store import store

MODULE = "sample"
REQUIRED_FIELDS = ["配置编号", "数据源名称", "采样算法"]
STATUS_ORDER = ["草稿", "已启用", "已暂停", "已关闭"]
ACTION_RULES = {"启用采样": "草稿", "暂停采样": "已启用", "关闭采样": "已暂停"}
NEGATIVE_ACTIONS = []


def _optional_int(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


class SampleService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        page_rows = [dict(row) for row in rows[start:start + size]]
        for row in page_rows:
            # 清单保留完成位置：刷新页面后仍能看到最近一次运行跑到哪一批。
            progress = engine.progress_text(int(row.get("id", 0)))
            if progress:
                row["完成位置"] = progress
                row["当前版本"] = engine.current_version(int(row.get("id", 0)))
        return page_rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        row = dict(entry)
        progress = engine.progress_text(entry_id)
        if progress:
            row["完成位置"] = progress
            row["当前版本"] = engine.current_version(entry_id)
        return row

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in ["样本比例", "随机种子", "分层字段", "输出表名"]:
            if str(values.get(field) or "").strip():
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采样配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据采样管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采样配置已{action}"

    # ---- 分批采样：整组提交、断点续跑、结果核对 ----
    def submit_run(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """多选分层后整组一次提交：按比例快照切批，逐批写入输出表。"""
        config = store.find(MODULE, entry_id)
        if config is None:
            return None, f"采样配置 {entry_id} 不存在或已归档"
        try:
            layers = parse_layers(values.get("分层字段")) or None
            detail = engine.plan_run(
                config,
                layers=layers,
                source_rows=_optional_int(values.get("源行数")),
            )
            detail = engine.execute_run(
                int(detail["id"]), fail_at=_optional_int(values.get("模拟中断批次"))
            )
        except ValueError as exc:
            return None, str(exc)
        return detail, str(detail.get("消息") or "采样运行已提交")

    def resume_run(self, run_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """重试：只处理未完成批次，沿用每个分层字段保存的随机种子断点。"""
        try:
            detail = engine.resume_run(run_id, fail_at=_optional_int(values.get("模拟中断批次")))
        except KeyError:
            return None, f"采样运行 {run_id} 不存在"
        except ValueError as exc:
            return None, str(exc)
        return detail, str(detail.get("消息") or "采样运行已续跑")

    def list_runs(self, entry_id: int) -> tuple[list[dict[str, Any]] | None, str]:
        if store.find(MODULE, entry_id) is None:
            return None, f"采样配置 {entry_id} 不存在或已归档"
        return engine.runs_for_config(entry_id), ""

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        try:
            return engine.run_detail(run_id)
        except KeyError:
            return None

    def output_overview(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        if store.find(MODULE, entry_id) is None:
            return None, f"采样配置 {entry_id} 不存在或已归档"
        overview = engine.output_overview(entry_id)
        if overview is None:
            return None, "还没有采样运行，先整组提交一个批次组"
        return overview, ""

    def output_rows(
        self,
        entry_id: int,
        *,
        version: int | None = None,
        layer: str | None = None,
        page: int = 1,
        size: int = 200,
    ) -> tuple[list[dict[str, Any]], int]:
        return engine.output_rows(entry_id, version=version, layer=layer, page=page, size=size)

    def update_settings(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """更新样本比例等配置：历史采样结果不覆盖，新样本比例只影响新批次。"""
        config = store.find(MODULE, entry_id)
        if config is None:
            return None, f"采样配置 {entry_id} 不存在或已归档"
        try:
            changed = engine.update_settings(config, values)
        except ValueError as exc:
            return None, str(exc)
        return config, f"采样配置已更新：{'、'.join(changed)}；新参数只影响新批次，历史结果与断点保持不变"
