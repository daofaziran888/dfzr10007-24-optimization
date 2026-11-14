"""分批采样引擎：断点续跑、幂等提交、输出表写锁与版本保留。

针对「样本比例较大时每次失败都从第一条重新生成、重复采样把相同记录再次写入
样本输出表」的问题，把一次采样运行按分层字段切成若干批，逐批提交：

- 批次大小：每批 500 行样本；每个分层字段按 计划样本数 = round(源行数 × 样本比例)
  切成若干批，大样本按分层字段批量切分。
- 断点粒度：分层字段级随机种子断点（由配置随机种子确定性派生）+ 批次级完成位置；
  失败后从未完成的分层继续，重试只处理未完成批次并沿用断点种子，已完成的
  分层（如 region）不会被重写。
- 幂等提交：批次只推进一次，已完成的批次重复提交直接跳过，输出表不会出现重复样本。
- 输出表写锁：同一输出表同一时刻只允许一个运行写入，争用时后到者整组不写入。
- 版本保留：每个输出表保留最近 3 个版本的采样结果，历史版本互不覆盖；
  新样本比例只影响新提交的批次，已生成的批次沿用规划时的快照。
"""
from __future__ import annotations

import hashlib
import random
import threading
from datetime import datetime, timezone
from typing import Any, Callable

BATCH_SIZE = 500  # 每批样本行数
SOURCE_ROWS_PER_LAYER = 5000  # 每个分层字段模拟的源表行数（演示环境默认）
VERSION_RETENTION = 3  # 每个输出表保留的采样结果版本数

RUN_QUEUED = "待处理"
RUN_RUNNING = "运行中"
RUN_DONE = "已完成"
RUN_FAILED = "部分失败"

BATCH_PENDING = "待处理"
BATCH_DONE = "已完成"
BATCH_FAILED = "失败"

SETTINGS_FIELDS = ["样本比例", "随机种子", "分层字段", "输出表名"]


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def parse_ratio(raw: Any) -> float:
    """把样本比例解析成 (0, 1] 的小数；支持 0.2、"20%"、"20" 等写法。"""
    text = str(raw or "").strip()
    if not text:
        raise ValueError("样本比例为空，无法规划采样批次")
    try:
        if text.endswith("%"):
            value = float(text[:-1]) / 100
        else:
            value = float(text)
            if value > 1:
                value = value / 100
    except ValueError as exc:
        raise ValueError(f"样本比例「{text}」不是可解析的数字") from exc
    if not 0 < value <= 1:
        raise ValueError(f"样本比例「{text}」超出 (0, 1] 范围")
    return value


def parse_seed(raw: Any) -> int:
    text = str(raw or "").strip()
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"随机种子「{text}」不是整数") from exc


def parse_layers(raw: Any) -> list[str]:
    """分层字段支持列表或逗号分隔字符串，保持顺序去重。"""
    if isinstance(raw, (list, tuple)):
        parts = [str(item).strip() for item in raw]
    else:
        parts = str(raw or "").replace("，", ",").split(",")
    layers: list[str] = []
    for part in parts:
        if part and part not in layers:
            layers.append(part)
    return layers


def derive_layer_seed(base_seed: int, layer: str) -> int:
    """每个分层字段的随机种子断点：由基础种子确定性派生，重试时原样沿用。"""
    digest = hashlib.sha256(f"{base_seed}:{layer}".encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def layer_sample_rows(layer_seed: int, source_rows: int, sample_count: int) -> list[int]:
    """按断点种子确定性抽出一个分层的源行号（升序），重试结果完全一致。"""
    rng = random.Random(layer_seed)
    return sorted(rng.sample(range(1, source_rows + 1), sample_count))


class SampleRunEngine:
    """采样运行引擎：状态全部放在进程内单例里，刷新页面后完成位置不丢。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[int, dict[str, Any]] = {}
        self._batches: dict[int, list[dict[str, Any]]] = {}
        self._outputs: dict[str, list[dict[str, Any]]] = {}
        self._table_locks: dict[str, threading.Lock] = {}
        self._table_owner: dict[str, str] = {}
        self._run_seq = 0
        self._row_seq = 0
        # 测试钩子：提交每批前调用，便于模拟慢写入制造锁争用。
        self.batch_hook: Callable[[dict[str, Any]], None] | None = None

    # ---- 输出表写锁：同一输出表同一时刻只允许一个运行写入 ----
    def acquire_output_lock(self, table: str, owner: str) -> bool:
        with self._lock:
            lock = self._table_locks.setdefault(table, threading.Lock())
            if not lock.acquire(blocking=False):
                return False
            self._table_owner[table] = owner
            return True

    def release_output_lock(self, table: str, owner: str) -> None:
        with self._lock:
            lock = self._table_locks.get(table)
            if lock is not None and self._table_owner.get(table) == owner:
                self._table_owner.pop(table, None)
                lock.release()

    def output_lock_owner(self, table: str) -> str | None:
        return self._table_owner.get(table)

    # ---- 规划：整组一次提交，按比例快照切批 ----
    def plan_run(
        self,
        config: dict[str, Any],
        *,
        layers: list[str] | None = None,
        source_rows: int | None = None,
    ) -> dict[str, Any]:
        ratio = parse_ratio(config.get("样本比例"))
        seed = parse_seed(config.get("随机种子"))
        table = str(config.get("输出表名") or "").strip()
        if not table:
            raise ValueError("输出表名为空，无法规划采样批次")
        config_layers = parse_layers(config.get("分层字段"))
        if not config_layers:
            raise ValueError("分层字段为空，无法按分层切分批采样")
        selected = layers or config_layers
        unknown = [layer for layer in selected if layer not in config_layers]
        if unknown:
            raise ValueError(f"分层字段「{'、'.join(unknown)}」不在采样配置的分层字段里")
        if not selected:
            raise ValueError("请至少选择一个分层字段再整组提交")
        rows_per_layer = int(source_rows or SOURCE_ROWS_PER_LAYER)
        if rows_per_layer <= 0:
            raise ValueError("源行数必须为正整数")

        with self._lock:
            self._run_seq += 1
            run_id = self._run_seq
            version = 1 + max(
                (int(run["版本"]) for run in self._runs.values() if run["输出表名"] == table),
                default=0,
            )
            run = {
                "id": run_id,
                "运行编号": f"RUN-{run_id:04d}",
                "config_id": int(config.get("id", 0)),
                "配置编号": config.get("配置编号"),
                "样本比例": ratio,
                "随机种子": seed,
                "分层字段": list(selected),
                "输出表名": table,
                "源行数": rows_per_layer,
                "版本": version,
                "状态": RUN_QUEUED,
                "结果已清理": False,
                "创建时间": _now(),
                "完成时间": None,
            }
            batches: list[dict[str, Any]] = []
            seq = 0
            for layer in selected:
                sample_count = max(1, round(rows_per_layer * ratio))
                layer_seed = derive_layer_seed(seed, layer)
                layer_batches = max(1, (sample_count + BATCH_SIZE - 1) // BATCH_SIZE)
                for index in range(layer_batches):
                    start = index * BATCH_SIZE
                    end = min(start + BATCH_SIZE, sample_count)
                    seq += 1
                    batches.append({
                        "序号": seq,
                        "分层字段": layer,
                        "层内批次": index + 1,
                        "层内批数": layer_batches,
                        "随机种子断点": layer_seed,
                        "样本起点": start,
                        "样本终点": end,
                        "计划行数": end - start,
                        "已采样行数": 0,
                        "状态": BATCH_PENDING,
                        "写入次数": 0,
                        "完成时间": None,
                        "失败原因": None,
                    })
            self._runs[run_id] = run
            self._batches[run_id] = batches
        return self.run_detail(run_id)

    # ---- 执行：逐批提交，断点续跑 ----
    def execute_run(self, run_id: int, *, fail_at: int | None = None) -> dict[str, Any]:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(f"采样运行 {run_id} 不存在")
            if run["状态"] == RUN_RUNNING:
                raise ValueError(f"采样运行 {run['运行编号']} 正在执行中，请勿重复提交")
            batches = self._batches[run_id]
            todo = [b for b in batches if b["状态"] != BATCH_DONE]
            if not todo:
                run["消息"] = "没有未完成批次，无需重试"
                return self.run_detail(run_id)
            run["状态"] = RUN_RUNNING
            run["消息"] = ""

        table = run["输出表名"]
        owner = run["运行编号"]
        if not self.acquire_output_lock(table, owner):
            holder = self.output_lock_owner(table) or "另一个运行"
            with self._lock:
                run["状态"] = RUN_FAILED
                run["消息"] = f"输出表「{table}」正被运行 {holder} 写入，本次整组未写入，可稍后重试"
            return self.run_detail(run_id)

        interrupted = False
        try:
            layer_cache: dict[str, list[int]] = {}
            for batch in todo:
                if fail_at is not None and batch["序号"] == fail_at:
                    with self._lock:
                        batch["状态"] = BATCH_FAILED
                        batch["失败原因"] = "模拟中断：本批未写入，重试时沿用原随机种子断点"
                        run["状态"] = RUN_FAILED
                    interrupted = True
                    break
                self._commit_batch(run, batch, layer_cache)
        finally:
            self.release_output_lock(table, owner)

        with self._lock:
            if not interrupted and run["状态"] != RUN_FAILED:
                run["状态"] = RUN_DONE
                run["完成时间"] = _now()
                self._purge_old_versions(table)
            done = sum(1 for b in batches if b["状态"] == BATCH_DONE)
            total = len(batches)
            if run["状态"] == RUN_DONE:
                run["消息"] = (
                    f"运行 {owner} 已完成：{done}/{total} 批写入输出表「{table}」（版本 v{run['版本']}）"
                )
            elif run["状态"] == RUN_FAILED and not run.get("消息"):
                run["消息"] = (
                    f"运行 {owner} 部分失败：{done}/{total} 批完成，"
                    "重试只处理未完成批次并沿用随机种子断点"
                )
        return self.run_detail(run_id)

    def resume_run(self, run_id: int, *, fail_at: int | None = None) -> dict[str, Any]:
        """断点续跑：已完成的批次直接跳过，只补未完成批次，种子断点原样沿用。"""
        return self.execute_run(run_id, fail_at=fail_at)

    def _commit_batch(
        self,
        run: dict[str, Any],
        batch: dict[str, Any],
        layer_cache: dict[str, list[int]],
    ) -> None:
        """提交单个批次；重复提交只推进一次，已写入的行不会再次写入。"""
        with self._lock:
            if batch["状态"] == BATCH_DONE:
                return  # 幂等：重复批次只推进一次
            run_id = int(run["id"])
            table = run["输出表名"]
            already = any(
                row["运行编号"] == run["运行编号"] and row["批次序号"] == batch["序号"]
                for row in self._outputs.get(table, [])
            )
            if already:
                batch["状态"] = BATCH_DONE  # 输出表里已有本批数据，直接认定完成
                return
        if self.batch_hook is not None:
            self.batch_hook(batch)

        layer = batch["分层字段"]
        if layer not in layer_cache:
            sample_count = max(1, round(int(run["源行数"]) * float(run["样本比例"])))
            layer_cache[layer] = layer_sample_rows(
                int(batch["随机种子断点"]), int(run["源行数"]), sample_count
            )
        picked = layer_cache[layer][batch["样本起点"]:batch["样本终点"]]
        stamp = _now()
        with self._lock:
            rows = self._outputs.setdefault(table, [])
            for source_row in picked:
                self._row_seq += 1
                rows.append({
                    "id": self._row_seq,
                    "输出表名": table,
                    "运行编号": run["运行编号"],
                    "配置编号": run["配置编号"],
                    "版本": run["版本"],
                    "分层字段": layer,
                    "批次序号": batch["序号"],
                    "样本ID": f"{run['配置编号']}-{layer}-{source_row:06d}",
                    "源行号": source_row,
                    "抽样时间": stamp,
                })
            batch["状态"] = BATCH_DONE
            batch["已采样行数"] = len(picked)
            batch["写入次数"] += 1
            batch["完成时间"] = stamp
            batch["失败原因"] = None

    def _purge_old_versions(self, table: str) -> None:
        """版本保留：每个输出表只保留最近 VERSION_RETENTION 个版本的采样结果。"""
        versions = sorted(
            {int(run["版本"]) for run in self._runs.values() if run["输出表名"] == table},
            reverse=True,
        )
        drop = set(versions[VERSION_RETENTION:])
        if not drop:
            return
        rows = self._outputs.get(table, [])
        self._outputs[table] = [row for row in rows if int(row["版本"]) not in drop]
        for run in self._runs.values():
            if run["输出表名"] == table and int(run["版本"]) in drop:
                run["结果已清理"] = True

    # ---- 查询 ----
    def run_detail(self, run_id: int) -> dict[str, Any]:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(f"采样运行 {run_id} 不存在")
            batches = [dict(batch) for batch in self._batches.get(run_id, [])]
            done = sum(1 for batch in batches if batch["状态"] == BATCH_DONE)
            detail = dict(run)
            detail["批次"] = batches
            detail["完成位置"] = f"{done}/{len(batches)}"
            return detail

    def runs_for_config(self, config_id: int) -> list[dict[str, Any]]:
        with self._lock:
            runs = [run for run in self._runs.values() if int(run["config_id"]) == config_id]
            result = []
            for run in sorted(runs, key=lambda item: int(item["id"])):
                batches = self._batches.get(int(run["id"]), [])
                done = sum(1 for batch in batches if batch["状态"] == BATCH_DONE)
                item = {key: value for key, value in run.items() if key != "消息"}
                item["批次数"] = len(batches)
                item["完成位置"] = f"{done}/{len(batches)}"
                result.append(item)
            return result

    def progress_text(self, config_id: int) -> str | None:
        """清单里展示的完成位置：刷新后依旧保留最近一次运行的进度。"""
        with self._lock:
            runs = [run for run in self._runs.values() if int(run["config_id"]) == config_id]
            if not runs:
                return None
            latest = max(runs, key=lambda run: int(run["id"]))
            batches = self._batches.get(int(latest["id"]), [])
            done = sum(1 for batch in batches if batch["状态"] == BATCH_DONE)
            total = len(batches)
            if latest["状态"] == RUN_DONE:
                return f"已完成 v{latest['版本']}（{total}/{total} 批）"
            pending_layers: list[str] = []
            for batch in batches:
                if batch["状态"] != BATCH_DONE and batch["分层字段"] not in pending_layers:
                    pending_layers.append(batch["分层字段"])
            return f"{done}/{total} 批（待续跑：{'、'.join(pending_layers)}）"

    def current_version(self, config_id: int) -> str | None:
        with self._lock:
            runs = [run for run in self._runs.values() if int(run["config_id"]) == config_id]
            if not runs:
                return None
            latest = max(runs, key=lambda run: int(run["id"]))
            return f"v{latest['版本']}"

    def output_overview(self, config_id: int) -> dict[str, Any] | None:
        with self._lock:
            runs = [run for run in self._runs.values() if int(run["config_id"]) == config_id]
            if not runs:
                return None
            table = max(runs, key=lambda run: int(run["id"]))["输出表名"]
            rows = self._outputs.get(table, [])
            versions = []
            for run in sorted(
                (r for r in self._runs.values() if r["输出表名"] == table),
                key=lambda item: int(item["版本"]),
            ):
                version_rows = [row for row in rows if int(row["版本"]) == int(run["版本"])]
                layers: dict[str, int] = {}
                for row in version_rows:
                    layers[row["分层字段"]] = layers.get(row["分层字段"], 0) + 1
                versions.append({
                    "版本": run["版本"],
                    "运行编号": run["运行编号"],
                    "配置编号": run["配置编号"],
                    "状态": run["状态"],
                    "样本比例": run["样本比例"],
                    "行数": len(version_rows),
                    "分层行数": layers,
                    "结果已清理": run["结果已清理"],
                })
            return {"输出表名": table, "总行数": len(rows), "版本": versions}

    def output_rows(
        self,
        config_id: int,
        *,
        version: int | None = None,
        layer: str | None = None,
        page: int = 1,
        size: int = 200,
    ) -> tuple[list[dict[str, Any]], int]:
        overview = self.output_overview(config_id)
        if overview is None:
            return [], 0
        rows = list(self._outputs.get(overview["输出表名"], []))
        if version is not None:
            rows = [row for row in rows if int(row["版本"]) == version]
        if layer:
            rows = [row for row in rows if row["分层字段"] == layer]
        rows.sort(key=lambda row: int(row["id"]))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    # ---- 配置变更：只影响新批次 ----
    def update_settings(self, config: dict[str, Any], values: dict[str, Any]) -> list[str]:
        """更新采样配置；历史运行、批次断点与输出结果保持原样，新比例只影响新批次。"""
        changed: list[str] = []
        if "样本比例" in values and str(values.get("样本比例") or "").strip():
            ratio = parse_ratio(values["样本比例"])
            config["样本比例"] = f"{ratio:g}"
            changed.append("样本比例")
        if "随机种子" in values and str(values.get("随机种子") or "").strip():
            seed = parse_seed(values["随机种子"])
            config["随机种子"] = str(seed)
            changed.append("随机种子")
        if "分层字段" in values and str(values.get("分层字段") or "").strip():
            layers = parse_layers(values["分层字段"])
            if not layers:
                raise ValueError("分层字段不能为空")
            config["分层字段"] = ",".join(layers)
            changed.append("分层字段")
        if "输出表名" in values and str(values.get("输出表名") or "").strip():
            config["输出表名"] = str(values["输出表名"]).strip()
            changed.append("输出表名")
        if not changed:
            raise ValueError("没有可更新的采样配置字段")
        return changed


engine = SampleRunEngine()
