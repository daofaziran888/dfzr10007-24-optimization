"""数据采样管理接口：维护采样配置，覆盖启用采样、暂停采样、关闭采样等动作。

分批采样接口：
- 整组提交：POST /api/sample/{entry_id}/runs，多选分层字段一次提交，逐批写入输出表；
- 断点续跑：POST /api/sample/runs/{run_id}/resume，只补未完成批次并沿用随机种子断点；
- 结果核对：GET  /api/sample/{entry_id}/output，按版本与分层字段核对输出表内容。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.sample import SampleService

router = APIRouter(prefix="/api/sample", tags=["数据采样管理"])

service = SampleService()

LIST_FIELDS = ["配置编号", "数据源名称", "采样算法", "样本比例", "随机种子", "分层字段", "输出表名", "配置状态", "完成位置"]
STATUSES = ["草稿", "已启用", "已暂停", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、已启用、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按配置编号与状态过滤数据采样管理列表；清单保留每条配置的完成位置。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/runs/{run_id}", response_model=dict)
def get_run(run_id: int) -> dict:
    """单次采样运行明细：逐条列出每个批次的随机种子断点与完成状态。"""
    detail = service.get_run(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"采样运行 {run_id} 不存在")
    return detail


@router.post("/runs/{run_id}/resume", response_model=ActionResult)
def resume_run(run_id: int, payload: EntryPayload) -> ActionResult:
    """断点续跑：只处理未完成批次，沿用分层字段的随机种子断点，已完成分层不重写。"""
    detail, message = service.resume_run(run_id, payload.values)
    if detail is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=detail)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条采样配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采样配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条采样配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="采样配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条采样配置执行启用采样、暂停采样、关闭采样；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}/runs", response_model=dict)
def list_runs(entry_id: int) -> dict:
    """采样配置的版本历史：每次整组提交生成一个版本，历史结果互不覆盖。"""
    runs, message = service.list_runs(entry_id)
    if runs is None:
        raise HTTPException(status_code=404, detail=message)
    return {"items": runs, "total": len(runs)}


@router.post("/{entry_id}/runs", response_model=ActionResult)
def submit_run(entry_id: int, payload: EntryPayload) -> ActionResult:
    """整组提交一批分层：按样本比例快照切批，逐批写入输出表；争用输出表时整组不写入。"""
    detail, message = service.submit_run(entry_id, payload.values)
    if detail is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=detail)


@router.get("/{entry_id}/output", response_model=dict)
def output_overview(
    entry_id: int,
    version: int | None = Query(default=None, description="按版本核对输出表"),
    layer: str | None = Query(default=None, description="按分层字段核对输出表"),
    page: int = 1,
    size: int = 50,
) -> dict:
    """核对输出表：版本汇总 + 样本明细；历史版本在保留范围内不覆盖。"""
    if size > 500:
        raise HTTPException(status_code=400, detail="每页最多 500 条，请缩小分页范围")
    overview, message = service.output_overview(entry_id)
    if overview is None:
        raise HTTPException(status_code=404, detail=message)
    items, total = service.output_rows(entry_id, version=version, layer=layer, page=page, size=size)
    return {"overview": overview, "items": items, "total": total, "page": page, "size": size}


@router.post("/{entry_id}/settings", response_model=ActionResult)
def update_settings(entry_id: int, payload: EntryPayload) -> ActionResult:
    """更新样本比例、随机种子、分层字段、输出表名；新样本比例只影响新批次。"""
    entry, message = service.update_settings(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据采样管理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "sample", "total": total, "items": items}
