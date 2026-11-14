"""SAMP-0002 分批采样核对脚本：断点续跑、幂等提交、写锁与版本保留。

直接用服务层跑完整流程，覆盖：
1. 多选分层整组一次提交，逐批落断点；
2. region 层完成后 city 层中断，重启续跑不重写 region；
3. 重复批次只推进一次，输出表无重复样本；
4. 新样本比例只影响新批次，历史结果不覆盖；
5. 两个运行争用同一输出表时只能一个写入；
6. 每个输出表只保留最近 3 个版本；
7. 清单保留完成位置。

运行方式：cd backend && .venv/bin/python tests/verify_sample_run.py
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.sample import SampleService  # noqa: E402
from app.services.sample_run import (  # noqa: E402
    VERSION_RETENTION,
    derive_layer_seed,
    engine,
    layer_sample_rows,
)
from app.store import store  # noqa: E402

service = SampleService()
CHECKS: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"核对失败：{label} {detail}")
    CHECKS.append(label)
    print(f"  PASS {label}")


def samp0002() -> dict:
    config = store.find("sample", 2)
    assert config is not None
    return config


def output_ids(version: int | None = None, layer: str | None = None) -> list[str]:
    rows, _ = engine.output_rows(2, version=version, layer=layer, page=1, size=100000)
    return sorted(str(row["样本ID"]) for row in rows)


print("== 1. SAMP-0002 整组提交 region+city，模拟 city 层第 1 批（全局第 3 批）中断 ==")
config = samp0002()
check("SAMP-0002 样本比例/随机种子/分层字段/输出表名就绪",
      (config["样本比例"], config["随机种子"], config["分层字段"], config["输出表名"])
      == ("0.20", "42", "region,city", "dm_sample_samp0002"))

detail, message = service.submit_run(2, {"分层字段": ["region", "city"], "模拟中断批次": 3})
assert detail is not None
run_id = int(detail["id"])
batches = {batch["序号"]: batch for batch in detail["批次"]}
check("整组提交生成 4 个批次（每层 1000 行 ÷ 每批 500 行）", len(batches) == 4 and detail["状态"] == "部分失败")
check("region 层 2 批已完成", batches[1]["状态"] == "已完成" and batches[2]["状态"] == "已完成")
check("city 层第 1 批失败、第 2 批待处理", batches[3]["状态"] == "失败" and batches[4]["状态"] == "待处理")
check("每个分层字段保存随机种子断点",
      batches[1]["随机种子断点"] == derive_layer_seed(42, "region")
      and batches[3]["随机种子断点"] == derive_layer_seed(42, "city")
      and batches[3]["随机种子断点"] == batches[4]["随机种子断点"])
check("中断后输出表只写入 region 层 1000 行", len(output_ids(version=1)) == 1000)
region_snapshot = output_ids(version=1, layer="region")

print("== 2. 断点续跑：只补 city 层，region 层不重写 ==")
detail, message = service.resume_run(run_id, {})
assert detail is not None
check("续跑后运行已完成", detail["状态"] == "已完成" and detail["完成位置"] == "4/4")
check("region 层样本未被重写", output_ids(version=1, layer="region") == region_snapshot)
check("输出表共 2000 行（region 1000 + city 1000）", len(output_ids(version=1)) == 2000)
check("重试沿用随机种子断点（断点值未变）",
      {b["序号"]: b["随机种子断点"] for b in detail["批次"]}[3] == derive_layer_seed(42, "city"))

print("== 3. 用配置参数独立重算，核对数据采样结果 ==")
for layer in ("region", "city"):
    expected = layer_sample_rows(derive_layer_seed(42, layer), 5000, round(5000 * 0.2))
    rows, _ = engine.output_rows(2, version=1, layer=layer, page=1, size=100000)
    actual = sorted(int(row["源行号"]) for row in rows)
    check(f"{layer} 层样本与 种子42+比例0.20 重算结果一致", actual == expected)
all_rows, _ = engine.output_rows(2, version=1, page=1, size=100000)
check("输出表无重复样本（流水线不会读到重复输入）",
      len({row["样本ID"] for row in all_rows}) == len(all_rows))

print("== 4. 幂等：重复批次只推进一次 ==")
detail_again, message = service.resume_run(run_id, {})
assert detail_again is not None
check("已完成运行再次重试不推进", "没有未完成批次" in message)
check("输出表行数不变", len(output_ids(version=1)) == 2000)
check("每批写入次数都是 1", all(b["写入次数"] == 1 for b in detail_again["批次"]))
engine._commit_batch(engine._runs[run_id], engine._batches[run_id][0], {})
check("直接重复提交已完成批次被幂等拦截", len(output_ids(version=1)) == 2000)

print("== 5. 新样本比例只影响新批次，历史结果不覆盖 ==")
config_v1_rows = len(output_ids(version=1))
updated, message = service.update_settings(2, {"样本比例": "0.10"})
check("样本比例更新成功", updated is not None and updated["样本比例"] == "0.1")
detail_v2, _ = service.submit_run(2, {"分层字段": ["region", "city"]})
assert detail_v2 is not None
check("新运行 v2 按新比例每层 500 行、每层 1 批",
      detail_v2["版本"] == 2 and len(detail_v2["批次"]) == 2
      and all(b["计划行数"] == 500 for b in detail_v2["批次"]))
check("历史 v1 结果未被覆盖", len(output_ids(version=1)) == config_v1_rows)
check("v1 批次仍沿用旧比例快照（每批 500 行、共 4 批）",
      all(b["计划行数"] == 500 for b in detail["批次"]) and len(detail["批次"]) == 4)

print("== 6. 输出表写锁：两个运行争用同一输出表时只能一个写入 ==")
service.update_settings(3, {"输出表名": "dm_sample_samp0002"})
plan3 = engine.plan_run(store.find("sample", 3))
plan2 = engine.plan_run(samp0002())
check("同一输出表版本递增", plan3["版本"] == 3 and plan2["版本"] == 4)
check("手动占用写锁后另一运行整组不写入", engine.acquire_output_lock("dm_sample_samp0002", "TEST-HOLDER"))
locked = engine.execute_run(int(plan3["id"]))
check("争用失败：整组未写入、可稍后重试",
      locked["状态"] == "部分失败" and "TEST-HOLDER" in locked["消息"]
      and all(b["状态"] == "待处理" for b in locked["批次"]))
engine.release_output_lock("dm_sample_samp0002", "TEST-HOLDER")

started = threading.Event()
def slow_hook(_batch: dict) -> None:
    started.set()
    time.sleep(0.05)

engine.batch_hook = slow_hook
results: dict[str, dict] = {}
def run_thread(key: str, rid: int) -> None:
    results[key] = engine.execute_run(rid)

t1 = threading.Thread(target=run_thread, args=("A", int(plan3["id"])))
t1.start()
started.wait(2)
t2 = threading.Thread(target=run_thread, args=("B", int(plan2["id"])))
t2.start()
t1.join(); t2.join()
engine.batch_hook = None
statuses = sorted(results[key]["状态"] for key in ("A", "B"))
check("并发争用同一输出表：一个完成、一个未写入", statuses == ["已完成", "部分失败"])
loser = results["B"] if results["B"]["状态"] == "部分失败" else results["A"]
recovered = engine.resume_run(int(loser["id"]))
check("争用失败方重试后完成", recovered["状态"] == "已完成")

print("== 7. 版本保留：每个输出表只保留最近 3 个版本 ==")
service.submit_run(2, {"分层字段": ["region"]})   # v5
service.submit_run(2, {"分层字段": ["city"]})     # v6
versions = engine.output_overview(2)["版本"]
kept = [v["版本"] for v in versions]
check(f"版本清单完整（共 {len(kept)} 个版本元数据）", kept == [1, 2, 3, 4, 5, 6])
alive = {int(row["版本"]) for row in engine._outputs["dm_sample_samp0002"]}
check(f"输出表只保留最近 {VERSION_RETENTION} 个版本的行", alive == {4, 5, 6})
check("被清理版本标记「结果已清理」",
      all(v["结果已清理"] for v in versions if v["版本"] in (1, 2, 3)))

print("== 8. 清单保留完成位置（刷新后仍可见） ==")
items, _ = service.list_entries()
by_code = {row["配置编号"]: row for row in items}
check("SAMP-0002 完成位置保留在清单里",
      by_code["SAMP-0002"].get("完成位置", "").startswith("已完成 v6"))
check("SAMP-0002 当前版本 v6", by_code["SAMP-0002"].get("当前版本") == "v6")
check("SAMP-0003 完成位置保留在清单里", "已完成" in by_code["SAMP-0003"].get("完成位置", ""))

print(f"\n全部 {len(CHECKS)} 项核对通过。")
