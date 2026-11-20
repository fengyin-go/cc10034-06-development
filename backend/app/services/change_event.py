"""变更事件领域服务：消费管道中的 ChangeEventRecord，按事务归并并产出可回放事务包。

上游：binlog_parser 产出事件推入管道 → change_event 消费解析并按事务归并
下游：事务包导出（replay package）→ 导入回放（幂等，只写入从未应用的事务）

关键边界约定：
- 事务成员携带 binlog_file/binlog_pos，提交坐标由 XID/COMMIT 边界事件显式登记；
- 观察窗结束而 commit 未到的事务保持 OPEN，导出时明确标注，不伪装成完整明细；
- tx_key / member_key 由来源坐标确定性派生，重复导出身份稳定；
- 导出文件与事务明细查询共用 pipeline_bus.transactions 同一数据源，位点口径一致。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import TransactionMember, pipeline_bus

PACKAGE_VERSION = "1.0"
PACKAGE_KIND = "cdc-tx-replay-package"


class ChangeEventService(BaseModuleService):
    MODULE = "change_event"
    ENTITY = "变更记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["事件ID", "来源表", "操作类型", "变更前镜像", "变更后镜像", "事务ID", "事件时间"]
    REQUIRED_FIELDS = ["事件ID", "来源表", "操作类型"]
    STATUS_ORDER = ["已捕获", "已解析", "已下发", "解析失败"]
    ACTION_RULES = {"解析事件": "已捕获", "下发事件": "已解析", "标记失败": "已解析"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 管道消费：变更事件落库 + 按事务归并
    # ------------------------------------------------------------------
    def consume_pipeline_events(self, batch_size: int = 50) -> list[dict[str, Any]]:
        """从管道总线消费变更事件，落库为变更记录并按事务ID归并到事务组。

        消费 pipeline_bus 中 binlog_parser 产出的缓冲区事件：
        1. 每条事件写入 change_event 变更记录表（携带 binlog_file/binlog_pos 来源坐标）；
        2. 同步归并进 pipeline_bus.transactions —— 导出文件与事务明细共用这一份数据。
        """
        raw_events = pipeline_bus.consume_events(batch_size=batch_size)
        if not raw_events:
            return []
        by_op: dict[str, int] = {}
        by_table: dict[str, int] = {}
        entries: list[dict[str, Any]] = []
        rows = self._store_rows()
        known_member_keys = {
            row.get("member_key") for row in rows if row.get("member_key")
        }
        for evt in raw_events:
            by_op[evt.operation] = by_op.get(evt.operation, 0) + 1
            by_table[evt.source_table] = by_table.get(evt.source_table, 0) + 1
            before_str = json.dumps(evt.before_image, ensure_ascii=False) if evt.before_image else "{}"
            after_str = json.dumps(evt.after_image, ensure_ascii=False) if evt.after_image else "{}"
            # 归并到事务组（成员按来源坐标去重），回填稳定身份
            group = pipeline_bus.merge_transaction_event(evt)
            member_key = TransactionMember.build_key(evt.event_id, evt.binlog_file, evt.binlog_pos)
            member = next(m for m in group.members if m.member_key == member_key)
            # 同一来源坐标的事件重复消费时不重复落库，保持成员身份稳定
            if member.member_key in known_member_keys:
                continue
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "事件ID": evt.event_id,
                "来源表": f"{evt.source_db}.{evt.source_table}",
                "操作类型": evt.operation,
                "变更前镜像": before_str[:200],
                "变更后镜像": after_str[:200],
                "事务ID": evt.tx_id,
                "事件时间": evt.event_time.isoformat(),
                "raw_format": evt.raw_format,
                "binlog_file": evt.binlog_file,
                "binlog_pos": evt.binlog_pos,
                "gtid": evt.gtid,
                "member_key": member.member_key,
                "tx_key": group.tx_key,
                "status": "已捕获",
                "pending": True,
                "abnormal": False,
            }
            known_member_keys.add(member.member_key)
            rows.append(entry)
            entries.append(entry)
        pipeline_bus.write_audit(
            "事件消费", f"{len(entries)}条事件",
            f"消费管道事件 {len(raw_events)} 条，新落库 {len(entries)} 条，按事务归并完成",
            "change_event", "已解析"
        )
        return entries

    def _store_rows(self) -> list[dict[str, Any]]:
        from app.store import store
        return store.rows(self.MODULE)

    # ------------------------------------------------------------------
    # 事务视图：与导出共用同一数据源
    # ------------------------------------------------------------------
    def list_transactions(self, status_filter: str | None = None) -> dict[str, Any]:
        """列出归并后的事务明细（摘要），数据源与导出文件完全一致。"""
        groups = pipeline_bus.list_transactions()
        items = []
        for group in groups:
            if status_filter and group.state != status_filter:
                continue
            items.append(group.to_dict(include_members=False))
        committed = sum(1 for g in groups if g.state == "COMMITTED")
        open_tx = sum(1 for g in groups if g.state == "OPEN")
        rolled_back = sum(1 for g in groups if g.state == "ROLLED_BACK")
        watermark_file, watermark_pos = pipeline_bus.high_watermark()
        return {
            "items": items,
            "total": len(items),
            "committed": committed,
            "open": open_tx,
            "rolled_back": rolled_back,
            "watermark": {"binlog_file": watermark_file, "binlog_pos": watermark_pos},
        }

    def get_transaction_detail(self, tx_key: str) -> dict[str, Any] | None:
        """打开事务详情：复核成员、起始坐标与事务提交位置。"""
        group = pipeline_bus.get_transaction(tx_key)
        if group is None:
            return None
        detail = group.to_dict(include_members=True)
        detail["export_batches"] = [
            {
                "batch_no": batch["batch_no"],
                "exported_at": batch["exported_at"],
                "included": group.tx_key in batch.get("committed_tx_keys", [])
                or group.tx_key in batch.get("open_tx_keys", []),
                "state_in_package": (
                    "COMMITTED" if group.tx_key in batch.get("committed_tx_keys", [])
                    else "OPEN" if group.tx_key in batch.get("open_tx_keys", [])
                    else "NOT_INCLUDED"
                ),
            }
            for batch in pipeline_bus.export_batches
        ]
        return detail

    def list_export_batches(self) -> list[dict[str, Any]]:
        """导出批次登记：复核每轮文件实际囊括的事务范围。"""
        return list(pipeline_bus.export_batches)

    # ------------------------------------------------------------------
    # 事务包导出
    # ------------------------------------------------------------------
    def build_replay_package(self) -> dict[str, Any]:
        """把归并后的事务按提交边界打包成可回放事件数据包。

        范围规则：
        - 仅 COMMITTED 事务进入可回放事务集合；
        - OPEN 事务（观察窗结束、commit 未到）单独列入 open_transactions，
          明确标注未闭合，绝不伪装成完整明细；
        - ROLLED_BACK 事务不进入数据包；
        - 包内每个事务/成员的坐标直接取自事务归并表，与详情页同源。
        """
        groups = pipeline_bus.list_transactions()
        committed_groups = [g for g in groups if g.state == "COMMITTED"]
        open_groups = [g for g in groups if g.state == "OPEN"]
        rolled_back = [g for g in groups if g.state == "ROLLED_BACK"]

        watermark_file, watermark_pos = pipeline_bus.high_watermark()
        transactions_payload = [g.to_dict(include_members=True) for g in committed_groups]
        open_payload = [g.to_dict(include_members=True) for g in open_groups]

        member_count = sum(len(g.members) for g in committed_groups)
        package = {
            "package_kind": PACKAGE_KIND,
            "package_version": PACKAGE_VERSION,
            "exported_at": datetime.now().isoformat(),
            "source_module": self.MODULE,
            "observation_window": {
                "watermark": {"binlog_file": watermark_file, "binlog_pos": watermark_pos},
                "note": "观察窗结束位置；commit 未到的事务列入 open_transactions",
            },
            "summary": {
                "committed_transactions": len(committed_groups),
                "open_transactions": len(open_groups),
                "rolled_back_transactions": len(rolled_back),
                "replay_members": member_count,
            },
            "transactions": transactions_payload,
            "open_transactions": open_payload,
        }
        package["package_checksum"] = self._checksum(package)
        return package

    def export_replay_package(self) -> tuple[dict[str, Any], str]:
        """导出事务包并登记本轮观察窗与实际囊括范围。"""
        package = self.build_replay_package()
        committed_keys = [t["tx_key"] for t in package["transactions"]]
        open_keys = [t["tx_key"] for t in package["open_transactions"]]
        member_keys = sorted(
            m["member_key"] for t in package["transactions"] for m in t["members"]
        )
        batch = pipeline_bus.register_export_batch({
            "watermark": package["observation_window"]["watermark"],
            "committed_tx_keys": committed_keys,
            "open_tx_keys": open_keys,
            "rolled_back_tx_keys": [
                g.tx_key for g in pipeline_bus.list_transactions() if g.state == "ROLLED_BACK"
            ],
            "committed_count": len(committed_keys),
            "open_count": len(open_keys),
            "member_count": len(member_keys),
            "member_keys": member_keys,
            "package_checksum": package["package_checksum"],
            "filename": self.package_filename(package),
        })
        # 批次号先落进包体，再重算校验和，保证下载文件自校验始终成立。
        package["export_batch_no"] = batch["batch_no"]
        package["package_checksum"] = self._checksum(
            {k: v for k, v in package.items() if k != "package_checksum"}
        )
        batch["package_checksum"] = package["package_checksum"]
        pipeline_bus.write_audit(
            "事务包导出", f"批次{batch['batch_no']}",
            f"导出事务包：完整事务 {len(committed_keys)} 笔，未闭合事务 {len(open_keys)} 笔，"
            f"成员 {len(member_keys)} 条，观察窗 {batch['watermark']['binlog_file']}:"
            f"{batch['watermark']['binlog_pos']}",
            "change_event", "已记录"
        )
        return package, batch["filename"]

    def package_filename(self, package: dict[str, Any]) -> str:
        stamp = package["exported_at"].replace("-", "").replace(":", "").replace(".", "").replace("T", "-")
        return f"change_event_tx_replay_{stamp}.json"

    def serialize_package(self, package: dict[str, Any]) -> bytes:
        return json.dumps(package, ensure_ascii=False, indent=2, sort_keys=False).encode("utf-8")

    # ------------------------------------------------------------------
    # 导入回放：幂等，只写入从未应用的事务
    # ------------------------------------------------------------------
    def replay_package(self, package: dict[str, Any]) -> dict[str, Any]:
        """回放导入事务包，按稳定身份幂等应用。

        - 校验包类型与校验和；
        - OPEN 事务一律拒绝（未闭合不能当完整明细落库）；
        - tx_key 已在 replayed_tx_keys 登记的事务整体跳过，成员不重复写入；
        - 同一事务内成员 member_key 去重，保证重复导入只写入从未应用的事务。
        """
        if package.get("package_kind") != PACKAGE_KIND:
            return self._replay_result(ok=False, message="不是合法的CDC事务回放包（package_kind 不匹配）")
        checksum = package.get("package_checksum", "")
        if checksum != self._checksum({k: v for k, v in package.items() if k != "package_checksum"}):
            return self._replay_result(ok=False, message="事务包校验和不一致，数据可能已被篡改")

        applied: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        rows = self._store_rows()

        open_in_package = package.get("open_transactions", [])
        for tx in open_in_package:
            rejected.append({
                "tx_key": tx.get("tx_key"),
                "tx_id": tx.get("tx_id"),
                "reason": "事务未闭合（OPEN），缺少提交坐标，不能作为完整明细回放",
            })

        for tx in package.get("transactions", []):
            tx_key = tx.get("tx_key", "")
            tx_id = tx.get("tx_id", "")
            if tx.get("state") != "COMMITTED" or not tx.get("commit"):
                rejected.append({"tx_key": tx_key, "tx_id": tx_id, "reason": "事务缺少提交边界，拒绝回放"})
                continue
            existing = pipeline_bus.replayed_tx_keys.get(tx_key)
            if existing is not None:
                skipped.append({
                    "tx_key": tx_key,
                    "tx_id": tx_id,
                    "reason": "事务已应用，重复导入整体跳过",
                    "first_applied_at": existing.get("applied_at"),
                    "member_count": existing.get("member_count"),
                })
                continue
            member_written = 0
            member_keys: list[str] = []
            for member in tx.get("members", []):
                member_key = member.get("member_key", "")
                member_keys.append(member_key)
                if self._member_already_applied(member_key):
                    continue
                rows.append({
                    "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                    "事件ID": member.get("event_id", ""),
                    "来源表": f"{member.get('source_db', '')}.{member.get('source_table', '')}",
                    "操作类型": member.get("operation", ""),
                    "变更前镜像": json.dumps(member.get("before_image") or {}, ensure_ascii=False)[:200],
                    "变更后镜像": json.dumps(member.get("after_image") or {}, ensure_ascii=False)[:200],
                    "事务ID": tx_id,
                    "事件时间": member.get("event_time", ""),
                    "raw_format": member.get("raw_format", "debezium"),
                    "binlog_file": member.get("binlog_file", ""),
                    "binlog_pos": member.get("binlog_pos", 0),
                    "gtid": member.get("gtid", ""),
                    "member_key": member_key,
                    "tx_key": tx_key,
                    "commit_binlog_file": tx["commit"].get("binlog_file", ""),
                    "commit_binlog_pos": tx["commit"].get("binlog_pos"),
                    "status": "已下发",
                    "pending": False,
                    "abnormal": False,
                    "replayed": True,
                })
                member_written += 1
            applied_record = {
                "tx_key": tx_key,
                "tx_id": tx_id,
                "member_count": len(member_keys),
                "members_written": member_written,
                "commit": tx.get("commit"),
                "applied_at": datetime.now().isoformat(),
                "member_keys": member_keys,
            }
            pipeline_bus.replayed_tx_keys[tx_key] = applied_record
            applied.append(applied_record)

        run = {
            "run_no": len(pipeline_bus.replay_runs) + 1,
            "replayed_at": datetime.now().isoformat(),
            "applied": applied,
            "skipped": skipped,
            "rejected": rejected,
        }
        pipeline_bus.replay_runs.append(run)
        pipeline_bus.write_audit(
            "事务包回放", f"第{run['run_no']}次导入",
            f"应用事务 {len(applied)} 笔，跳过重复 {len(skipped)} 笔，拒绝未闭合 {len(rejected)} 笔",
            "change_event", "已记录"
        )
        return self._replay_result(
            ok=not rejected,
            message=(
                f"回放完成：应用 {len(applied)} 笔事务、跳过 {len(skipped)} 笔重复事务、"
                f"拒绝 {len(rejected)} 笔未闭合/非法事务"
            ),
            applied=applied, skipped=skipped, rejected=rejected,
        )

    def _member_already_applied(self, member_key: str) -> bool:
        for record in pipeline_bus.replayed_tx_keys.values():
            if member_key in record.get("member_keys", []):
                return True
        return False

    def list_replay_runs(self) -> list[dict[str, Any]]:
        return list(pipeline_bus.replay_runs)

    def _replay_result(self, *, ok: bool, message: str, **extra: Any) -> dict[str, Any]:
        return {"ok": ok, "message": message, **extra}

    @staticmethod
    def _checksum(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # 原有分类/差异能力
    # ------------------------------------------------------------------
    def classify_by_operation(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """对消费后的事件按操作类型分类统计。

        为下游 format_adapter 提供操作类型分布以决定格式适配策略。
        """
        ops: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        tables: dict[str, int] = {}
        total_rows_affected = 0
        for evt in events:
            op = evt.get("操作类型", "").upper()
            if op == "INSERT":
                ops["insert"] += 1
            elif op == "UPDATE":
                ops["update"] += 1
            elif op == "DELETE":
                ops["delete"] += 1
            tbl = evt.get("来源表", "unknown")
            tables[tbl] = tables.get(tbl, 0) + 1
            total_rows_affected += 1
        return {
            "total_events": len(events),
            "by_operation": ops,
            "by_table": tables,
            "insert_ratio": round(ops["insert"] / max(len(events), 1) * 100, 1),
            "update_ratio": round(ops["update"] / max(len(events), 1) * 100, 1),
            "delete_ratio": round(ops["delete"] / max(len(events), 1) * 100, 1),
        }

    def delta_computation(self, event: dict[str, Any]) -> dict[str, Any]:
        """对变更前后镜像执行差异计算。

        计算 UPDATE 操作中哪些字段实际发生了变化，
        产出变更增量列表供下游 SCD 策略消费。
        """
        try:
            before = json.loads(event.get("变更前镜像", "{}"))
            after_raw = event.get("变更后镜像", "{}")
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
        except (json.JSONDecodeError, TypeError):
            return {"delta_fields": [], "delta_count": 0}
        changed = []
        for key in after:
            if key not in before or before[key] != after[key]:
                delta_val = {"field": key, "old": before.get(key), "new": after[key]}
                changed.append(delta_val)
        return {"delta_fields": changed, "delta_count": len(changed)}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "下发事件":
            pipeline_bus.write_audit(
                "事件下发", entry.get("来源表", ""),
                f"事件 {entry_id} 下发给下游消费者",
                "change_event", "已下发"
            )
        return entry, msg
