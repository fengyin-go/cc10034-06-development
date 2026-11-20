"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


@dataclass
class TransactionMember:
    """事务成员：同一事务内按 Binlog 顺序排列的一条变更事件。

    member_key 由成员自身的来源坐标与事件身份确定性派生，
    保证同一事件无论落在哪一次导出，身份都保持稳定。
    """
    member_key: str
    event_id: str
    source_db: str
    source_table: str
    operation: str
    binlog_file: str
    binlog_pos: int
    event_time: str
    before_image: dict[str, Any] | None = None
    after_image: dict[str, Any] | None = None
    gtid: str = ""
    raw_format: str = "debezium"
    seq: int = 0

    @staticmethod
    def build_key(event_id: str, binlog_file: str, binlog_pos: int) -> str:
        raw = f"{event_id}|{binlog_file}|{binlog_pos}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @classmethod
    def from_event(cls, event: ChangeEventRecord, seq: int) -> "TransactionMember":
        return cls(
            member_key=cls.build_key(event.event_id, event.binlog_file, event.binlog_pos),
            event_id=event.event_id,
            source_db=event.source_db,
            source_table=event.source_table,
            operation=event.operation,
            binlog_file=event.binlog_file,
            binlog_pos=event.binlog_pos,
            event_time=event.event_time.isoformat(),
            before_image=event.before_image,
            after_image=event.after_image,
            gtid=event.gtid,
            raw_format=event.raw_format,
            seq=seq,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "member_key": self.member_key,
            "seq": self.seq,
            "event_id": self.event_id,
            "source_db": self.source_db,
            "source_table": self.source_table,
            "operation": self.operation,
            "binlog_file": self.binlog_file,
            "binlog_pos": self.binlog_pos,
            "event_time": self.event_time,
            "before_image": self.before_image,
            "after_image": self.after_image,
            "gtid": self.gtid,
            "raw_format": self.raw_format,
        }


@dataclass
class TransactionGroup:
    """事务归并组：按事务ID聚合的变更集合，携带完整提交边界。

    只有收到显式提交边界（Xid/COMMIT）后 state 才是 COMMITTED；
    观察窗结束而 commit 未到时保持 OPEN，绝不伪装成完整明细。
    """
    tx_id: str
    state: str  # OPEN / COMMITTED / ROLLED_BACK
    members: list[TransactionMember] = field(default_factory=list)
    begin_file: str = ""
    begin_pos: int = 0
    commit_file: str = ""
    commit_pos: int | None = None
    gtid: str = ""
    first_seen: str = ""
    last_seen: str = ""
    tx_key: str = ""

    @staticmethod
    def build_tx_key(tx_id: str, begin_file: str, begin_pos: int) -> str:
        raw = f"{tx_id}|{begin_file}|{begin_pos}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def recompute_tx_key(self) -> str:
        self.tx_key = self.build_tx_key(self.tx_id, self.begin_file, self.begin_pos)
        return self.tx_key

    def to_dict(self, include_members: bool = True) -> dict[str, Any]:
        data = {
            "tx_key": self.tx_key,
            "tx_id": self.tx_id,
            "state": self.state,
            "member_count": len(self.members),
            "begin": {"binlog_file": self.begin_file, "binlog_pos": self.begin_pos},
            "commit": (
                {"binlog_file": self.commit_file, "binlog_pos": self.commit_pos}
                if self.state == "COMMITTED" and self.commit_pos is not None
                else None
            ),
            "gtid": self.gtid,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }
        if include_members:
            data["members"] = [m.to_dict() for m in self.members]
        return data


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }
        # 事务归并表 — change_event 消费后按事务ID聚合，导出与事务明细共用同一数据源
        self.transactions: dict[str, TransactionGroup] = {}
        self.tx_order: list[str] = []
        # 导出批次登记表 — 每次事务包导出的观察窗与实际囊括范围
        self.export_batches: list[dict[str, Any]] = []
        # 回放（导入）登记表 — 已应用事务的稳定身份，支撑重复导入幂等
        self.replayed_tx_keys: dict[str, dict[str, Any]] = {}
        self.replay_runs: list[dict[str, Any]] = []

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    # ------------------------------------------------------------------
    # 事务边界：归并、提交、回滚
    # ------------------------------------------------------------------
    def merge_transaction_event(self, event: ChangeEventRecord) -> TransactionGroup:
        """把一条已消费的变更事件按事务ID归并到事务组。

        事务起点坐标取该事务第一条变更的 (binlog_file, binlog_pos)，
        事务身份 tx_key 由事务ID与起点坐标确定性派生，重复导出保持不变。
        """
        group = self.transactions.get(event.tx_id)
        if group is None:
            group = TransactionGroup(
                tx_id=event.tx_id,
                state="OPEN",
                begin_file=event.binlog_file,
                begin_pos=event.binlog_pos,
                gtid=event.gtid,
                first_seen=event.event_time.isoformat(),
            )
            self.transactions[event.tx_id] = group
            self.tx_order.append(event.tx_id)
        elif not group.members:
            # 边界事件先到达时建立的空壳组：起点坐标与首见时间由第一条成员补齐，
            # 不能沿用提交事件坐标充当事务起点。
            group.begin_file = event.binlog_file
            group.begin_pos = event.binlog_pos
            group.first_seen = event.event_time.isoformat()
        member = TransactionMember.from_event(event, seq=len(group.members) + 1)
        # 同一来源坐标的成员重复归并（重复解析/重复消费）时保持成员集合稳定，
        # 不追加重复成员，稳定身份不随解析次数变化。
        if any(existing.member_key == member.member_key for existing in group.members):
            group.last_seen = event.event_time.isoformat()
            return group
        group.members.append(member)
        group.last_seen = event.event_time.isoformat()
        if event.gtid and not group.gtid:
            group.gtid = event.gtid
        group.recompute_tx_key()
        return group

    def mark_transaction_commit(
        self, tx_id: str, binlog_file: str, binlog_pos: int, gtid: str = "",
    ) -> TransactionGroup | None:
        """登记事务提交边界（Xid/COMMIT 事件坐标）。

        提交坐标必须显式记录；未收到提交边界的事务永远保持 OPEN。
        """
        group = self.transactions.get(tx_id)
        if group is None:
            # 提交先于变更成员到达（理论上不会发生），建立空壳 OPEN 组等待成员补齐
            group = TransactionGroup(
                tx_id=tx_id, state="OPEN",
                first_seen=datetime.now().isoformat(),
                last_seen=datetime.now().isoformat(),
            )
            self.transactions[tx_id] = group
            self.tx_order.append(tx_id)
        group.state = "COMMITTED"
        group.commit_file = binlog_file
        group.commit_pos = int(binlog_pos)
        if gtid:
            group.gtid = gtid
        # 空壳组（边界先于成员到达）的起点坐标留给首条成员补齐，
        # 这里不用提交坐标冒充事务起点。
        group.last_seen = datetime.now().isoformat()
        group.recompute_tx_key()
        return group

    def mark_transaction_rollback(
        self, tx_id: str, binlog_file: str, binlog_pos: int,
    ) -> TransactionGroup | None:
        """登记事务回滚边界：回滚事务不进入可回放明细。

        边界先于成员消费到达时建立 ROLLED_BACK 空壳组，后续成员归并不会复活状态。
        """
        group = self.transactions.get(tx_id)
        if group is None:
            group = TransactionGroup(
                tx_id=tx_id, state="ROLLED_BACK",
                commit_file=binlog_file, commit_pos=int(binlog_pos),
                first_seen=datetime.now().isoformat(),
                last_seen=datetime.now().isoformat(),
            )
            self.transactions[tx_id] = group
            self.tx_order.append(tx_id)
            group.recompute_tx_key()
            return group
        group.state = "ROLLED_BACK"
        group.commit_file = binlog_file
        group.commit_pos = int(binlog_pos)
        group.recompute_tx_key()
        return group

    def list_transactions(self) -> list[TransactionGroup]:
        return [self.transactions[tx_id] for tx_id in self.tx_order if tx_id in self.transactions]

    def get_transaction(self, tx_key: str) -> TransactionGroup | None:
        for group in self.transactions.values():
            if group.tx_key == tx_key:
                return group
        return None

    def high_watermark(self) -> tuple[str, int]:
        """当前已消费事件流的最高坐标（观察窗结束位置）。"""
        last_file, last_pos = "", 0
        for group in self.list_transactions():
            for member in group.members:
                if (member.binlog_file, member.binlog_pos) > (last_file, last_pos):
                    last_file, last_pos = member.binlog_file, member.binlog_pos
            if group.commit_pos is not None and (group.commit_file, group.commit_pos) > (last_file, last_pos):
                last_file, last_pos = group.commit_file, group.commit_pos
        return last_file, last_pos

    def register_export_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        """登记一次事务包导出的观察窗与实际囊括的事务范围。"""
        batch_no = len(self.export_batches) + 1
        record = {"batch_no": batch_no, "exported_at": datetime.now().isoformat(), **batch}
        self.export_batches.append(record)
        return record

    # ------------------------------------------------------------------
    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        committed = sum(1 for g in self.transactions.values() if g.state == "COMMITTED")
        open_tx = sum(1 for g in self.transactions.values() if g.state == "OPEN")
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
            "tx_total": len(self.transactions),
            "tx_committed": committed,
            "tx_open": open_tx,
            "export_batch_count": len(self.export_batches),
            "replayed_tx_count": len(self.replayed_tx_keys),
        }


pipeline_bus = PipelineBus()
