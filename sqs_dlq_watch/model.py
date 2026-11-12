"""Data model shared by the collector, the analyzer and the reporters."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}

# Name patterns that make a queue *look* like a dead-letter queue. Used only to
# detect orphans: a queue that nobody redrives into but was clearly built as one.
DLQ_NAME_HINTS = ("-dlq", "_dlq", "dlq-", "dlq_", "-dead-letter", "-deadletter", "-failed")


@dataclass
class Queue:
    """One SQS queue plus the attributes we care about."""

    url: str
    name: str
    arn: str
    messages: int = 0
    messages_in_flight: int = 0
    retention_seconds: int = 345600  # SQS default: 4 days
    is_fifo: bool = False
    redrive_target_arn: Optional[str] = None
    max_receive_count: Optional[int] = None
    # RedriveAllowPolicy of this queue in its role as a DLQ. None means the attribute
    # is absent or unreadable, which SQS treats as allowAll.
    redrive_allow_permission: Optional[str] = None
    redrive_allow_source_arns: List[str] = field(default_factory=list)
    oldest_message_age_seconds: Optional[int] = None

    def allows_source(self, source_arn: str) -> bool:
        """Whether this queue's RedriveAllowPolicy lets ``source_arn`` use it as a DLQ."""
        if self.redrive_allow_permission == "denyAll":
            return False
        if self.redrive_allow_permission == "byQueue":
            return source_arn in self.redrive_allow_source_arns
        return True

    @property
    def looks_like_dlq(self) -> bool:
        low = self.name.lower()
        return any(hint in low for hint in DLQ_NAME_HINTS)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "url": self.url,
            "arn": self.arn,
            "messages": self.messages,
            "messages_in_flight": self.messages_in_flight,
            "retention_seconds": self.retention_seconds,
            "fifo": self.is_fifo,
            "redrive_target_arn": self.redrive_target_arn,
            "max_receive_count": self.max_receive_count,
            "redrive_allow_permission": self.redrive_allow_permission,
            "redrive_allow_source_arns": list(self.redrive_allow_source_arns),
            "oldest_message_age_seconds": self.oldest_message_age_seconds,
        }


@dataclass
class Finding:
    """A single problem worth a human's attention."""

    rule: str
    severity: str
    queue: str
    detail: str
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = {
            "rule": self.rule,
            "severity": self.severity,
            "queue": self.queue,
            "detail": self.detail,
        }
        if self.data:
            out["data"] = self.data
        return out


@dataclass
class Topology:
    """Who redrives into whom, resolved from the RedrivePolicy of every queue."""

    queues: List[Queue]
    # dead-letter queue ARN -> names of the source queues that point at it
    dlq_sources: Dict[str, List[str]] = field(default_factory=dict)
    # (source queue name, target ARN) pairs whose target does not exist here
    dangling: List[Tuple[str, str]] = field(default_factory=list)

    def by_arn(self) -> Dict[str, Queue]:
        return {q.arn: q for q in self.queues}

    def is_dlq(self, queue: Queue) -> bool:
        return queue.arn in self.dlq_sources

    def role(self, queue: Queue) -> str:
        if self.is_dlq(queue):
            return "dlq"
        if queue.redrive_target_arn:
            return "source"
        if queue.looks_like_dlq:
            return "orphan-dlq"
        return "unprotected"


def parse_redrive_policy(raw: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """Pull (deadLetterTargetArn, maxReceiveCount) out of the JSON blob SQS returns.

    SQS stores RedrivePolicy as a JSON *string* inside the attribute map, and a
    malformed or half-written policy is a real thing to hit, so bad JSON degrades
    to "no redrive" instead of exploding the run.
    """
    if not raw:
        return None, None
    try:
        policy = json.loads(raw)
    except (ValueError, TypeError):
        return None, None
    if not isinstance(policy, dict):
        return None, None
    target = policy.get("deadLetterTargetArn") or None
    count = policy.get("maxReceiveCount")
    try:
        count = int(count) if count is not None else None
    except (TypeError, ValueError):
        count = None
    return target, count


def parse_redrive_allow_policy(raw: Optional[str]) -> Tuple[Optional[str], List[str]]:
    """Pull (redrivePermission, sourceQueueArns) out of the RedriveAllowPolicy blob.

    Like the redrive policy, bad JSON degrades to (None, []), which callers read as
    the SQS default of allowAll rather than inventing a denial.
    """
    if not raw:
        return None, []
    try:
        policy = json.loads(raw)
    except (ValueError, TypeError):
        return None, []
    if not isinstance(policy, dict):
        return None, []
    permission = policy.get("redrivePermission")
    if permission not in ("allowAll", "denyAll", "byQueue"):
        permission = None
    arns = policy.get("sourceQueueArns") or []
    if not isinstance(arns, list):
        arns = []
    return permission, [str(a) for a in arns]


def build_topology(queues: Iterable[Queue]) -> Topology:
    """Resolve DLQ relationships from the redrive policies of the source queues."""
    queues = list(queues)
    topo = Topology(queues=queues)
    known = {q.arn for q in queues}
    for q in queues:
        target = q.redrive_target_arn
        if not target:
            continue
        if target in known:
            topo.dlq_sources.setdefault(target, []).append(q.name)
        else:
            topo.dangling.append((q.name, target))
    for sources in topo.dlq_sources.values():
        sources.sort()
    topo.dangling.sort()
    return topo


def matches_any(name: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)
