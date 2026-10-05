"""Turn a queue topology into findings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .model import Finding, Queue, Topology


@dataclass
class Thresholds:
    max_messages: int = 0
    max_age_hours: float = 24.0
    # A DLQ message older than this share of the retention window is about to be
    # deleted by SQS itself, which is the point of no return.
    retention_warn_ratio: float = 0.8


def _hours(seconds: int) -> float:
    return seconds / 3600.0


def _age_text(seconds: int) -> str:
    if seconds >= 86400:
        return "%.1fd" % (seconds / 86400.0)
    if seconds >= 3600:
        return "%.1fh" % _hours(seconds)
    return "%dm" % (seconds // 60)


def _backlog(queue: Queue, limit: int) -> Finding:
    severity = "high" if queue.messages > 10 * max(limit, 1) else "medium"
    return Finding(
        rule="dlq_backlog",
        severity=severity,
        queue=queue.name,
        detail="%d message(s) in the dead-letter queue (threshold %d)" % (queue.messages, limit),
        data={"messages": queue.messages, "threshold": limit},
    )


def _stale(queue: Queue, limit_hours: float, warn_ratio: float) -> Finding:
    age = queue.oldest_message_age_seconds or 0
    retention = queue.retention_seconds or 0
    doomed = retention > 0 and age >= warn_ratio * retention
    if doomed:
        severity = "high"
        detail = "oldest message is %s old, %.0f%% of the %s retention window — it is about to be dropped" % (
            _age_text(age),
            100.0 * age / retention,
            _age_text(retention),
        )
    else:
        severity = "high" if age > 3 * limit_hours * 3600 else "medium"
        detail = "oldest message is %s old (limit %.1fh)" % (_age_text(age), limit_hours)
    return Finding(
        rule="dlq_stale_messages",
        severity=severity,
        queue=queue.name,
        detail=detail,
        data={
            "oldest_message_age_seconds": age,
            "retention_seconds": retention,
            "limit_hours": limit_hours,
        },
    )


def analyze(topo: Topology, thresholds: Thresholds) -> List[Finding]:
    """Produce every finding for a resolved topology, worst first."""
    findings: List[Finding] = []

    for queue in topo.queues:
        role = topo.role(queue)

        if role == "dlq":
            if queue.messages > thresholds.max_messages:
                findings.append(_backlog(queue, thresholds.max_messages))
            age = queue.oldest_message_age_seconds
            if age is not None and queue.messages > 0 and age > thresholds.max_age_hours * 3600:
                findings.append(_stale(queue, thresholds.max_age_hours, thresholds.retention_warn_ratio))

        elif role == "orphan-dlq":
            severity = "medium" if queue.messages > 0 else "low"
            detail = "named like a dead-letter queue but no source queue redrives into it"
            if queue.messages > 0:
                detail += "; %d message(s) are stranded there" % queue.messages
            findings.append(
                Finding(
                    rule="orphan_dlq",
                    severity=severity,
                    queue=queue.name,
                    detail=detail,
                    data={"messages": queue.messages},
                )
            )

        elif role == "unprotected":
            findings.append(
                Finding(
                    rule="source_no_redrive",
                    severity="medium",
                    queue=queue.name,
                    detail="no redrive policy: a poison message loops until retention deletes it",
                    data={"retention_seconds": queue.retention_seconds},
                )
            )

    for source, target in topo.dangling:
        findings.append(
            Finding(
                rule="dangling_redrive",
                severity="high",
                queue=source,
                detail="redrive policy points at %s, which does not exist in this region" % target,
                data={"target_arn": target},
            )
        )

    findings.sort(key=lambda f: (SEVERITY_RANK.get(f.severity, 9), f.rule, f.queue))
    return findings


SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
