"""Turn a queue topology into findings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .model import Finding, Queue, Topology


@dataclass
class Thresholds:
    max_messages: int = 0
    max_age_hours: float = 24.0
    # A DLQ message older than this share of the retention window is about to be
    # deleted by SQS itself, which is the point of no return.
    retention_warn_ratio: float = 0.8
    # A maxReceiveCount below this sends messages to the DLQ on transient failures.
    min_receive_count: int = 3


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


def _redrive_pairs(topo: Topology) -> List[Tuple[Queue, Queue]]:
    """(source, dlq) for every redrive policy whose target exists in this region."""
    by_arn = topo.by_arn()
    pairs = []
    for queue in topo.queues:
        dlq = by_arn.get(queue.redrive_target_arn) if queue.redrive_target_arn else None
        if dlq is not None:
            pairs.append((queue, dlq))
    return pairs


def _policy_findings(topo: Topology, thresholds: Thresholds) -> List[Finding]:
    """Checks on the redrive configuration itself, independent of what the queues hold."""
    findings: List[Finding] = []
    # DLQ name -> (dlq, sources) for the rules whose fix lives on the DLQ.
    short_retention: Dict[str, Tuple[Queue, List[Queue]]] = {}
    blocked: Dict[str, Tuple[Queue, List[Queue]]] = {}

    for source, dlq in _redrive_pairs(topo):
        count = source.max_receive_count
        if count is not None and count < thresholds.min_receive_count:
            findings.append(
                Finding(
                    rule="redrive_low_max_receive",
                    severity="medium",
                    queue=source.name,
                    detail="maxReceiveCount is %d (minimum %d): %d failed receive(s) move a message to %s, so transient errors land in the DLQ"
                    % (count, thresholds.min_receive_count, count, dlq.name),
                    data={
                        "max_receive_count": count,
                        "min_receive_count": thresholds.min_receive_count,
                        "dead_letter_queue": dlq.name,
                    },
                )
            )

        if source.is_fifo != dlq.is_fifo:
            findings.append(
                Finding(
                    rule="dlq_type_mismatch",
                    severity="high",
                    queue=source.name,
                    detail="%s queue redrives into %s queue %s; SQS requires both to be the same type"
                    % (_kind(source), _kind(dlq), dlq.name),
                    data={
                        "fifo": source.is_fifo,
                        "dead_letter_queue": dlq.name,
                        "dead_letter_queue_fifo": dlq.is_fifo,
                    },
                )
            )
        elif not dlq.is_fifo and dlq.retention_seconds <= source.retention_seconds:
            # Standard queues keep the original enqueue timestamp when a message is
            # dead-lettered, so time spent in the source counts against the DLQ's
            # retention. FIFO queues reset it, so the rule does not apply there.
            short_retention.setdefault(dlq.name, (dlq, []))[1].append(source)

        if not dlq.allows_source(source.arn):
            blocked.setdefault(dlq.name, (dlq, []))[1].append(source)

    for dlq, sources in short_retention.values():
        findings.append(
            Finding(
                rule="dlq_retention_shorter",
                severity="medium",
                queue=dlq.name,
                detail="retention %s is not longer than source(s) %s; messages keep their original enqueue time and expire here early"
                % (
                    _age_text(dlq.retention_seconds),
                    ", ".join("%s (%s)" % (s.name, _age_text(s.retention_seconds)) for s in sources),
                ),
                data={
                    "retention_seconds": dlq.retention_seconds,
                    "sources": {s.name: s.retention_seconds for s in sources},
                },
            )
        )

    for dlq, sources in blocked.values():
        findings.append(
            Finding(
                rule="redrive_allow_blocks",
                severity="high",
                queue=dlq.name,
                detail="RedriveAllowPolicy (%s) does not allow source(s) %s that point at this queue"
                % (dlq.redrive_allow_permission, ", ".join(s.name for s in sources)),
                data={
                    "redrive_permission": dlq.redrive_allow_permission,
                    "allowed_source_arns": list(dlq.redrive_allow_source_arns),
                    "blocked_sources": [s.name for s in sources],
                },
            )
        )
    return findings


def _kind(queue: Queue) -> str:
    return "FIFO" if queue.is_fifo else "standard"


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

    findings.extend(_policy_findings(topo, thresholds))

    findings.sort(key=lambda f: (SEVERITY_RANK.get(f.severity, 9), f.rule, f.queue))
    return findings


SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
