"""Text table and JSON renderers."""

from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Dict, List, Optional

from .model import Finding, Topology

ROLE_LABEL = {
    "dlq": "DLQ",
    "source": "source",
    "orphan-dlq": "orphan DLQ",
    "unprotected": "no redrive",
}


def _age(seconds: Optional[int]) -> str:
    if seconds is None:
        return "-"
    if seconds >= 86400:
        return "%.1fd" % (seconds / 86400.0)
    if seconds >= 3600:
        return "%.1fh" % (seconds / 3600.0)
    return "%dm" % (seconds // 60)


def _table(rows: List[List[str]], headers: List[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip()
    out = [line, "  ".join("-" * w for w in widths).rstrip()]
    for row in rows:
        out.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip())
    return "\n".join(out)


def render_text(topo: Topology, findings: List[Finding], region: Optional[str]) -> str:
    chunks: List[str] = []
    where = region or "default region"
    chunks.append("SQS dead-letter report — %s — %d queue(s)" % (where, len(topo.queues)))

    rows = []
    for queue in sorted(topo.queues, key=lambda q: q.name):
        role = topo.role(queue)
        sources = topo.dlq_sources.get(queue.arn, [])
        feeds = ", ".join(sources) if sources else "-"
        rows.append(
            [
                queue.name,
                ROLE_LABEL[role],
                str(queue.messages),
                str(queue.messages_in_flight),
                _age(queue.oldest_message_age_seconds),
                feeds,
            ]
        )
    if rows:
        chunks.append(
            _table(rows, ["QUEUE", "ROLE", "MSGS", "FLIGHT", "OLDEST", "FED BY"])
        )

    if findings:
        frows = [[f.severity.upper(), f.rule, f.queue, f.detail] for f in findings]
        chunks.append(_table(frows, ["SEV", "RULE", "QUEUE", "DETAIL"]))
        counts = summarize(findings)
        chunks.append(
            "%d finding(s): %d high, %d medium, %d low"
            % (len(findings), counts["high"], counts["medium"], counts["low"])
        )
    else:
        chunks.append("No findings.")
    return "\n\n".join(chunks)


def summarize(findings: List[Finding]) -> Dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for f in findings:
        if f.severity in counts:
            counts[f.severity] += 1
    return counts


def render_json(
    topo: Topology,
    findings: List[Finding],
    region: Optional[str],
    now: Optional[_dt.datetime] = None,
) -> str:
    now = now or _dt.datetime.now(_dt.timezone.utc)
    payload: Dict[str, Any] = {
        "region": region,
        "generated_at": now.replace(microsecond=0).isoformat(),
        "summary": dict(
            summarize(findings),
            queues=len(topo.queues),
            dead_letter_queues=len(topo.dlq_sources),
            findings=len(findings),
        ),
        "queues": [
            dict(q.to_dict(), role=topo.role(q), fed_by=topo.dlq_sources.get(q.arn, []))
            for q in sorted(topo.queues, key=lambda q: q.name)
        ],
        "findings": [f.to_dict() for f in findings],
    }
    return json.dumps(payload, indent=2, sort_keys=False)
