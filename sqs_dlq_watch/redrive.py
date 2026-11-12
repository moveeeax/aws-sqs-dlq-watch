"""Move messages out of a dead-letter queue with SQS's own redrive API
(StartMessageMoveTask) and show the state of those tasks (ListMessageMoveTasks).

Everything that can be checked before the call is checked here, so a refused plan
never reaches StartMessageMoveTask.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .collect import _call, describe_queue
from .model import Queue
from .report import _table

# StartMessageMoveTask accepts at most 500 messages per second.
MAX_MESSAGES_PER_SECOND = 500
# ListMessageMoveTasks returns the latest task only unless asked for more; 10 is the cap.
MAX_TASKS_LISTED = 10


def resolve_queue(sqs, ref: str) -> Queue:
    """Look a queue up by name or ARN and read its attributes."""
    kwargs: Dict[str, Any] = {}
    if ref.startswith("arn:"):
        parts = ref.split(":")
        if len(parts) != 6 or not parts[5]:
            raise ValueError("not an SQS queue ARN: %s" % ref)
        kwargs["QueueName"] = parts[5]
        if parts[4]:
            kwargs["QueueOwnerAWSAccountId"] = parts[4]
    else:
        kwargs["QueueName"] = ref
    url = (_call(sqs.get_queue_url, **kwargs) or {}).get("QueueUrl")
    if not url:
        raise ValueError("queue not found: %s" % ref)
    return describe_queue(sqs, url)


def dead_letter_sources(sqs, queue: Queue) -> List[str]:
    """Names of the queues whose redrive policy points at ``queue``."""
    names: List[str] = []
    token: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"QueueUrl": queue.url, "MaxResults": 1000}
        if token:
            kwargs["NextToken"] = token
        page = _call(sqs.list_dead_letter_source_queues, **kwargs) or {}
        names.extend(u.rstrip("/").rsplit("/", 1)[-1] for u in page.get("queueUrls") or [])
        token = page.get("NextToken")
        if not token:
            break
    return sorted(names)


def list_tasks(sqs, queue: Queue) -> List[Dict[str, Any]]:
    """The most recent move tasks of one DLQ, newest first, as SQS returns them."""
    resp = _call(sqs.list_message_move_tasks, SourceArn=queue.arn, MaxResults=MAX_TASKS_LISTED)
    return list((resp or {}).get("Results") or [])


@dataclass
class Plan:
    """What a redrive would do, and every reason it must not."""

    dlq: Queue
    destination: Optional[Queue] = None
    sources: List[str] = field(default_factory=list)
    max_per_second: Optional[int] = None
    running_task: Optional[Dict[str, Any]] = None
    problems: List[str] = field(default_factory=list)

    def request(self) -> Dict[str, Any]:
        """The StartMessageMoveTask arguments for this plan."""
        kwargs: Dict[str, Any] = {"SourceArn": self.dlq.arn}
        if self.destination is not None:
            kwargs["DestinationArn"] = self.destination.arn
        if self.max_per_second is not None:
            kwargs["MaxNumberOfMessagesPerSecond"] = self.max_per_second
        return kwargs

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue": self.dlq.name,
            "queue_arn": self.dlq.arn,
            "messages": self.dlq.messages,
            "destination": self.destination.name if self.destination else None,
            "destination_arn": self.destination.arn if self.destination else None,
            "sources": self.sources,
            "max_messages_per_second": self.max_per_second,
            "problems": self.problems,
        }


def plan_redrive(
    sqs, queue_ref: str, to_ref: Optional[str] = None, max_per_second: Optional[int] = None
) -> Plan:
    dlq = resolve_queue(sqs, queue_ref)
    plan = Plan(dlq=dlq, max_per_second=max_per_second)
    plan.sources = dead_letter_sources(sqs, dlq)

    if to_ref:
        plan.destination = resolve_queue(sqs, to_ref)
        dest = plan.destination
        if dest.arn == dlq.arn:
            plan.problems.append("the destination is the dead-letter queue itself")
        elif dest.is_fifo != dlq.is_fifo:
            plan.problems.append(
                "%s is a %s queue but %s is %s; redrive needs both of the same type"
                % (dlq.name, _kind(dlq), dest.name, _kind(dest))
            )
    elif not plan.sources:
        # Without a destination SQS sends each message back where it came from,
        # which only works for a queue that some source actually dead-letters into.
        plan.problems.append(
            "no source queue redrives into %s; pass --to to choose a destination" % dlq.name
        )

    if dlq.messages <= 0:
        plan.problems.append("%s holds no visible messages" % dlq.name)

    for task in list_tasks(sqs, dlq):
        if task.get("Status") in ("RUNNING", "CANCELLING"):
            plan.running_task = task
            plan.problems.append(
                "a move task is already %s on %s (one active task per queue)"
                % (task["Status"].lower(), dlq.name)
            )
            break
    return plan


def start_redrive(sqs, plan: Plan) -> str:
    """Call StartMessageMoveTask and return the task handle."""
    resp = _call(sqs.start_message_move_task, **plan.request()) or {}
    return resp.get("TaskHandle") or ""


def _kind(queue: Queue) -> str:
    return "FIFO" if queue.is_fifo else "standard"


def _started(ms: Any) -> str:
    try:
        stamp = _dt.datetime.fromtimestamp(int(ms) / 1000.0, tz=_dt.timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return "-"
    return stamp.strftime("%Y-%m-%d %H:%M UTC")


def _name(arn: Optional[str]) -> str:
    return arn.rsplit(":", 1)[-1] if arn else ""


def render_plan_text(plan: Plan, region: Optional[str]) -> str:
    where = region or "default region"
    if plan.destination is not None:
        dest = plan.destination.name
    elif plan.sources:
        dest = "original source queue(s): %s" % ", ".join(plan.sources)
    else:
        dest = "original source queue(s): none found"
    rate = (
        "%d message(s)/s" % plan.max_per_second
        if plan.max_per_second is not None
        else "chosen by SQS"
    )
    lines = [
        "SQS redrive plan — %s" % where,
        "",
        "  from:  %s (%d message(s))" % (plan.dlq.name, plan.dlq.messages),
        "  to:    %s" % dest,
        "  rate:  %s" % rate,
    ]
    if plan.problems:
        lines.append("")
        lines.extend("  refused: %s" % p for p in plan.problems)
    return "\n".join(lines)


def render_tasks_text(rows: List[Tuple[Queue, Dict[str, Any]]], region: Optional[str]) -> str:
    where = region or "default region"
    head = "SQS redrive tasks — %s" % where
    if not rows:
        return head + "\n\nNo redrive tasks."
    table = []
    for queue, task in rows:
        moved = task.get("ApproximateNumberOfMessagesMoved")
        total = task.get("ApproximateNumberOfMessagesToMove")
        rate = task.get("MaxNumberOfMessagesPerSecond")
        table.append(
            [
                queue.name,
                str(task.get("Status") or "-"),
                "%s/%s" % ("-" if moved is None else moved, "-" if total is None else total),
                "%s/s" % rate if rate is not None else "auto",
                _started(task.get("StartedTimestamp")),
                _name(task.get("DestinationArn")) or "original source",
                str(task.get("FailureReason") or ""),
            ]
        )
    return "\n\n".join(
        [
            head,
            _table(table, ["QUEUE", "STATUS", "MOVED", "RATE", "STARTED", "DESTINATION", "FAILURE"]),
        ]
    )


def render_tasks_json(rows: List[Tuple[Queue, Dict[str, Any]]], region: Optional[str]) -> str:
    return json.dumps(
        {
            "region": region,
            "tasks": [dict(task, queue=queue.name) for queue, task in rows],
        },
        indent=2,
    )
