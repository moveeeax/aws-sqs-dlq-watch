"""Offline stand-ins for the SQS and CloudWatch clients.

They live in the package rather than in ``tests/`` so the example script can build
the exact same scenario the test suite asserts on — the README output is produced
by running real code, not pasted by hand.
"""

from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Dict, List, Optional

FIXED_NOW = _dt.datetime(2026, 10, 5, 12, 0, 0, tzinfo=_dt.timezone.utc)


class FakeSQS:
    """A tiny SQS stub: ListQueues (paginated), GetQueueAttributes, ReceiveMessage."""

    def __init__(
        self,
        spec: Dict[str, Any],
        page_size: int = 2,
        now: Optional[_dt.datetime] = None,
        fail_on: Optional[str] = None,
    ) -> None:
        self.region = spec.get("region", "eu-central-1")
        self.account = str(spec.get("account", "123456789012"))
        self.page_size = page_size
        self.now = now or FIXED_NOW
        self.fail_on = fail_on
        self.calls: List[str] = []
        self.queues: Dict[str, Dict[str, Any]] = {}
        for raw in spec.get("queues", []):
            self.queues[raw["name"]] = raw

    # -- helpers ---------------------------------------------------------
    def url(self, name: str) -> str:
        return "https://sqs.%s.amazonaws.com/%s/%s" % (self.region, self.account, name)

    def arn(self, name: str) -> str:
        return "arn:aws:sqs:%s:%s:%s" % (self.region, self.account, name)

    def _name_from_url(self, url: str) -> str:
        return url.rstrip("/").rsplit("/", 1)[-1]

    def _boom(self, op: str) -> None:
        self.calls.append(op)
        if self.fail_on == op:
            raise RuntimeError("AccessDenied: not authorized to perform sqs:%s" % op)

    # -- API -------------------------------------------------------------
    def list_queues(self, **kwargs: Any) -> Dict[str, Any]:
        self._boom("ListQueues")
        names = sorted(self.queues)
        prefix = kwargs.get("QueueNamePrefix")
        if prefix:
            names = [n for n in names if n.startswith(prefix)]
        start = int(kwargs.get("NextToken") or 0)
        page = names[start : start + self.page_size]
        out: Dict[str, Any] = {"QueueUrls": [self.url(n) for n in page]}
        if start + self.page_size < len(names):
            out["NextToken"] = str(start + self.page_size)
        return out

    def get_queue_attributes(self, **kwargs: Any) -> Dict[str, Any]:
        self._boom("GetQueueAttributes")
        name = self._name_from_url(kwargs["QueueUrl"])
        raw = self.queues[name]
        attrs = {
            "QueueArn": self.arn(name),
            "ApproximateNumberOfMessages": str(raw.get("messages", 0)),
            "ApproximateNumberOfMessagesNotVisible": str(raw.get("in_flight", 0)),
            "MessageRetentionPeriod": str(raw.get("retention_seconds", 345600)),
        }
        if raw.get("fifo"):
            attrs["FifoQueue"] = "true"
        redrive = raw.get("redrive")
        if redrive:
            target = redrive.get("target_arn") or self.arn(redrive["target"])
            attrs["RedrivePolicy"] = json.dumps(
                {
                    "deadLetterTargetArn": target,
                    "maxReceiveCount": redrive.get("maxReceiveCount", 5),
                }
            )
        if raw.get("broken_redrive"):
            attrs["RedrivePolicy"] = raw["broken_redrive"]
        return {"Attributes": attrs}

    def receive_message(self, **kwargs: Any) -> Dict[str, Any]:
        self._boom("ReceiveMessage")
        name = self._name_from_url(kwargs["QueueUrl"])
        raw = self.queues[name]
        age = raw.get("oldest_age_seconds")
        if not raw.get("messages") or age is None:
            return {}
        epoch_ms = int(self.now.timestamp() * 1000)
        # The newest visible message is deliberately much younger, which is how a
        # real peek behaves: the batch is a sample, not a sorted window.
        ages = [age, max(0, age // 4)]
        return {
            "Messages": [
                {"MessageId": "%s-%d" % (name, i), "Attributes": {"SentTimestamp": str(epoch_ms - a * 1000)}}
                for i, a in enumerate(ages)
            ]
        }


class FakeCloudWatch:
    """Serves ApproximateAgeOfOldestMessage from the same fixture."""

    def __init__(self, sqs: FakeSQS, fail: bool = False) -> None:
        self.sqs = sqs
        self.fail = fail
        self.calls: List[str] = []

    def get_metric_statistics(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs.get("MetricName", ""))
        if self.fail:
            raise RuntimeError("AccessDenied: cloudwatch:GetMetricStatistics")
        name = kwargs["Dimensions"][0]["Value"]
        raw = self.sqs.queues.get(name, {})
        age = raw.get("oldest_age_seconds")
        if age is None:
            return {"Datapoints": []}
        return {
            "Datapoints": [
                {"Timestamp": self.sqs.now - _dt.timedelta(minutes=5), "Maximum": float(age)}
            ]
        }


def load_spec(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def clients_from_spec(spec: Dict[str, Any], **kwargs: Any):
    sqs = FakeSQS(spec, **kwargs)
    return sqs, FakeCloudWatch(sqs)
