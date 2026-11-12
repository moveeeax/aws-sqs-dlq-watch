"""Read queues and their attributes out of SQS (and the oldest-message age out of
CloudWatch or a zero-visibility peek)."""

from __future__ import annotations

import datetime as _dt
from typing import Any, Callable, Dict, Iterable, List, Optional

from .model import Queue, matches_any, parse_redrive_allow_policy, parse_redrive_policy

AGE_SOURCES = ("cloudwatch", "peek", "none")


class AwsError(RuntimeError):
    """Any failure that came back from the AWS side of the call."""


def _call(fn: Callable[..., Any], **kwargs: Any) -> Dict[str, Any]:
    try:
        return fn(**kwargs)
    except AwsError:
        raise
    except Exception as exc:  # botocore ClientError, EndpointConnectionError, ...
        raise AwsError("%s: %s" % (getattr(fn, "__name__", "aws call"), exc)) from exc


def make_clients(region: Optional[str], profile: Optional[str]):
    """Build (sqs, cloudwatch) boto3 clients. Imported lazily so the analyzer and
    its tests stay runnable without boto3 installed."""
    import boto3  # noqa: PLC0415 - deliberate lazy import

    session = boto3.session.Session(profile_name=profile, region_name=region)
    return session.client("sqs"), session.client("cloudwatch")


def list_queue_urls(sqs, prefix: Optional[str] = None) -> List[str]:
    """Page through ListQueues. Written against the raw API rather than a paginator
    so a stub client only has to implement one method."""
    urls: List[str] = []
    token: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"MaxResults": 1000}
        if prefix:
            kwargs["QueueNamePrefix"] = prefix
        if token:
            kwargs["NextToken"] = token
        page = _call(sqs.list_queues, **kwargs) or {}
        urls.extend(page.get("QueueUrls") or [])
        token = page.get("NextToken")
        if not token:
            break
    return urls


def _int(attrs: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(attrs.get(key, default))
    except (TypeError, ValueError):
        return default


def describe_queue(sqs, url: str) -> Queue:
    attrs = (_call(sqs.get_queue_attributes, QueueUrl=url, AttributeNames=["All"]) or {}).get(
        "Attributes", {}
    )
    name = url.rstrip("/").rsplit("/", 1)[-1]
    target, max_receive = parse_redrive_policy(attrs.get("RedrivePolicy"))
    allow_permission, allow_sources = parse_redrive_allow_policy(attrs.get("RedriveAllowPolicy"))
    return Queue(
        url=url,
        name=name,
        arn=attrs.get("QueueArn") or "arn:unknown:%s" % name,
        messages=_int(attrs, "ApproximateNumberOfMessages"),
        messages_in_flight=_int(attrs, "ApproximateNumberOfMessagesNotVisible"),
        retention_seconds=_int(attrs, "MessageRetentionPeriod", 345600),
        is_fifo=str(attrs.get("FifoQueue", "false")).lower() == "true",
        redrive_target_arn=target,
        max_receive_count=max_receive,
        redrive_allow_permission=allow_permission,
        redrive_allow_source_arns=allow_sources,
    )


def age_from_cloudwatch(cw, queue: Queue, now: Optional[_dt.datetime] = None) -> Optional[int]:
    """ApproximateAgeOfOldestMessage is a CloudWatch metric, not an SQS attribute.
    Returns None when the metric is missing or the call is not permitted."""
    now = now or _dt.datetime.now(_dt.timezone.utc)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName="ApproximateAgeOfOldestMessage",
            Dimensions=[{"Name": "QueueName", "Value": queue.name}],
            StartTime=now - _dt.timedelta(minutes=30),
            EndTime=now,
            Period=300,
            Statistics=["Maximum"],
        )
    except Exception:
        return None
    points = (resp or {}).get("Datapoints") or []
    if not points:
        return None
    newest = max(points, key=lambda p: p.get("Timestamp") or now)
    try:
        return int(newest["Maximum"])
    except (KeyError, TypeError, ValueError):
        return None


def age_from_peek(sqs, queue: Queue, now: Optional[_dt.datetime] = None) -> Optional[int]:
    """Receive up to 10 messages with VisibilityTimeout=0 and take the oldest
    SentTimestamp. This does not consume anything, but a standard queue gives no
    ordering guarantee, so the result is a lower bound on the real oldest age."""
    now = now or _dt.datetime.now(_dt.timezone.utc)
    try:
        resp = sqs.receive_message(
            QueueUrl=queue.url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=0,
            WaitTimeSeconds=0,
            AttributeNames=["SentTimestamp"],
        )
    except Exception:
        return None
    stamps: List[int] = []
    for msg in (resp or {}).get("Messages") or []:
        raw = (msg.get("Attributes") or {}).get("SentTimestamp")
        try:
            stamps.append(int(raw))
        except (TypeError, ValueError):
            continue
    if not stamps:
        return None
    epoch_ms = int(now.timestamp() * 1000)
    return max(0, (epoch_ms - min(stamps)) // 1000)


def collect(
    sqs,
    cw=None,
    age_source: str = "cloudwatch",
    ignore: Iterable[str] = (),
    prefix: Optional[str] = None,
    dlq_arns_for_age: Optional[Iterable[str]] = None,
    now: Optional[_dt.datetime] = None,
) -> List[Queue]:
    """Describe every queue in the region, skipping names matched by ``ignore``.

    Ages are normally filled in later by :func:`fill_ages`, once the topology says
    which queues are dead-letter queues. Pass ``dlq_arns_for_age`` to do it here
    instead when the set is already known.
    """
    ignore = list(ignore)
    queues = []
    for url in list_queue_urls(sqs, prefix=prefix):
        name = url.rstrip("/").rsplit("/", 1)[-1]
        if ignore and matches_any(name, ignore):
            continue
        queues.append(describe_queue(sqs, url))
    if dlq_arns_for_age is not None:
        fill_ages(queues, sqs, cw, age_source, set(dlq_arns_for_age), now=now)
    return queues


def fill_ages(
    queues: Iterable[Queue],
    sqs,
    cw,
    age_source: str,
    only_arns: Optional[set] = None,
    now: Optional[_dt.datetime] = None,
) -> None:
    """Attach ``oldest_message_age_seconds`` in place. Queues with no messages are
    skipped: there is nothing to age, and peeking an empty queue costs a request."""
    if age_source == "none":
        return
    for queue in queues:
        if only_arns is not None and queue.arn not in only_arns:
            continue
        if queue.messages <= 0:
            continue
        if age_source == "cloudwatch" and cw is not None:
            queue.oldest_message_age_seconds = age_from_cloudwatch(cw, queue, now=now)
        elif age_source == "peek":
            queue.oldest_message_age_seconds = age_from_peek(sqs, queue, now=now)
