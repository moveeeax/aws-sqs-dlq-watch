from __future__ import annotations

import pytest

from sqs_dlq_watch.collect import (
    AwsError,
    age_from_cloudwatch,
    age_from_peek,
    collect,
    fill_ages,
    list_queue_urls,
)
from sqs_dlq_watch.model import build_topology
from sqs_dlq_watch.testing import FIXED_NOW, FakeCloudWatch, FakeSQS


def test_list_queue_urls_follows_the_pagination_token(clients):
    sqs, _ = clients
    urls = list_queue_urls(sqs)
    assert len(urls) == 8
    assert sqs.calls.count("ListQueues") == 4  # 8 queues at page_size=2


def test_prefix_is_pushed_down_to_the_api(clients):
    sqs, _ = clients
    urls = list_queue_urls(sqs, prefix="orders")
    assert [u.rsplit("/", 1)[-1] for u in urls] == ["orders", "orders-dlq"]


def test_collect_reads_attributes_and_redrive_policies(clients):
    sqs, _ = clients
    queues = {q.name: q for q in collect(sqs, age_source="none")}
    assert queues["orders"].redrive_target_arn == sqs.arn("orders-dlq")
    assert queues["orders"].max_receive_count == 5
    assert queues["orders-dlq"].messages == 418
    assert queues["orders"].messages_in_flight == 2
    assert queues["orders-dlq"].retention_seconds == 345600
    assert queues["webhooks"].redrive_target_arn is None


def test_ignore_globs_skip_queues_before_they_are_described(clients):
    sqs, _ = clients
    names = {q.name for q in collect(sqs, age_source="none", ignore=["*-dlq", "webhooks"])}
    assert names == {"orders", "invoices", "receipts", "payments"}


def test_an_sqs_error_becomes_an_aws_error(spec):
    sqs = FakeSQS(spec, fail_on="GetQueueAttributes")
    with pytest.raises(AwsError) as exc:
        collect(sqs, age_source="none")
    assert "AccessDenied" in str(exc.value)


def test_age_from_cloudwatch_reads_the_metric(clients):
    sqs, cw = clients
    queue = [q for q in collect(sqs, age_source="none") if q.name == "orders-dlq"][0]
    assert age_from_cloudwatch(cw, queue, now=FIXED_NOW) == 309000


def test_age_from_cloudwatch_degrades_to_none_without_permission(clients, spec):
    sqs, _ = clients
    cw = FakeCloudWatch(sqs, fail=True)
    queue = [q for q in collect(sqs, age_source="none") if q.name == "orders-dlq"][0]
    assert age_from_cloudwatch(cw, queue, now=FIXED_NOW) is None


def test_age_from_peek_takes_the_oldest_sent_timestamp(clients):
    sqs, _ = clients
    queue = [q for q in collect(sqs, age_source="none") if q.name == "orders-dlq"][0]
    assert age_from_peek(sqs, queue, now=FIXED_NOW) == 309000


def test_fill_ages_only_touches_the_requested_queues(clients):
    sqs, cw = clients
    queues = collect(sqs, age_source="none")
    topo = build_topology(queues)
    fill_ages(queues, sqs, cw, "cloudwatch", only_arns=set(topo.dlq_sources))
    ages = {q.name: q.oldest_message_age_seconds for q in queues}
    assert ages["orders-dlq"] == 309000
    assert ages["shared-dlq"] == 5400
    assert ages["webhooks"] is None  # not a DLQ
    assert ages["legacy-import-dlq"] is None  # orphan, not requested here


def test_fill_ages_none_source_asks_nothing(clients):
    sqs, cw = clients
    queues = collect(sqs, age_source="none")
    fill_ages(queues, sqs, cw, "none")
    assert cw.calls == []
    assert all(q.oldest_message_age_seconds is None for q in queues)


def test_empty_queues_are_never_peeked(clients):
    sqs, cw = clients
    queues = collect(sqs, age_source="none")
    fill_ages(queues, sqs, cw, "peek", only_arns={q.arn for q in queues})
    assert "ReceiveMessage" in sqs.calls
    peeked = sqs.calls.count("ReceiveMessage")
    assert peeked == len([q for q in queues if q.messages > 0])
