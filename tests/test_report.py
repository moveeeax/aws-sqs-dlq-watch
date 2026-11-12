from __future__ import annotations

import datetime as dt
import json

from sqs_dlq_watch.analyze import Thresholds, analyze
from sqs_dlq_watch.model import Queue, build_topology
from sqs_dlq_watch.report import render_json, render_text, summarize


def topo():
    return build_topology(
        [
            Queue(url="u/orders", name="orders", arn="arn:orders", messages=4,
                  redrive_target_arn="arn:orders-dlq"),
            Queue(url="u/orders-dlq", name="orders-dlq", arn="arn:orders-dlq", messages=9,
                  retention_seconds=1209600,
                  oldest_message_age_seconds=200000),
        ]
    )


def test_text_report_is_aligned_and_lists_the_sources():
    t = topo()
    out = render_text(t, analyze(t, Thresholds()), "eu-central-1")
    lines = out.splitlines()
    assert lines[0].startswith("SQS dead-letter report — eu-central-1 — 2 queue(s)")
    header = [ln for ln in lines if ln.startswith("QUEUE")][0]
    assert "FED BY" in header
    dlq_row = [ln for ln in lines if ln.startswith("orders-dlq")][0]
    assert "DLQ" in dlq_row and "orders" in dlq_row
    assert "2.3d" in dlq_row
    assert "finding(s)" in lines[-1]


def test_text_report_says_so_when_there_is_nothing_to_report():
    t = build_topology([Queue(url="u/a", name="a", arn="arn:a", redrive_target_arn="arn:b"),
                        Queue(url="u/b", name="b", arn="arn:b")])
    assert "No findings." in render_text(t, [], None)
    assert "default region" in render_text(t, [], None)


def test_json_report_round_trips():
    t = topo()
    now = dt.datetime(2026, 10, 5, 9, 0, tzinfo=dt.timezone.utc)
    payload = json.loads(render_json(t, analyze(t, Thresholds()), "eu-central-1", now=now))
    assert payload["generated_at"] == "2026-10-05T09:00:00+00:00"
    assert payload["region"] == "eu-central-1"
    assert payload["summary"]["dead_letter_queues"] == 1
    assert payload["queues"][0]["name"] == "orders"
    assert payload["findings"][0]["rule"] in {"dlq_backlog", "dlq_stale_messages"}


def test_summarize_counts_by_severity():
    t = topo()
    findings = analyze(t, Thresholds())
    counts = summarize(findings)
    assert sum(counts.values()) == len(findings)
    assert counts == {"high": 0, "medium": 2, "low": 0}
