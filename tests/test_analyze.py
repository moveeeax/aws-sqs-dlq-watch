from __future__ import annotations

from sqs_dlq_watch.analyze import Thresholds, analyze
from sqs_dlq_watch.model import Queue, build_topology


def q(name, **kw):
    return Queue(url="https://sqs/1/%s" % name, name=name, arn="arn:%s" % name, **kw)


def rules(findings):
    return sorted((f.rule, f.queue) for f in findings)


def test_clean_account_has_no_findings():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), q("orders-dlq", messages=0)]
    )
    assert analyze(topo, Thresholds()) == []


def test_non_empty_dlq_is_a_finding():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), q("orders-dlq", messages=3)]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("dlq_backlog", "orders-dlq")]
    assert found[0].severity == "medium"
    assert found[0].data["messages"] == 3


def test_a_big_backlog_is_high_severity():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), q("orders-dlq", messages=400)]
    )
    assert analyze(topo, Thresholds())[0].severity == "high"


def test_max_messages_threshold_suppresses_small_backlogs():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), q("orders-dlq", messages=5)]
    )
    assert analyze(topo, Thresholds(max_messages=10)) == []
    assert analyze(topo, Thresholds(max_messages=4))[0].rule == "dlq_backlog"


def test_old_messages_in_a_dlq_are_a_separate_finding():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            q("orders-dlq", messages=2, oldest_message_age_seconds=40 * 3600),
        ]
    )
    found = analyze(topo, Thresholds(max_messages=10, max_age_hours=24))
    assert rules(found) == [("dlq_stale_messages", "orders-dlq")]
    assert "40.0h" in found[0].detail or "1.7d" in found[0].detail


def test_a_message_near_the_end_of_retention_escalates_to_high():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            q(
                "orders-dlq",
                messages=1,
                retention_seconds=345600,
                oldest_message_age_seconds=330000,
            ),
        ]
    )
    found = analyze(topo, Thresholds(max_messages=10, max_age_hours=24))
    assert found[0].severity == "high"
    assert "about to be dropped" in found[0].detail


def test_age_is_ignored_when_the_dlq_is_empty():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            q("orders-dlq", messages=0, oldest_message_age_seconds=10 ** 6),
        ]
    )
    assert analyze(topo, Thresholds()) == []


def test_source_queue_without_redrive_is_flagged():
    topo = build_topology([q("webhooks", messages=10)])
    found = analyze(topo, Thresholds())
    assert rules(found) == [("source_no_redrive", "webhooks")]
    assert found[0].severity == "medium"


def test_orphan_dlq_is_flagged_and_escalates_when_it_holds_messages():
    empty = build_topology([q("legacy-dlq", messages=0)])
    assert analyze(empty, Thresholds())[0].severity == "low"

    stranded = build_topology([q("legacy-dlq", messages=12)])
    found = analyze(stranded, Thresholds())
    assert found[0].rule == "orphan_dlq"
    assert found[0].severity == "medium"
    assert "stranded" in found[0].detail


def test_dangling_redrive_target_is_high_severity():
    topo = build_topology([q("payments", messages=1, redrive_target_arn="arn:gone")])
    found = analyze(topo, Thresholds())
    assert rules(found) == [("dangling_redrive", "payments")]
    assert found[0].severity == "high"
    assert found[0].data["target_arn"] == "arn:gone"


def test_findings_are_sorted_worst_first():
    topo = build_topology(
        [
            q("webhooks", messages=1),
            q("legacy-dlq", messages=0),
            q("payments", redrive_target_arn="arn:gone"),
        ]
    )
    sev = [f.severity for f in analyze(topo, Thresholds())]
    assert sev == ["high", "medium", "low"]
