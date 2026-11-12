from __future__ import annotations

from sqs_dlq_watch.analyze import Thresholds, analyze
from sqs_dlq_watch.model import Queue, build_topology


def q(name, **kw):
    return Queue(url="https://sqs/1/%s" % name, name=name, arn="arn:%s" % name, **kw)


def dlq(name, **kw):
    # Longer retention than the 4-day default of the sources, so the tests for the
    # other rules do not also trip dlq_retention_shorter.
    kw.setdefault("retention_seconds", 14 * 86400)
    return q(name, **kw)


def rules(findings):
    return sorted((f.rule, f.queue) for f in findings)


def test_clean_account_has_no_findings():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), dlq("orders-dlq", messages=0)]
    )
    assert analyze(topo, Thresholds()) == []


def test_non_empty_dlq_is_a_finding():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), dlq("orders-dlq", messages=3)]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("dlq_backlog", "orders-dlq")]
    assert found[0].severity == "medium"
    assert found[0].data["messages"] == 3


def test_a_big_backlog_is_high_severity():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), dlq("orders-dlq", messages=400)]
    )
    assert analyze(topo, Thresholds())[0].severity == "high"


def test_max_messages_threshold_suppresses_small_backlogs():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq"), dlq("orders-dlq", messages=5)]
    )
    assert analyze(topo, Thresholds(max_messages=10)) == []
    assert analyze(topo, Thresholds(max_messages=4))[0].rule == "dlq_backlog"


def test_old_messages_in_a_dlq_are_a_separate_finding():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            dlq("orders-dlq", messages=2, oldest_message_age_seconds=40 * 3600),
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
            dlq("orders-dlq", messages=0, oldest_message_age_seconds=10 ** 6),
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


def test_low_max_receive_count_is_flagged_on_the_source():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq", max_receive_count=1), dlq("orders-dlq")]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("redrive_low_max_receive", "orders")]
    assert found[0].severity == "medium"
    assert found[0].data == {
        "max_receive_count": 1,
        "min_receive_count": 3,
        "dead_letter_queue": "orders-dlq",
    }


def test_min_receive_count_threshold_is_configurable():
    topo = build_topology(
        [q("orders", redrive_target_arn="arn:orders-dlq", max_receive_count=3), dlq("orders-dlq")]
    )
    assert analyze(topo, Thresholds()) == []
    assert rules(analyze(topo, Thresholds(min_receive_count=5))) == [
        ("redrive_low_max_receive", "orders")
    ]


def test_dlq_retention_not_longer_than_the_source_is_flagged_once_per_dlq():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:shared-dlq", retention_seconds=345600),
            q("refunds", redrive_target_arn="arn:shared-dlq", retention_seconds=86400),
            q("invoices", redrive_target_arn="arn:shared-dlq", retention_seconds=7 * 86400),
            q("shared-dlq", retention_seconds=345600),
        ]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("dlq_retention_shorter", "shared-dlq")]
    assert found[0].severity == "medium"
    # equal retention counts: time already spent in the source is lost either way
    assert found[0].data["sources"] == {"orders": 345600, "invoices": 604800}
    assert "orders (4.0d)" in found[0].detail and "refunds" not in found[0].detail


def test_retention_rule_skips_fifo_pairs_because_fifo_resets_the_enqueue_time():
    topo = build_topology(
        [
            q("orders.fifo", redrive_target_arn="arn:orders-dlq.fifo", is_fifo=True),
            q("orders-dlq.fifo", is_fifo=True),
        ]
    )
    assert analyze(topo, Thresholds()) == []


def test_fifo_source_with_a_standard_dlq_is_a_type_mismatch():
    topo = build_topology(
        [
            q("orders.fifo", redrive_target_arn="arn:orders-dlq", is_fifo=True),
            dlq("orders-dlq"),
        ]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("dlq_type_mismatch", "orders.fifo")]
    assert found[0].severity == "high"
    assert "FIFO queue redrives into standard queue orders-dlq" in found[0].detail


def test_redrive_allow_policy_deny_all_blocks_every_source():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            dlq("orders-dlq", redrive_allow_permission="denyAll"),
        ]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("redrive_allow_blocks", "orders-dlq")]
    assert found[0].severity == "high"
    assert found[0].data["blocked_sources"] == ["orders"]


def test_redrive_allow_policy_by_queue_blocks_only_unlisted_sources():
    topo = build_topology(
        [
            q("invoices", redrive_target_arn="arn:shared-dlq"),
            q("receipts", redrive_target_arn="arn:shared-dlq"),
            dlq(
                "shared-dlq",
                redrive_allow_permission="byQueue",
                redrive_allow_source_arns=["arn:invoices"],
            ),
        ]
    )
    found = analyze(topo, Thresholds())
    assert rules(found) == [("redrive_allow_blocks", "shared-dlq")]
    assert found[0].data["blocked_sources"] == ["receipts"]
    assert "byQueue" in found[0].detail


def test_policy_rules_ignore_dangling_targets():
    topo = build_topology([q("payments", redrive_target_arn="arn:gone", max_receive_count=1)])
    assert rules(analyze(topo, Thresholds())) == [("dangling_redrive", "payments")]
