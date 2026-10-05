from __future__ import annotations

import json

from sqs_dlq_watch.model import Queue, build_topology, matches_any, parse_redrive_policy


def q(name, **kw):
    return Queue(url="https://sqs/1/%s" % name, name=name, arn="arn:%s" % name, **kw)


def test_parse_redrive_policy_reads_target_and_count():
    raw = json.dumps({"deadLetterTargetArn": "arn:orders-dlq", "maxReceiveCount": "5"})
    assert parse_redrive_policy(raw) == ("arn:orders-dlq", 5)


def test_parse_redrive_policy_survives_garbage():
    assert parse_redrive_policy("not json at all") == (None, None)
    assert parse_redrive_policy("[]") == (None, None)
    assert parse_redrive_policy(None) == (None, None)
    assert parse_redrive_policy(json.dumps({"deadLetterTargetArn": "arn:x"})) == ("arn:x", None)


def test_topology_resolves_dlq_relationships():
    topo = build_topology(
        [
            q("orders", redrive_target_arn="arn:orders-dlq"),
            q("refunds", redrive_target_arn="arn:orders-dlq"),
            q("orders-dlq"),
        ]
    )
    assert topo.dlq_sources == {"arn:orders-dlq": ["orders", "refunds"]}
    assert topo.dangling == []
    roles = {x.name: topo.role(x) for x in topo.queues}
    assert roles == {"orders": "source", "refunds": "source", "orders-dlq": "dlq"}


def test_topology_flags_a_redrive_target_that_does_not_exist():
    topo = build_topology([q("payments", redrive_target_arn="arn:gone-dlq")])
    assert topo.dlq_sources == {}
    assert topo.dangling == [("payments", "arn:gone-dlq")]


def test_queue_named_like_a_dlq_with_no_sources_is_an_orphan():
    topo = build_topology([q("legacy-import-dlq", messages=4)])
    assert topo.role(topo.queues[0]) == "orphan-dlq"


def test_plain_queue_without_redrive_is_unprotected():
    topo = build_topology([q("webhooks", messages=1)])
    assert topo.role(topo.queues[0]) == "unprotected"


def test_a_dlq_that_also_has_its_own_redrive_is_still_a_dlq():
    topo = build_topology(
        [q("a", redrive_target_arn="arn:b"), q("b", redrive_target_arn="arn:c"), q("c")]
    )
    by_name = {x.name: topo.role(x) for x in topo.queues}
    assert by_name["b"] == "dlq"
    assert by_name["c"] == "dlq"


def test_matches_any_globs_on_the_queue_name():
    assert matches_any("tf-state-lock", ["tf-*"])
    assert not matches_any("orders", ["tf-*", "*-test"])
