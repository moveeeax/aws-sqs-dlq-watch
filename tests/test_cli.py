from __future__ import annotations

import datetime as dt
import json

from sqs_dlq_watch.cli import EXIT_ERROR, EXIT_FINDINGS, EXIT_OK, build_parser, run
from sqs_dlq_watch.testing import FakeCloudWatch, FakeSQS


def invoke(clients, *argv):
    sqs, cw = clients
    args = build_parser().parse_args(["--region", "eu-central-1", *argv])
    return run(args, sqs=sqs, cw=cw)


def test_table_run_reports_every_rule_and_exits_one(clients, capsys):
    code = invoke(clients)
    out = capsys.readouterr().out
    assert code == EXIT_FINDINGS
    for rule in ("dlq_backlog", "dlq_stale_messages", "orphan_dlq", "source_no_redrive", "dangling_redrive"):
        assert rule in out
    assert "orders-dlq" in out and "418" in out
    assert "HIGH" in out


def test_orphan_dlq_age_is_resolved_too(clients, capsys):
    invoke(clients)
    out = capsys.readouterr().out
    # legacy-import-dlq holds stranded messages 900000s old -> 10.4d in the table
    assert "10.4d" in out


def test_json_output_is_valid_and_carries_roles(clients, capsys):
    code = invoke(clients, "--json")
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_FINDINGS
    roles = {q["name"]: q["role"] for q in payload["queues"]}
    assert roles["orders"] == "source"
    assert roles["orders-dlq"] == "dlq"
    assert roles["legacy-import-dlq"] == "orphan-dlq"
    assert roles["webhooks"] == "unprotected"
    assert payload["summary"]["queues"] == 8
    assert payload["summary"]["dead_letter_queues"] == 2
    assert payload["summary"]["findings"] == len(payload["findings"])
    assert payload["queues"][0]["fed_by"] == [] or isinstance(payload["queues"][0]["fed_by"], list)
    fed = {q["name"]: q["fed_by"] for q in payload["queues"]}
    assert fed["shared-dlq"] == ["invoices", "receipts"]


def test_clean_account_exits_zero(capsys):
    clean = {
        "region": "eu-central-1",
        "account": "123456789012",
        "queues": [
            {"name": "orders", "messages": 4, "redrive": {"target": "orders-dlq"}},
            {"name": "orders-dlq", "messages": 0},
        ],
    }
    sqs = FakeSQS(clean)
    code = invoke((sqs, FakeCloudWatch(sqs)))
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "No findings." in out


def test_exit_zero_flag_keeps_the_exit_code_at_zero(clients):
    assert invoke(clients, "--exit-zero") == EXIT_OK


def test_ignore_flag_drops_queues_from_the_report(clients, capsys):
    invoke(clients, "--ignore", "legacy-*", "--ignore", "webhooks")
    out = capsys.readouterr().out
    assert "legacy-import-dlq" not in out
    assert "orphan_dlq" not in out
    assert "source_no_redrive" not in out


def test_thresholds_are_honoured(clients, capsys):
    invoke(clients, "--max-messages", "1000", "--max-age-hours", "2000")
    out = capsys.readouterr().out
    assert "dlq_backlog" not in out
    assert "dlq_stale_messages" not in out


def test_an_aws_failure_exits_two(spec, capsys):
    sqs = FakeSQS(spec, fail_on="ListQueues")
    code = invoke((sqs, FakeCloudWatch(sqs)))
    assert code == EXIT_ERROR
    assert "AccessDenied" in capsys.readouterr().err


def test_peek_age_source_matches_cloudwatch_on_the_fixture(spec, capsys):
    # The peek path ages messages against the wall clock, so the stub has to be
    # anchored to the same clock the CLI will read.
    sqs = FakeSQS(spec, now=dt.datetime.now(dt.timezone.utc))
    invoke((sqs, FakeCloudWatch(sqs)), "--age-source", "peek", "--json")
    payload = json.loads(capsys.readouterr().out)
    ages = {q["name"]: q["oldest_message_age_seconds"] for q in payload["queues"]}
    assert 309000 <= ages["orders-dlq"] <= 309010
