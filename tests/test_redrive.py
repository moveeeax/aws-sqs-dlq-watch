from __future__ import annotations

import json

import pytest

from sqs_dlq_watch.cli import EXIT_ERROR, EXIT_OK, main
from sqs_dlq_watch.collect import AwsError
from sqs_dlq_watch.redrive import dead_letter_sources, list_tasks, plan_redrive, resolve_queue
from sqs_dlq_watch.testing import FakeSQS


def redrive(sqs, *argv):
    return main(["redrive", "--region", "eu-central-1", *argv], sqs=sqs)


def test_resolve_queue_accepts_a_name_or_an_arn(clients):
    sqs, _ = clients
    by_name = resolve_queue(sqs, "orders-dlq")
    by_arn = resolve_queue(sqs, sqs.arn("orders-dlq"))
    assert by_name.arn == by_arn.arn == sqs.arn("orders-dlq")
    assert by_name.messages == 418


def test_resolve_queue_rejects_an_unknown_queue_and_a_bad_arn(clients):
    sqs, _ = clients
    with pytest.raises(AwsError):
        resolve_queue(sqs, "nope")
    with pytest.raises(ValueError):
        resolve_queue(sqs, "arn:aws:sqs:eu-central-1")


def test_dead_letter_sources_follows_pagination(spec):
    sqs = FakeSQS(spec, page_size=1)
    assert dead_letter_sources(sqs, resolve_queue(sqs, "shared-dlq")) == ["invoices", "receipts"]
    assert sqs.calls.count("ListDeadLetterSourceQueues") == 2


def test_list_tasks_asks_for_more_than_the_default_single_task(clients):
    sqs, _ = clients
    tasks = list_tasks(sqs, resolve_queue(sqs, "orders-dlq"))
    assert [t["Status"] for t in tasks] == ["FAILED", "COMPLETED"]


def test_plan_back_to_the_source_queues(clients):
    sqs, _ = clients
    plan = plan_redrive(sqs, "orders-dlq", max_per_second=50)
    assert plan.problems == []
    assert plan.sources == ["orders"]
    assert plan.request() == {
        "SourceArn": sqs.arn("orders-dlq"),
        "MaxNumberOfMessagesPerSecond": 50,
    }


def test_plan_refuses_an_orphan_dlq_without_a_destination(clients):
    sqs, _ = clients
    plan = plan_redrive(sqs, "legacy-import-dlq")
    assert any("pass --to" in p for p in plan.problems)
    # with an explicit destination the same queue is fine
    assert plan_redrive(sqs, "legacy-import-dlq", to_ref="webhooks").problems == []


def test_plan_refuses_a_destination_of_the_other_queue_type():
    spec = {
        "queues": [
            {"name": "orders.fifo", "fifo": True, "redrive": {"target": "orders-dlq.fifo"}},
            {"name": "orders-dlq.fifo", "fifo": True, "messages": 3},
            {"name": "plain", "messages": 0},
        ]
    }
    sqs = FakeSQS(spec)
    plan = plan_redrive(sqs, "orders-dlq.fifo", to_ref="plain")
    assert any("same type" in p for p in plan.problems)
    assert plan_redrive(sqs, "orders-dlq.fifo", to_ref="orders.fifo").problems == []


def test_plan_refuses_the_dlq_as_its_own_destination_and_an_empty_dlq(clients):
    sqs, _ = clients
    assert any("itself" in p for p in plan_redrive(sqs, "orders-dlq", to_ref="orders-dlq").problems)
    spec = {"queues": [{"name": "a", "redrive": {"target": "a-dlq"}}, {"name": "a-dlq"}]}
    assert any("no visible messages" in p for p in plan_redrive(FakeSQS(spec), "a-dlq").problems)


def test_plan_refuses_while_a_task_is_running(spec):
    spec["queues"][1]["move_tasks"] = [{"Status": "RUNNING", "TaskHandle": "h1"}]
    sqs = FakeSQS(spec)
    plan = plan_redrive(sqs, "orders-dlq")
    assert plan.running_task["TaskHandle"] == "h1"
    assert any("already running" in p for p in plan.problems)


def test_without_yes_nothing_is_started(clients, capsys):
    sqs, _ = clients
    code = redrive(sqs, "--queue", "orders-dlq")
    captured = capsys.readouterr()
    assert code == EXIT_ERROR
    assert "without --yes" in captured.err
    assert "orders-dlq (418 message(s))" in captured.out
    assert sqs.started == []
    assert "StartMessageMoveTask" not in sqs.calls


def test_dry_run_prints_the_plan_and_exits_zero(clients, capsys):
    sqs, _ = clients
    code = redrive(sqs, "--queue", "orders-dlq", "--max-per-second", "50", "--dry-run")
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "original source queue(s): orders" in out
    assert "50 message(s)/s" in out
    assert "Dry run" in out
    assert sqs.started == []


def test_yes_starts_the_move_task(clients, capsys):
    sqs, _ = clients
    code = redrive(sqs, "--queue", "orders-dlq", "--to", "orders", "--max-per-second", "25", "--yes")
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert sqs.started == [
        {
            "SourceArn": sqs.arn("orders-dlq"),
            "DestinationArn": sqs.arn("orders"),
            "MaxNumberOfMessagesPerSecond": 25,
        }
    ]
    assert "Started move task task-orders-dlq-1" in out


def test_a_refused_plan_is_never_started_even_with_yes(clients, capsys):
    sqs, _ = clients
    code = redrive(sqs, "--queue", "legacy-import-dlq", "--yes", "--json")
    captured = capsys.readouterr()
    assert code == EXIT_ERROR
    assert json.loads(captured.out)["started"] is False
    assert "refused" in captured.err
    assert sqs.started == []


def test_rate_outside_the_api_limit_is_a_usage_error(clients):
    sqs, _ = clients
    for bad in ("0", "501"):
        with pytest.raises(SystemExit) as exc:
            redrive(sqs, "--queue", "orders-dlq", "--max-per-second", bad, "--yes")
        assert exc.value.code == 2
    assert sqs.started == []


def test_queue_is_required_unless_status(clients):
    sqs, _ = clients
    with pytest.raises(SystemExit):
        redrive(sqs, "--yes")
    with pytest.raises(SystemExit):
        redrive(sqs, "--status", "--to", "orders")


def test_status_lists_tasks_of_every_dlq(clients, capsys):
    sqs, _ = clients
    code = redrive(sqs, "--status")
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "120/538" in out and "50/s" in out
    assert "kms:Decrypt" in out
    assert "2026-10-03 11:00 UTC" in out
    # shared-dlq has no tasks; the orphan is not a DLQ and is never asked
    assert sqs.calls.count("ListMessageMoveTasks") == 2


def test_status_json_for_one_queue(clients, capsys):
    sqs, _ = clients
    redrive(sqs, "--queue", "orders-dlq", "--yes")
    capsys.readouterr()
    redrive(sqs, "--status", "--queue", "orders-dlq", "--json")
    payload = json.loads(capsys.readouterr().out)
    assert [t["Status"] for t in payload["tasks"]] == ["RUNNING", "FAILED", "COMPLETED"]
    assert {t["queue"] for t in payload["tasks"]} == {"orders-dlq"}


def test_status_with_no_tasks_says_so(capsys):
    sqs = FakeSQS({"queues": [{"name": "a", "redrive": {"target": "a-dlq"}}, {"name": "a-dlq"}]})
    assert redrive(sqs, "--status") == EXIT_OK
    assert "No redrive tasks." in capsys.readouterr().out


def test_an_aws_failure_during_start_exits_two(spec, capsys):
    sqs = FakeSQS(spec, fail_on="StartMessageMoveTask")
    assert redrive(sqs, "--queue", "orders-dlq", "--yes") == EXIT_ERROR
    assert "AccessDenied" in capsys.readouterr().err


def test_the_audit_still_runs_without_a_subcommand(clients, capsys):
    sqs, cw = clients
    assert main(["--region", "eu-central-1", "--exit-zero"], sqs=sqs, cw=cw) == EXIT_OK
    assert "SQS dead-letter report" in capsys.readouterr().out
