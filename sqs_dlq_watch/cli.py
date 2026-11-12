"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Sequence

from .analyze import Thresholds, analyze
from .collect import AGE_SOURCES, AwsError, collect, fill_ages, make_clients
from .model import build_topology
from .redrive import (
    MAX_MESSAGES_PER_SECOND,
    list_tasks,
    plan_redrive,
    render_plan_text,
    render_tasks_json,
    render_tasks_text,
    resolve_queue,
    start_redrive,
)
from .report import render_json, render_text

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aws-sqs-dlq-watch",
        description=(
            "Audit SQS dead-letter queues: backlogged DLQs, messages about to expire, "
            "orphan DLQs and source queues with no redrive policy."
        ),
        epilog=(
            "To move messages out of a DLQ, see: aws-sqs-dlq-watch redrive --help"
        ),
    )
    p.add_argument("--region", help="AWS region (defaults to the usual boto3 resolution)")
    p.add_argument("--profile", help="named AWS profile")
    p.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="messages allowed in a DLQ before it is a finding (default: 0)",
    )
    p.add_argument(
        "--max-age-hours",
        type=float,
        default=24.0,
        help="age of the oldest DLQ message before it is a finding (default: 24)",
    )
    p.add_argument(
        "--age-source",
        choices=AGE_SOURCES,
        default="cloudwatch",
        help=(
            "where the oldest-message age comes from: the CloudWatch metric, a "
            "zero-visibility peek at the queue, or skip it (default: cloudwatch)"
        ),
    )
    p.add_argument(
        "--min-receive-count",
        type=int,
        default=3,
        metavar="N",
        help="lowest maxReceiveCount a redrive policy may use before it is a finding (default: 3)",
    )
    p.add_argument("--prefix", help="only queues whose name starts with this")
    p.add_argument(
        "--ignore",
        action="append",
        default=[],
        metavar="GLOB",
        help="skip queues matching this glob (repeatable)",
    )
    p.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    p.add_argument(
        "--exit-zero",
        action="store_true",
        help="always exit 0, even with findings (for reporting runs)",
    )
    return p


def run(args: argparse.Namespace, sqs=None, cw=None) -> int:
    """The whole flow, with injectable clients so the tests never touch AWS."""
    try:
        if sqs is None:
            sqs, cw = make_clients(args.region, args.profile)
        queues = collect(sqs, cw, ignore=args.ignore, prefix=args.prefix)
    except AwsError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:  # missing boto3, bad profile, no credentials
        print("error: %s" % exc, file=sys.stderr)
        return EXIT_ERROR

    topo = build_topology(queues)
    # The age lookup costs a request per queue, so ask only about the queues where
    # an age means something: real DLQs and orphan DLQs holding stranded messages.
    aged = set(topo.dlq_sources)
    aged.update(q.arn for q in queues if topo.role(q) == "orphan-dlq")
    fill_ages(queues, sqs, cw, args.age_source, only_arns=aged)

    thresholds = Thresholds(
        max_messages=args.max_messages,
        max_age_hours=args.max_age_hours,
        min_receive_count=args.min_receive_count,
    )
    findings = analyze(topo, thresholds)

    if args.json:
        print(render_json(topo, findings, args.region))
    else:
        print(render_text(topo, findings, args.region))

    if findings and not args.exit_zero:
        return EXIT_FINDINGS
    return EXIT_OK


def _rate(value: str) -> int:
    rate = int(value)
    if not 1 <= rate <= MAX_MESSAGES_PER_SECOND:
        raise argparse.ArgumentTypeError(
            "must be between 1 and %d (the StartMessageMoveTask limit)" % MAX_MESSAGES_PER_SECOND
        )
    return rate


def build_redrive_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aws-sqs-dlq-watch redrive",
        description=(
            "Move messages out of a dead-letter queue with StartMessageMoveTask, or list "
            "the recent move tasks with --status. Starting a task needs --yes."
        ),
    )
    p.add_argument("--region", help="AWS region (defaults to the usual boto3 resolution)")
    p.add_argument("--profile", help="named AWS profile")
    p.add_argument(
        "--queue",
        metavar="NAME|ARN",
        help="the dead-letter queue to redrive (with --status: only this queue)",
    )
    p.add_argument(
        "--to",
        metavar="NAME|ARN",
        help="destination queue (default: each message goes back to its original source)",
    )
    p.add_argument(
        "--max-per-second",
        type=_rate,
        metavar="N",
        help="fixed move rate, 1-%d messages per second (default: chosen by SQS)"
        % MAX_MESSAGES_PER_SECOND,
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true", help="check and print the plan, start nothing"
    )
    mode.add_argument("--yes", action="store_true", help="actually start the move task")
    mode.add_argument(
        "--status",
        action="store_true",
        help="list recent move tasks (of --queue, or of every DLQ in the region)",
    )
    p.add_argument("--json", action="store_true", help="emit JSON instead of text")
    return p


def run_redrive(args: argparse.Namespace, sqs=None) -> int:
    """Plan, refuse or start a redrive; or list move tasks with --status."""
    try:
        if sqs is None:
            sqs, _ = make_clients(args.region, args.profile)
        if args.status:
            return _redrive_status(args, sqs)
        plan = plan_redrive(sqs, args.queue, to_ref=args.to, max_per_second=args.max_per_second)
    except Exception as exc:  # AwsError, unknown queue, missing boto3, no credentials
        print("error: %s" % exc, file=sys.stderr)
        return EXIT_ERROR

    payload = plan.to_dict()
    if not args.json:
        print(render_plan_text(plan, args.region), flush=True)

    if plan.problems:
        if args.json:
            print(json.dumps(dict(payload, started=False), indent=2))
        print("error: redrive refused, nothing was started", file=sys.stderr)
        return EXIT_ERROR

    if not args.yes:
        if args.json:
            print(json.dumps(dict(payload, started=False), indent=2))
        if args.dry_run:
            if not args.json:
                print("\nDry run: nothing was started.")
            return EXIT_OK
        print("error: refusing to start a redrive without --yes", file=sys.stderr)
        return EXIT_ERROR

    try:
        handle = start_redrive(sqs, plan)
    except Exception as exc:
        print("error: %s" % exc, file=sys.stderr)
        return EXIT_ERROR
    if args.json:
        print(json.dumps(dict(payload, started=True, task_handle=handle), indent=2))
    else:
        print("\nStarted move task %s" % handle)
        print("Follow it with: aws-sqs-dlq-watch redrive --status --queue %s" % plan.dlq.name)
    return EXIT_OK


def _redrive_status(args: argparse.Namespace, sqs) -> int:
    if args.queue:
        dlqs = [resolve_queue(sqs, args.queue)]
    else:
        # Move tasks only exist on real DLQs, so ask about those and nothing else.
        queues = collect(sqs)
        topo = build_topology(queues)
        dlqs = [q for q in sorted(queues, key=lambda q: q.name) if topo.is_dlq(q)]
    rows = [(queue, task) for queue in dlqs for task in list_tasks(sqs, queue)]
    if args.json:
        print(render_tasks_json(rows, args.region))
    else:
        print(render_tasks_text(rows, args.region))
    return EXIT_OK


def main(argv: Optional[Sequence[str]] = None, sqs=None, cw=None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    if argv and argv[0] == "redrive":
        p = build_redrive_parser()
        args = p.parse_args(argv[1:])
        if args.status and (args.to or args.max_per_second is not None):
            p.error("--status does not take --to or --max-per-second")
        if not args.status and not args.queue:
            p.error("--queue is required unless --status is given")
        return run_redrive(args, sqs=sqs)
    args = build_parser().parse_args(argv)
    return run(args, sqs=sqs, cw=cw)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__: List[str] = [
    "main",
    "run",
    "run_redrive",
    "build_parser",
    "build_redrive_parser",
    "EXIT_OK",
    "EXIT_FINDINGS",
    "EXIT_ERROR",
]
