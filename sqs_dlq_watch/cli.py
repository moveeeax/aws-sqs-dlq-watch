"""Command line entry point."""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Sequence

from .analyze import Thresholds, analyze
from .collect import AGE_SOURCES, AwsError, collect, fill_ages, make_clients
from .model import build_topology
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

    findings = analyze(
        topo, Thresholds(max_messages=args.max_messages, max_age_hours=args.max_age_hours)
    )

    if args.json:
        print(render_json(topo, findings, args.region))
    else:
        print(render_text(topo, findings, args.region))

    if findings and not args.exit_zero:
        return EXIT_FINDINGS
    return EXIT_OK


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__: List[str] = ["main", "run", "build_parser", "EXIT_OK", "EXIT_FINDINGS", "EXIT_ERROR"]
