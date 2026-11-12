#!/usr/bin/env python3
"""Run the full audit against examples/fixtures/queues.json — no AWS account needed.

    python examples/demo.py          # table, exits 1 because the fixture has findings
    python examples/demo.py --json   # same run as JSON
    python examples/demo.py redrive --status
    python examples/demo.py redrive --queue orders-dlq --max-per-second 50 --dry-run
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqs_dlq_watch.cli import main as cli_main  # noqa: E402
from sqs_dlq_watch.testing import clients_from_spec, load_spec  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "queues.json")


def main() -> int:
    spec = load_spec(FIXTURE)
    sqs, cw = clients_from_spec(spec)
    argv = sys.argv[1:] + ["--region", spec["region"]]
    return cli_main(argv, sqs=sqs, cw=cw)


if __name__ == "__main__":
    raise SystemExit(main())
