# aws-sqs-dlq-watch

[![ci](https://github.com/moveeeax/aws-sqs-dlq-watch/actions/workflows/ci.yml/badge.svg)](https://github.com/moveeeax/aws-sqs-dlq-watch/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Audit the dead-letter queues in an AWS account: which ones are filling up, which hold
messages that are about to fall off the retention window, which are orphans nothing
redrives into, and which source queues have no redrive policy at all.

A DLQ only helps if somebody looks at it. This is the thing that looks.

## What it checks

| rule | meaning |
| --- | --- |
| `dlq_backlog` | a dead-letter queue holds more than `--max-messages` messages |
| `dlq_stale_messages` | its oldest message is older than `--max-age-hours`; escalates to `high` once the message passes 80% of the retention window and is about to be deleted by SQS itself |
| `source_no_redrive` | a queue with no `RedrivePolicy` — a poison message loops there until retention kills it |
| `orphan_dlq` | a queue named like a DLQ that no source queue points at anymore |
| `dangling_redrive` | a `RedrivePolicy` whose `deadLetterTargetArn` does not exist in this region |

Roles are resolved from the data, not from naming conventions: a queue is a DLQ because
some other queue's `RedrivePolicy` names its ARN. The name heuristic is only used to spot
orphans, where there is no relationship left to read.

## Install

```bash
pip install git+https://github.com/moveeeax/aws-sqs-dlq-watch
```

## Usage

```bash
aws-sqs-dlq-watch --region eu-central-1 --max-messages 0 --max-age-hours 24
```

```
SQS dead-letter report — eu-central-1 — 8 queue(s)

QUEUE              ROLE        MSGS  FLIGHT  OLDEST  FED BY
-----------------  ----------  ----  ------  ------  ------------------
invoices           source      3     0       -       -
legacy-import-dlq  orphan DLQ  37    0       10.4d   -
orders             source      14    2       -       -
orders-dlq         DLQ         418   0       3.6d    orders
payments           source      5     0       -       -
receipts           source      0     0       -       -
shared-dlq         DLQ         2     0       1.5h    invoices, receipts
webhooks           no redrive  91    7       -       -

SEV     RULE                QUEUE              DETAIL
------  ------------------  -----------------  ----------------------------------------------------------------------
HIGH    dangling_redrive    payments           redrive policy points at arn:aws:sqs:…:payments-dlq-old, which does not exist in this region
HIGH    dlq_backlog         orders-dlq         418 message(s) in the dead-letter queue (threshold 0)
HIGH    dlq_stale_messages  orders-dlq         oldest message is 3.6d old, 89% of the 4.0d retention window — it is about to be dropped
MEDIUM  dlq_backlog         shared-dlq         2 message(s) in the dead-letter queue (threshold 0)
MEDIUM  orphan_dlq          legacy-import-dlq  named like a dead-letter queue but no source queue redrives into it; 37 message(s) are stranded there
MEDIUM  source_no_redrive   webhooks           no redrive policy: a poison message loops until retention deletes it

6 finding(s): 3 high, 3 medium, 0 low
```

That exact report comes from the offline fixture, so you can reproduce it without an AWS
account:

```bash
python examples/demo.py
python examples/demo.py --json | jq '.findings[] | select(.severity=="high")'
```

JSON for a pipeline:

```bash
aws-sqs-dlq-watch --region eu-central-1 --json | jq '.summary'
```

### Flags

```
--region REGION          AWS region (defaults to the usual boto3 resolution)
--profile PROFILE        named AWS profile
--max-messages N         messages allowed in a DLQ before it is a finding (default: 0)
--max-age-hours H        age of the oldest DLQ message before it is a finding (default: 24)
--age-source SOURCE      cloudwatch | peek | none (default: cloudwatch)
--prefix PREFIX          only queues whose name starts with this
--ignore GLOB            skip queues matching this glob (repeatable)
--json                   emit JSON instead of a table
--exit-zero              always exit 0, even with findings
```

### Exit codes

| code | meaning |
| --- | --- |
| 0 | no findings |
| 1 | findings (use `--exit-zero` for reporting-only runs) |
| 2 | AWS error — no credentials, no permission, bad region |

### In CI

```yaml
- name: Audit dead-letter queues
  run: aws-sqs-dlq-watch --region eu-central-1 --max-messages 0 --max-age-hours 6
```

## Where the oldest-message age comes from

`ApproximateAgeOfOldestMessage` is a CloudWatch metric, not an SQS queue attribute, so
`--age-source cloudwatch` (the default) reads the metric and needs
`cloudwatch:GetMetricStatistics`. If the metric is missing or not permitted the age is
reported as `-` rather than failing the run.

`--age-source peek` instead receives up to 10 messages with `VisibilityTimeout=0` and takes
the oldest `SentTimestamp`. Nothing is consumed and nothing is hidden from other consumers,
but a standard queue gives no ordering guarantee, so the result is a lower bound on the real
oldest age. Use it when CloudWatch is not available; prefer the metric otherwise.

`--age-source none` skips the lookup entirely and makes the run pure SQS.

## IAM

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sqs:ListQueues", "sqs:GetQueueAttributes"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["cloudwatch:GetMetricStatistics"],
      "Resource": "*"
    }
  ]
}
```

`--age-source peek` additionally needs `sqs:ReceiveMessage` on the dead-letter queues.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
```

The test suite runs entirely offline against the stub clients in `sqs_dlq_watch/testing.py`
and the fixture in `examples/fixtures/queues.json` — the same fixture the example script
uses, so the README output above is generated by real code.

## License

MIT
