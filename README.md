# aws-sqs-dlq-watch

[![ci](https://github.com/moveeeax/aws-sqs-dlq-watch/actions/workflows/ci.yml/badge.svg)](https://github.com/moveeeax/aws-sqs-dlq-watch/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Audit the dead-letter queues in an AWS account: which ones are filling up, which hold
messages that are about to fall off the retention window, which are orphans nothing
redrives into, which source queues have no redrive policy at all, and which redrive
policies are set up to lose messages. Then move the messages back with a guarded
`redrive` command instead of the console.

A DLQ only helps if somebody looks at it. This is the thing that looks.

## What it checks

| rule | meaning |
| --- | --- |
| `dlq_backlog` | a dead-letter queue holds more than `--max-messages` messages |
| `dlq_stale_messages` | its oldest message is older than `--max-age-hours`; escalates to `high` once the message passes 80% of the retention window and is about to be deleted by SQS itself |
| `source_no_redrive` | a queue with no `RedrivePolicy` — a poison message loops there until retention kills it |
| `orphan_dlq` | a queue named like a DLQ that no source queue points at anymore |
| `dangling_redrive` | a `RedrivePolicy` whose `deadLetterTargetArn` does not exist in this region |
| `redrive_low_max_receive` | a source queue's `maxReceiveCount` is below `--min-receive-count` (default 3), so one or two transient failures dead-letter a message |
| `dlq_retention_shorter` | a standard DLQ whose `MessageRetentionPeriod` is not longer than one of its sources: SQS keeps the original enqueue timestamp, so time spent in the source is taken out of the DLQ's retention. FIFO pairs are skipped, FIFO resets the timestamp on the move |
| `dlq_type_mismatch` | a FIFO queue dead-letters into a standard queue or the other way round |
| `redrive_allow_blocks` | the DLQ's `RedriveAllowPolicy` (`denyAll`, or `byQueue` without this source's ARN) does not allow a source queue that points at it |

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

SEV     RULE                     QUEUE              DETAIL
------  -----------------------  -----------------  --------------------------------------------------------------------------------------------------------------------------
HIGH    dangling_redrive         payments           redrive policy points at arn:aws:sqs:…:payments-dlq-old, which does not exist in this region
HIGH    dlq_backlog              orders-dlq         418 message(s) in the dead-letter queue (threshold 0)
HIGH    dlq_stale_messages       orders-dlq         oldest message is 3.6d old, 89% of the 4.0d retention window — it is about to be dropped
HIGH    redrive_allow_blocks     shared-dlq         RedriveAllowPolicy (byQueue) does not allow source(s) receipts that point at this queue
MEDIUM  dlq_backlog              shared-dlq         2 message(s) in the dead-letter queue (threshold 0)
MEDIUM  dlq_retention_shorter    orders-dlq         retention 4.0d is not longer than source(s) orders (4.0d); messages keep their original enqueue time and expire here early
MEDIUM  orphan_dlq               legacy-import-dlq  named like a dead-letter queue but no source queue redrives into it; 37 message(s) are stranded there
MEDIUM  redrive_low_max_receive  receipts           maxReceiveCount is 1 (minimum 3): 1 failed receive(s) move a message to shared-dlq, so transient errors land in the DLQ
MEDIUM  source_no_redrive        webhooks           no redrive policy: a poison message loops until retention deletes it

9 finding(s): 4 high, 5 medium, 0 low
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
--min-receive-count N    lowest maxReceiveCount a redrive policy may use (default: 3)
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

## Redrive

Once the report shows `orders-dlq` with 418 messages, move them back with SQS's own
redrive (`StartMessageMoveTask`):

```bash
aws-sqs-dlq-watch redrive --queue orders-dlq --dry-run          # print the plan, start nothing
aws-sqs-dlq-watch redrive --queue orders-dlq --max-per-second 50 --yes
aws-sqs-dlq-watch redrive --status                               # recent tasks of every DLQ
aws-sqs-dlq-watch redrive --status --queue orders-dlq --json
```

```
SQS redrive plan — eu-central-1

  from:  orders-dlq (418 message(s))
  to:    original source queue(s): orders
  rate:  50 message(s)/s

Dry run: nothing was started.
```

```
--queue NAME|ARN       the dead-letter queue to move messages out of
--to NAME|ARN          destination queue (default: every message goes back to its original source)
--max-per-second N     fixed rate, 1-500 messages per second (default: SQS picks the rate)
--dry-run              check and print the plan, start nothing
--yes                  start the move task; without it the command refuses
--status               list the last 10 move tasks of --queue, or of every DLQ in the region
--json                 emit JSON instead of text
--region, --profile    as for the audit
```

The plan is checked before anything is started, and the command refuses (exit 2) when:

- `--yes` is missing (and `--dry-run` is not given);
- no `--to` is given and `ListDeadLetterSourceQueues` returns no source for the queue, so
  there is no original source to send messages back to;
- `--to` is the DLQ itself, or a queue of the other type (FIFO vs standard);
- the DLQ has no visible messages;
- a move task is already running or cancelling on the DLQ (SQS allows one at a time).

Redriven messages are new messages in the destination: new message ID, new enqueue time.
A task runs asynchronously for up to 36 hours; follow it with `redrive --status`. These
limits are from the
[SQS redrive guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-dead-letter-queue-redrive.html).

Exit codes for `redrive`: 0 when the task was started, the dry run passed or the status
was listed; 2 for a refusal, an unknown queue or an AWS error.

The offline fixture works here too:

```bash
python examples/demo.py redrive --status
python examples/demo.py redrive --queue orders-dlq --max-per-second 50 --dry-run
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

`redrive` needs a separate grant, so the audit role can stay read-only. Scope the
resources down to your DLQs and destinations:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DeadLetterQueues",
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes",
        "sqs:ListDeadLetterSourceQueues",
        "sqs:ListMessageMoveTasks",
        "sqs:StartMessageMoveTask",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage"
      ],
      "Resource": "arn:aws:sqs:eu-central-1:123456789012:*-dlq"
    },
    {
      "Sid": "Destinations",
      "Effect": "Allow",
      "Action": ["sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SendMessage"],
      "Resource": "arn:aws:sqs:eu-central-1:123456789012:*"
    }
  ]
}
```

`redrive --status` without `--queue` also needs `sqs:ListQueues` to find the DLQs. For
queues encrypted with a customer managed KMS key, the redrive also needs `kms:Decrypt` on
the key of the DLQ messages and `kms:GenerateDataKey` plus `kms:Decrypt` on the
destination's key.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check .
```

The test suite runs entirely offline against the stub clients in `sqs_dlq_watch/testing.py`
and the fixture in `examples/fixtures/queues.json` — the same fixture the example script
uses, so the README output above is generated by real code.

## License

MIT
