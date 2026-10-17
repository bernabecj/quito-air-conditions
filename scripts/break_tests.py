"""
Break one contract on purpose, run the test that is supposed to catch it, then
restore. Useful to convince yourself a test is testing something real rather
than re-asserting whatever the code already does.

Each break is a single, surgical, reversible text substitution in one file
under producers/. Applying one again without restoring first is refused, and
`status` always shows what is currently broken, so nothing is left half-done.

    python -m scripts.break_tests list
    python -m scripts.break_tests break http-no-retry
    python -m scripts.break_tests status
    pytest tests/test_http.py -q                 # now fails, see why it should
    python -m scripts.break_tests restore http-no-retry
    python -m scripts.break_tests restore --all   # safety net, restores everything recorded

State lives in .break_tests_state.json (gitignored), so `restore --all` works
even in a fresh shell. It never touches git: this works the same with or
without uncommitted changes elsewhere in the tree.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT / ".break_tests_state.json"


@dataclass(frozen=True)
class Break:
    file: str
    old: str
    new: str
    description: str
    run: str
    why: str


BREAKS: dict[str, Break] = {
    "http-no-retry": Break(
        file="producers/http.py",
        old="RETRYABLE_STATUS = {429, 500, 502, 503, 504}",
        new="RETRYABLE_STATUS = set()  # BROKEN: nothing is retried",
        description="A 503 from the source is no longer retried.",
        run="pytest tests/test_http.py tests/test_remmaq.py -q",
        why=(
            "test_http.py expects a transient 503 to be retried and to succeed on the "
            "second try. With this break request_with_retry raises requests.HTTPError "
            "immediately instead of recovering, and the REMMAQ test that checks a 503 "
            "is retried fails the same way. This is the test that tells you the "
            "producer would go down on the first blip instead of riding it out."
        ),
    ),
    "envelope-allows-naive-datetime": Break(
        file="producers/envelope.py",
        old='    if moment.tzinfo is None:\n        raise ValueError("event and fetch times must be timezone-aware")\n',
        new="    pass  # BROKEN: naive datetimes silently pass through\n",
        description="iso_utc() no longer rejects a naive datetime.",
        run="pytest tests/test_envelope.py -q",
        why=(
            "test_iso_utc_rejects_naive expects a naive datetime to raise. Without the "
            "check, a naive datetime is silently treated as UTC by astimezone(), which "
            "would make a local-time bug in a future history loader invisible instead "
            "of crashing loudly the first time it happens."
        ),
    ),
    "remmaq-key-is-name-not-guid": Break(
        file="producers/remmaq_producer.py",
        old='station_id = site["SourceSiteID"]',
        new='station_id = site["SiteName"]  # BROKEN: not the stable GUID',
        description="Kafka messages are keyed by station name instead of the stable GUID.",
        run="pytest tests/test_producers.py -q",
        why=(
            "test_one_record_per_station_payload_untouched checks every key is the "
            "feed's SourceSiteID and is a member of STATIONS_BY_ID. Keying by name "
            "instead still works today, but the feed already spells one station "
            "'LosChillos' with no space; a future rename would silently split that "
            "station's history across two Kafka keys instead of being one station."
        ),
    ),
    "kafka-sink-swallows-failures": Break(
        file="producers/sinks.py",
        old='        if self.failed:\n            raise SinkError("; ".join(self.failed[:5]))\n',
        new="        pass  # BROKEN: delivery failures are no longer raised\n",
        description="KafkaSink.flush() no longer raises when a delivery failed.",
        run="pytest tests/test_sinks.py -q",
        why=(
            "test_failed_delivery_raises_on_flush and test_records_left_in_queue_raise "
            "expect flush() to raise SinkError, which is what makes the producer's exit "
            "code non-zero and the scheduler's run show red. Without it, a broker "
            "rejecting every record looks like a successful run: the cron job is green "
            "and the topic is empty."
        ),
    ),
    "kafka-acks-weakened": Break(
        file="producers/settings.py",
        old='"acks": "all",',
        new='"acks": "1",  # BROKEN: only the leader needs to have it',
        description="Kafka producer no longer waits for every in-sync replica.",
        run="pytest tests/test_settings.py -q",
        why=(
            "test_kafka_config_is_at_least_once pins acks='all'. With acks='1' a "
            "record can be acknowledged by the leader and then lost if it fails before "
            "replicating, which quietly turns at-least-once into best-effort."
        ),
    ),
    "dotnet-date-off-by-1000": Break(
        file="producers/remmaq.py",
        old="    return datetime.fromtimestamp(int(match.group(1)) / 1000, tz=timezone.utc)",
        new="    return datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)  # BROKEN: treats ms as seconds",
        description="/Date(ms)/ is parsed as if it were seconds, 1000x too far in the future.",
        run="pytest tests/test_remmaq.py -q",
        why=(
            "test_epoch_millis_to_utc pins the real capture's timestamp "
            "'/Date(1792188000000)/' to 2026-10-16T22:00:00Z. Off by a factor of 1000, "
            "the same string decodes to the year 58766, a wrong-unit bug that is easy "
            "to make and, without a fixed-data test, easy never to notice locally."
        ),
    ),
    "runner-hides-source-failure": Break(
        file="producers/runner.py",
        old="            exit_code = 1",
        new="            exit_code = 0  # BROKEN: failures no longer fail the process",
        description="The CLI exits 0 even when the source could not be reached.",
        run="pytest tests/test_runner.py -q",
        why=(
            "test_source_down_exits_one and test_source_changed_shape_exits_one expect "
            "exit code 1. A scheduler (GitHub Actions, Lambda, cron) only notices a "
            "dead producer through its exit code; returning 0 turns every outage into "
            "a silent gap in the data with a green checkmark next to it."
        ),
    ),
}


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    if state:
        STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    elif STATE_PATH.exists():
        STATE_PATH.unlink()


def do_break(name: str) -> None:
    spec = BREAKS[name]
    state = load_state()
    if name in state:
        sys.exit(f"'{name}' is already broken (since {state[name]}). Restore it first.")
    path = ROOT / spec.file
    text = path.read_text(encoding="utf-8")
    count = text.count(spec.old)
    if count != 1:
        sys.exit(
            f"expected exactly one occurrence of the original text in {spec.file}, found {count}. "
            "The source file has likely changed since this script was written; update BREAKS."
        )
    path.write_text(text.replace(spec.old, spec.new), encoding="utf-8")
    state[name] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    save_state(state)
    print(f"Broke '{name}': {spec.description}\n  file: {spec.file}")
    print(f"\nRun to see it fail:\n  {spec.run}\n")
    print(f"Why this test catches it:\n  {spec.why}")
    print(f"\nWhen done:\n  python -m scripts.break_tests restore {name}")


def do_restore(name: str) -> None:
    spec = BREAKS[name]
    state = load_state()
    path = ROOT / spec.file
    text = path.read_text(encoding="utf-8")
    if spec.new not in text:
        if name in state:
            del state[name]
            save_state(state)
        print(f"'{name}' was not broken in {spec.file}, nothing to do.")
        return
    path.write_text(text.replace(spec.new, spec.old), encoding="utf-8")
    state.pop(name, None)
    save_state(state)
    print(f"Restored '{name}' in {spec.file}.")


def do_restore_all() -> None:
    state = load_state()
    names = list(state) or list(BREAKS)  # even if the state file was lost, try everything
    restored = 0
    for name in names:
        spec = BREAKS.get(name)
        if spec is None:
            continue
        path = ROOT / spec.file
        text = path.read_text(encoding="utf-8")
        if spec.new in text:
            path.write_text(text.replace(spec.new, spec.old), encoding="utf-8")
            restored += 1
    save_state({})
    print(f"Restored {restored} break(s). Repository is back to its original state.")


def do_status() -> None:
    state = load_state()
    broken = []
    for name, spec in BREAKS.items():
        path = ROOT / spec.file
        if spec.new in path.read_text(encoding="utf-8"):
            broken.append(name)
    if not broken:
        print("Nothing broken. Repository matches the committed source.")
        return
    print("Currently broken:")
    for name in broken:
        tracked = "tracked" if name in state else "NOT TRACKED (state file out of sync)"
        print(f"  {name}  [{tracked}]  -> {BREAKS[name].file}")


def do_list() -> None:
    for name, spec in BREAKS.items():
        print(f"{name}")
        print(f"    {spec.description}")
        print(f"    run: {spec.run}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show every available break")
    sub.add_parser("status", help="show what is currently broken")

    p_break = sub.add_parser("break", help="apply one break")
    p_break.add_argument("name", choices=sorted(BREAKS))

    p_restore = sub.add_parser("restore", help="undo one break, or --all of them")
    p_restore.add_argument("name", nargs="?", choices=sorted(BREAKS))
    p_restore.add_argument("--all", action="store_true")

    args = parser.parse_args()

    if args.command == "list":
        do_list()
    elif args.command == "status":
        do_status()
    elif args.command == "break":
        do_break(args.name)
    elif args.command == "restore":
        if args.all:
            do_restore_all()
        elif args.name:
            do_restore(args.name)
        else:
            sub.choices["restore"].error("pass a break name or --all")
    return 0


if __name__ == "__main__":
    sys.exit(main())
