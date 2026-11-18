#!/usr/bin/env python3
"""Summarize results written by scripts/compare.sh.

    scripts/summarize.py <results dir> [--runs]

Prints, for every case, scenario and build, the mean over the repetitions that
finished, and how each build compares with the first build listed for that scenario.
It reads only the stored JSON reports and never runs anything, so it can be fixed and
re-run on the same results. A run that is unfinished or fails to parse is listed at
the end and left out; the others still count. --runs also prints every repetition.
"""
import csv
import json
import os
import statistics
import sys


def load_perf(path):
    """Counter name -> value from a `perf stat -x,` CSV, if the run has one."""
    counters = {}
    if not os.path.exists(path):
        return counters
    with open(path) as f:
        for row in csv.reader(f):
            if len(row) < 3 or not row[0] or row[0].startswith(("#", "<")):
                continue
            try:
                counters[row[2].replace("syscalls:sys_enter_", "")] = float(row[0])
            except ValueError:
                pass
    return counters


def runs_of(root):
    """Yields (case, build, rep, run dir) for every run directory under root."""
    for case in sorted(os.listdir(root)):
        case_dir = os.path.join(root, case)
        if not os.path.isdir(case_dir):
            continue
        for build in sorted(os.listdir(case_dir)):
            for rep in sorted(os.listdir(os.path.join(case_dir, build))):
                yield case, build, rep, os.path.join(case_dir, build, rep)


def parse(run_dir):
    """One row per my-mini-redis scenario in the run's JSON report."""
    with open(os.path.join(run_dir, "bench", "latest.json")) as f:
        report = json.load(f)
    perf = load_perf(os.path.join(run_dir, "perf.csv"))
    rows = []
    for r in report["runs"]:
        if r["server"] != "my-mini-redis":
            continue
        delivery = r.get("delivery_latency_ns") or [None] * 5
        # Syscalls are counted over 5 s; per delivery for pub/sub, else per operation.
        rate = r.get("deliveries_per_sec") or r["ops_per_sec"]
        per_op = lambda name: perf[name] / (rate * 5) if name in perf and rate else None
        rows.append({
            "label": r["label"],
            "ops": r["ops_per_sec"],
            "publishes": r.get("publishes_per_sec"),
            "deliveries": r.get("deliveries_per_sec"),
            "p50": r["latency_ns_p50_p90_p99_p999_max"][0],
            "p99": r["latency_ns_p50_p90_p99_p999_max"][2],
            "delivery_p99": delivery[2],
            "delivery_p999": delivery[3],
            "cores": r.get("server_cores"),
            "cpu_us": r.get("server_cpu_us_per_op"),
            "sendto": per_op("sendto"),
            "futex": per_op("futex"),
        })
    return rows


def mean(values):
    values = [v for v in values if v is not None]
    return statistics.mean(values) if values else None


def duration(ns):
    if ns is None:
        return "-"
    return f"{ns / 1e6:.2f}ms" if ns >= 1e6 else f"{ns / 1e3:.0f}us"


def number(value, digits=0):
    return "-" if value is None else f"{value:,.{digits}f}"


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__.strip())
        sys.exit(0 if len(sys.argv) > 1 else 2)
    root, show_runs = sys.argv[1], "--runs" in sys.argv[2:]

    groups, skipped = {}, []
    for case, build, rep, run_dir in runs_of(root):
        if not os.path.exists(os.path.join(run_dir, "done")):
            skipped.append(f"{case}/{build}/{rep}: unfinished (no done marker)")
            continue
        try:
            for row in parse(run_dir):
                groups.setdefault((case, row["label"]), {}).setdefault(build, []).append((rep, row))
        except Exception as error:  # one broken run must not hide the others
            skipped.append(f"{case}/{build}/{rep}: {type(error).__name__}: {error}")

    has_perf = any(row["sendto"] is not None for builds in groups.values()
                   for runs in builds.values() for _, row in runs)
    for (case, label), builds in sorted(groups.items()):
        print(f"\n{case}: {label}")
        header = (f"  {'build':12} {'n':>2} {'ops/s':>12} {'vs first':>9} {'deliveries/s':>13} "
                  f"{'p50':>8} {'p99':>8} {'delivery p99':>13} {'p99.9':>8} {'cores':>6} {'CPU/op':>9}")
        print(header + (f" {'sendto/op':>9} {'futex/op':>9}" if has_perf else ""))
        first = None
        for build, runs in builds.items():
            m = {key: mean([row[key] for _, row in runs]) for key in runs[0][1] if key != "label"}
            first = first or m["ops"]
            change = f"{m['ops'] / first - 1:+.1%}" if first else "-"
            line = (f"  {build:12} {len(runs):>2} {number(m['ops']):>12} {change:>9} "
                    f"{number(m['deliveries']):>13} {duration(m['p50']):>8} {duration(m['p99']):>8} "
                    f"{duration(m['delivery_p99']):>13} {duration(m['delivery_p999']):>8} "
                    f"{number(m['cores'], 2):>6} {number(m['cpu_us'], 3) + 'us':>9}")
            if has_perf:
                line += f" {number(m['sendto'], 4):>9} {number(m['futex'], 4):>9}"
            print(line)
            if show_runs:
                for rep, row in runs:
                    print(f"      {rep}: ops/s {number(row['ops'])}"
                          + (f", deliveries/s {number(row['deliveries'])}" if row["deliveries"] else ""))
    if skipped:
        print(f"\n{len(skipped)} run(s) left out:")
        for line in skipped:
            print(f"  {line}")


if __name__ == "__main__":
    main()
