#!/usr/bin/env bash
# Local, repeatable forensics for the deferred foreign-key commit rejection.
# Each scenario gets a fresh, throwaway store so runs are independent and
# idempotent. Full line-protocol frames are tee-d into evidence/*.out.
set -euo pipefail

# Resolve repo root from this script's location; never depend on CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$ROOT/target/debug/lockmere"
SCHEMA="$ROOT/config/sample.schema"
CONF="$ROOT/config/default.conf"
SCHED_DIR="$SCRIPT_DIR/schedules"
EVID_DIR="$SCRIPT_DIR/evidence"
RUN_DIR="$SCRIPT_DIR/runs"

mkdir -p "$EVID_DIR" "$RUN_DIR"

declare -a CASES=(
  "A_deferred_self_failure"
  "B_parent_deleted_by_other_tx"
  "C_rollback_and_reopen"
  "D_immediate_control"
)

for name in "${CASES[@]}"; do
  store="$RUN_DIR/$name-store"
  out="$EVID_DIR/$name.out"
  rm -rf "$store"
  echo "== running $name =="
  {
    echo "# store: $store"
    echo "# schedule: $SCHED_DIR/$name.lm"
    "$BIN" init "$store" "$SCHEMA" "$CONF"
    "$BIN" replay "$store" "$SCHED_DIR/$name.lm" "$CONF"
  } | tee "$out"
  echo "== $name OK -> $out =="
  echo
done

echo "All scenarios reproduced. Evidence in: $EVID_DIR"
