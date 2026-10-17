#!/usr/bin/env bash
#
# Runs the full concurrency sweep against the inference server and saves
# each run's output to results/<scenario>_<users>u.log so you have a clean
# record for every data point instead of scrolling back through terminal
# history.
#
# Requires: your full stack (Redis + API + scheduler + worker) already
# running in other terminals before you start this script. This script
# only drives Locust against it - it doesn't start/stop your services.
#
# Usage:
#   chmod +x run_sweep.sh
#   ./run_sweep.sh
#   ./run_sweep.sh --host http://localhost:8000 --duration 60s --cooldown 20
#
# On Windows: run this from Git Bash or WSL, not plain PowerShell.

set -euo pipefail

HOST="http://localhost:8000"
DURATION="60s"
COOLDOWN=20          # seconds to wait between runs, letting the queue drain
USER_LEVELS=(5 10 15 20 30 50)
LOCUSTFILE="locustfile.py"
RESULTS_DIR="results"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --cooldown) COOLDOWN="$2"; shift 2 ;;
    --locustfile) LOCUSTFILE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -f "$LOCUSTFILE" ]]; then
  echo "Can't find $LOCUSTFILE in the current directory. Run this script from the same folder as locustfile.py, or pass --locustfile <path>." >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR"
SUMMARY="$RESULTS_DIR/summary.tsv"
echo -e "scenario\tusers\trequests\tfailures\tfail_pct\tp50_ms\tp95_ms\tp99_ms\tavg_ms" > "$SUMMARY"

# spawn rate: ramp gently for small counts, faster for big ones, so the
# ramp-up itself doesn't distort the steady-state numbers you actually want
spawn_rate_for() {
  local users="$1"
  if (( users <= 10 )); then echo 1; else echo 5; fi
}

# Runs one scenario at one concurrency level, saves raw output, and appends
# a parsed line to the summary TSV.
run_one() {
  local scenario_label="$1"   # used in filenames/summary
  local locust_class="$2"     # "" for mixed (no class arg), or a class name
  local users="$3"
  local rate
  rate=$(spawn_rate_for "$users")

  local outfile="$RESULTS_DIR/${scenario_label}_${users}u.log"
  echo "=== $scenario_label @ ${users} users (rate=${rate}) -> $outfile ==="

  if [[ -n "$locust_class" ]]; then
    locust -f "$LOCUSTFILE" --headless -u "$users" -r "$rate" -t "$DURATION" \
      --host "$HOST" "$locust_class" > "$outfile" 2>&1 || true
  else
    locust -f "$LOCUSTFILE" --headless -u "$users" -r "$rate" -t "$DURATION" \
      --host "$HOST" > "$outfile" 2>&1 || true
  fi

  # Pull the aggregated summary line(s) out of the log and append to the TSV.
  # Locust's "=== Run summary ===" block has one line per endpoint name;
  # we tag each with the scenario/users for later comparison.
  awk -v scenario="$scenario_label" -v users="$users" '
    /Run summary/ { in_summary=1; next }
    in_summary && /^\(.*POST.*\):/ {
      # example line:
      # ('"'"'/infer [unique]'"'"', '"'"'POST'"'"'): requests=63 fails=0 p50=1500ms p95=2000ms p99=3100ms avg=1563ms
      match($0, /requests=([0-9]+)/, r)
      match($0, /fails=([0-9]+)/, f)
      match($0, /p50=([0-9]+)ms/, p50)
      match($0, /p95=([0-9]+)ms/, p95)
      match($0, /p99=([0-9]+)ms/, p99)
      match($0, /avg=([0-9]+)ms/, avg)
      req = r[1] + 0
      fail = f[1] + 0
      pct = (req > 0) ? (fail * 100.0 / req) : 0
      printf "%s\t%s\t%d\t%d\t%.1f\t%s\t%s\t%s\t%s\n", scenario, users, req, fail, pct, p50[1], p95[1], p99[1], avg[1] >> "'"$SUMMARY"'"
    }
    /^$/ { in_summary=0 }
  ' "$outfile" || echo "  (warning: couldn't parse summary line from $outfile - check it manually)"

  echo "  cooling down ${COOLDOWN}s to let the queue drain before the next run..."
  sleep "$COOLDOWN"
}

echo "Sweep: users=${USER_LEVELS[*]} duration=$DURATION host=$HOST"
echo "Results -> $RESULTS_DIR/  (raw logs) and $SUMMARY (parsed table)"
echo

for u in "${USER_LEVELS[@]}"; do
  run_one "unique"  "UniqueUser" "$u"
  run_one "repeat"  "RepeatUser" "$u"
  run_one "mixed"   ""           "$u"
done

echo
echo "Done. Summary table:"
column -t -s $'\t' "$SUMMARY"
