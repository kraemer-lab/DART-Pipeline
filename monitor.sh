#!/usr/bin/env bash
# monitor_job.sh — logs CPU/memory of a long-running command + all its
# child processes (so parallel workers, e.g. furrr/future, are included)
# at a fixed interval until it finishes.
#
# Usage:
#   ./monitor_job.sh [-i interval_seconds] [-o outfile.csv] command [args...]
#
# Example:
#   ./monitor_job.sh -i 10 -o run1.csv Rscript pipeline.R
#
# Output CSV columns: timestamp, elapsed_s, cpu_pct, rss_mb, nproc
# (cpu_pct and rss_mb are summed across the whole process tree)

set -u
# (deliberately no -e / pipefail: the job we're wrapping can legitimately
# exit non-zero, and the loop below should tolerate a process disappearing
# mid-check rather than aborting the whole monitoring run)

INTERVAL=5
OUTFILE="usage_$(date +%Y%m%d_%H%M%S).csv"

while getopts "i:o:h" opt; do
  case $opt in
    i) INTERVAL=$OPTARG ;;
    o) OUTFILE=$OPTARG ;;
    h) echo "Usage: $0 [-i interval_seconds] [-o outfile.csv] command [args...]"; exit 0 ;;
    *) echo "Usage: $0 [-i interval_seconds] [-o outfile.csv] command [args...]"; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 [-i interval_seconds] [-o outfile.csv] command [args...]"
  exit 1
fi

# all descendants of a PID (BFS), including the PID itself
get_descendants() {
  local root=$1
  ps -e -o pid=,ppid= | awk -v root="$root" '
    { ppid[$1]=$2 }
    END {
      queue[1]=root; qn=1
      result[root]=1
      while (qn>0) {
        cur=queue[qn]; qn--
        for (p in ppid) {
          if (ppid[p]==cur && !(p in result)) {
            result[p]=1; qn++; queue[qn]=p
          }
        }
      }
      out=""
      for (p in result) out = out (out=="" ? "" : ",") p
      print out
    }'
}

echo "timestamp,elapsed_s,cpu_pct,rss_mb,nproc" > "$OUTFILE"
START=$(date +%s)

"$@" &
PID=$!

trap 'echo "Interrupted — killing PID $PID"; kill "$PID" 2>/dev/null; exit 130' INT TERM

echo "Monitoring PID $PID -> $OUTFILE (every ${INTERVAL}s)"

PEAK_RSS=0
CPU_SUM=0
SAMPLES=0

while kill -0 "$PID" 2>/dev/null; do
  PIDS=$(get_descendants "$PID")
  read -r CPU RSS NP < <(ps -o %cpu=,rss= -p "$PIDS" 2>/dev/null \
    | awk '{c+=$1; r+=$2; n++} END{print c+0, r+0, n+0}')

  RSS_MB=$(awk -v r="$RSS" 'BEGIN{printf "%.1f", r/1024}')
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))

  echo "$(date '+%Y-%m-%d %H:%M:%S'),$ELAPSED,$CPU,$RSS_MB,$NP" >> "$OUTFILE"

  awk -v rss="$RSS" -v peak="$PEAK_RSS" 'BEGIN{exit !(rss>peak)}' && PEAK_RSS=$RSS
  CPU_SUM=$(awk -v s="$CPU_SUM" -v c="$CPU" 'BEGIN{print s+c}')
  SAMPLES=$((SAMPLES + 1))

  sleep "$INTERVAL"
done

wait "$PID"
EXIT_CODE=$?

TOTAL_ELAPSED=$(( $(date +%s) - START ))
PEAK_RSS_MB=$(awk -v r="$PEAK_RSS" 'BEGIN{printf "%.1f", r/1024}')
AVG_CPU=$(awk -v s="$CPU_SUM" -v n="$SAMPLES" 'BEGIN{if(n>0) printf "%.1f", s/n; else print 0}')

echo ""
echo "Done. Exit code: $EXIT_CODE"
echo "Wall time: ${TOTAL_ELAPSED}s | Peak RSS: ${PEAK_RSS_MB} MB | Avg CPU: ${AVG_CPU}%"
echo "Full log: $OUTFILE"

exit "$EXIT_CODE"
