#!/usr/bin/env bash
# Tests scripts/check-surefire-ran.sh against synthetic Surefire XML reports.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT/scripts/check-surefire-ran.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FAILURES=0
check() { # check <name> <expected-exit> <actual-exit>
  if [ "$2" = "$3" ]; then
    echo "ok   - $1"
  else
    echo "FAIL - $1: expected exit $2, got $3"
    FAILURES=$((FAILURES + 1))
  fi
}

report() { # report <dir> <class> <tests> <skipped> <failures> <errors>
  mkdir -p "$1"
  printf '<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="%s" time="1.0" tests="%s" errors="%s" skipped="%s" failures="%s">\n</testsuite>\n' \
    "$2" "$3" "$6" "$4" "$5" >"$1/TEST-$2.xml"
}

run() { # run <dir> <class...>; prints the exit status
  set +e
  "$SCRIPT" "$@" >/dev/null 2>&1
  local status=$?
  set -e
  echo "$status"
}

A=com.recsys.online.flink.OnlineFeatureStreamingJobTest
B=com.recsys.online.flink.KafkaFlinkPartitionIntegrationTest

report "$TMP/ran" "$A" 37 0 0 0
report "$TMP/ran" "$B" 2 0 0 0
check "every required class ran with nothing skipped" 0 "$(run "$TMP/ran" "$A" "$B")"

report "$TMP/skipped" "$A" 37 0 0 0
report "$TMP/skipped" "$B" 2 2 0 0
check "a Docker-less skip is a failure, not a pass" 1 "$(run "$TMP/skipped" "$A" "$B")"

report "$TMP/missing" "$A" 37 0 0 0
check "a required class with no report is a failure" 1 "$(run "$TMP/missing" "$A" "$B")"

report "$TMP/empty" "$A" 0 0 0 0
check "a report with zero tests is a failure" 1 "$(run "$TMP/empty" "$A")"

check "no classes named is a usage error" 2 "$(run "$TMP/ran")"

if [ "$FAILURES" -ne 0 ]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "all checks passed"
