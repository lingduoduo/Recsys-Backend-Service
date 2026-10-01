#!/usr/bin/env bash
# Fails unless each named test class has a Surefire report showing it ran with nothing skipped.
#
# Usage: check-surefire-ran.sh <reports-dir> <fully.qualified.TestClass>...
#
# Docker-dependent tests skip themselves quietly when Docker is absent (assumeTrue,
# @Testcontainers(disabledWithoutDocker = true)), and Maven reports a skip as success, so a
# green build cannot tell "verified" from "never ran". Failures and errors are Maven's job;
# this only answers whether the tests that matter actually executed.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <reports-dir> <fully.qualified.TestClass>..." >&2
  exit 2
fi

dir="$1"
shift
status=0
for class in "$@"; do
  report="$dir/TEST-$class.xml"
  if [ ! -f "$report" ]; then
    echo "MISSING  $class: no report at $report (the class did not run)"
    status=1
    continue
  fi
  # The first <testsuite ...> tag carries the totals.
  suite="$(grep -m1 -o '<testsuite [^>]*>' "$report")"
  tests="$(sed -n 's/.* tests="\([0-9]*\)".*/\1/p' <<<"$suite")"
  skipped="$(sed -n 's/.* skipped="\([0-9]*\)".*/\1/p' <<<"$suite")"
  if [ "${tests:-0}" -eq 0 ]; then
    echo "EMPTY    $class: report shows no tests"
    status=1
  elif [ "${skipped:-0}" -ne 0 ]; then
    echo "SKIPPED  $class: $skipped of $tests tests skipped (is Docker available?)"
    status=1
  else
    echo "RAN      $class: $tests tests, 0 skipped"
  fi
done
exit "$status"
