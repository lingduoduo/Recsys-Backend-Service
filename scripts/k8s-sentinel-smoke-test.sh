#!/usr/bin/env bash
# Smoke-tests the Redis Sentinel tier of k8s/base against a real cluster.
#
# Answers three questions the manifests alone cannot:
#   1. Do the Sentinel pods start at all?
#   2. Do the three of them reach quorum?
#   3. Can a client discover the primary through the Sentinel Service and then
#      actually connect to the address it is handed?
#
# Question 1 is not hypothetical. Before 2026-09-19 the template monitored the
# primary by hostname with `resolve-hostnames` left at its default of "no", which
# Redis treats as a fatal config error rather than a lookup, so every Sentinel pod
# CrashLoopBackOff'd. Nothing in CI noticed, because the EKS overlays scale this
# StatefulSet to zero in favour of ElastiCache. Run this after any change to the
# sentinel template.
#
# Usage:
#   scripts/k8s-sentinel-smoke-test.sh [--keep]
#
#   --keep   leave the namespace up afterwards for inspection
#
# Requires: kubectl pointed at a throwaway cluster (minikube, kind, k3d).
# Creates and deletes the `recsys` namespace. Do NOT run against a shared cluster.

set -euo pipefail

NS=recsys
KEEP=${1:-}
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Test-only credentials. This script must never be pointed at a real cluster, so
# these are deliberately fixed rather than generated — a reader should be able to
# tell at a glance that nothing here is a production secret.
REDIS_PW="smoketest-redis-pw"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mok:\033[0m %s\n' "$*"; }

say "Preflight"
kubectl cluster-info >/dev/null 2>&1 || fail "kubectl is not pointed at a reachable cluster"
CTX="$(kubectl config current-context)"
case "$CTX" in
  minikube|kind-*|k3d-*) pass "context '$CTX' looks like a throwaway cluster" ;;
  *) fail "refusing to run against context '$CTX' — this script deletes the '$NS' namespace" ;;
esac

say "Clean slate"
# Not optional. `kubectl apply` of a changed ConfigMap does not restart the pods that
# mounted it, so a stale Sentinel left running from a previous run would answer every
# check below while the manifest under test was never loaded — the script would pass on
# a broken template. Deleting the namespace is what makes the run mean something.
if kubectl get namespace "$NS" >/dev/null 2>&1; then
  echo "deleting existing '$NS' namespace first..."
  kubectl delete namespace "$NS" --timeout=180s >/dev/null \
    || fail "could not delete the existing '$NS' namespace"
fi

say "Namespace and secrets"
kubectl apply -f "$REPO_ROOT/k8s/base/namespace.yaml" >/dev/null

# recsys-secrets is `optional: true` in the manifests and ships in no file, so the
# smoke test renders it the same way an operator would (see docs/runbooks/redis-auth.md).
sed -e "s|__REDIS_PASSWORD__|$REDIS_PW|" \
    -e "s|__CATALOG_PASSWORD__|catalog-pw|" \
    -e "s|__MODEL_PASSWORD__|model-pw|" \
    -e "s|__ONLINE_PASSWORD__|online-pw|" \
    -e "s|__GATEWAY_PASSWORD__|gateway-pw|" \
    -e "s|__RECONCILIATION_PASSWORD__|reconciliation-pw|" \
    "$REPO_ROOT/k8s/base/redis-users.acl.template" > "$WORK/users.acl"
grep -q '__' "$WORK/users.acl" && fail "ACL template still has unrendered placeholders"

kubectl -n "$NS" delete secret recsys-secrets --ignore-not-found >/dev/null
kubectl -n "$NS" create secret generic recsys-secrets \
  --from-literal=redis-password="$REDIS_PW" \
  --from-file=redis-users.acl="$WORK/users.acl" >/dev/null
pass "recsys-secrets created with redis-password and redis-users.acl"

say "Applying the Redis tier"
kubectl apply -f "$REPO_ROOT/k8s/base/redis-cluster.yaml" >/dev/null
kubectl -n "$NS" rollout status statefulset/redis-primary --timeout=300s \
  || fail "redis-primary never became ready"
pass "redis-primary ready"

say "1. Do the Sentinel pods start?"
# rollout status is the assertion. A CrashLoopBackOff never satisfies it, which is
# exactly how the resolve-hostnames defect used to present.
if ! kubectl -n "$NS" rollout status statefulset/redis-sentinel --timeout=180s; then
  echo "--- pod state ---"
  kubectl -n "$NS" get pods -l app=redis-sentinel
  echo "--- logs from redis-sentinel-0 ---"
  kubectl -n "$NS" logs redis-sentinel-0 --tail=30 || true
  fail "Sentinel pods did not become ready. If the log says \"Can't resolve instance hostname\", the template is missing 'sentinel resolve-hostnames yes'."
fi
READY=$(kubectl -n "$NS" get statefulset redis-sentinel -o jsonpath='{.status.readyReplicas}')
[ "$READY" = "3" ] || fail "expected 3 ready Sentinels, got ${READY:-0}"
pass "all 3 Sentinel pods running and ready"

say "2. Do they reach quorum?"
# Sentinel itself sets no requirepass, but its pods carry REDISCLI_AUTH so that probes
# can reach the master. Left set, redis-cli sends AUTH to Sentinel and prints "AUTH
# failed" to stderr while still returning the right answer — a check that prints an
# error and then passes is a check nobody can trust. `env -u` drops it.
sentinel_field() { # $1 = pod ordinal, $2 = field name
  kubectl -n "$NS" exec "redis-sentinel-$1" -c sentinel -- \
    env -u REDISCLI_AUTH redis-cli -p 26379 sentinel master mymaster \
    | paste - - | awk -v f="$2" '$1==f{print $2}' | tr -d '\r'
}

# `num-other-sentinels` counts peers Sentinel has ever *known*, not peers it can reach:
# measured, with 2 of 3 pods deleted it still reported 2 while their flags showed
# s_down. Counting healthy peers from `SENTINEL sentinels` is what actually fails when
# quorum breaks.
healthy_peers() { # $1 = pod ordinal
  kubectl -n "$NS" exec "redis-sentinel-$1" -c sentinel -- \
    env -u REDISCLI_AUTH redis-cli -p 26379 sentinel sentinels mymaster \
    | paste - - | awk '$1=="flags" && $2 !~ /s_down|o_down|disconnected/' | wc -l | tr -d ' '
}

for i in 0 1 2; do
  OTHERS=$(healthy_peers "$i")
  [ "$OTHERS" = "2" ] \
    || fail "redis-sentinel-$i can reach ${OTHERS:-0} healthy peers, expected 2 (quorum is 2)"
  pass "redis-sentinel-$i reaches 2 healthy peers"
done

SLAVES=$(sentinel_field 0 num-slaves)
[ "${SLAVES:-0}" -ge 1 ] || fail "Sentinel sees ${SLAVES:-0} replicas, expected at least 1"
pass "Sentinel sees $SLAVES replica(s)"

say "3. Can a client discover the primary through the Sentinel Service?"
# Deliberately goes through the `redis-sentinel` ClusterIP Service rather than a pod
# name — that Service is what REDIS_SENTINEL_NODES actually resolves, and a Service
# that selects nothing would pass a pod-level check while failing every real client.
# The Sentinel port takes no password; the master does. Keeping MASTER_PW out of
# REDISCLI_AUTH means the Sentinel query is unauthenticated, as a real client's is.
PROBE_SCRIPT='
set -e
addr=$(redis-cli -h redis-sentinel -p 26379 sentinel get-master-addr-by-name mymaster)
ip=$(echo "$addr" | head -1)
port=$(echo "$addr" | tail -1)
echo "DISCOVERED $ip:$port"
echo "PING $(REDISCLI_AUTH="$MASTER_PW" redis-cli -h "$ip" -p "$port" ping)"
role=$(REDISCLI_AUTH="$MASTER_PW" redis-cli -h "$ip" -p "$port" info replication \
  | tr -d "\r" | sed -n "s/^role://p")
echo "ROLE $role"
'
DISCOVERY=$(kubectl -n "$NS" run "sentinel-probe-$$" \
  --rm -i --restart=Never --image=redis:7-alpine --quiet \
  --env="MASTER_PW=$REDIS_PW" -- sh -c "$PROBE_SCRIPT")

echo "$DISCOVERY"
echo "$DISCOVERY" | grep -q "^DISCOVERED " || fail "Sentinel Service returned no master address"
echo "$DISCOVERY" | grep -q "^PING PONG$"  || fail "the address Sentinel returned did not answer PING — discovery resolved to something unreachable"
echo "$DISCOVERY" | grep -q "^ROLE master$" || fail "the address Sentinel returned is not the master"
pass "discovery through the Service resolved to a reachable master"

say "RESULT: all checks passed"

if [ "$KEEP" = "--keep" ]; then
  echo "namespace '$NS' left in place (--keep)"
else
  kubectl delete namespace "$NS" --wait=false >/dev/null
  echo "namespace '$NS' deletion requested"
fi
