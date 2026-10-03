#!/usr/bin/env bash
# Fresh-cluster Redis write-discovery contract: delete or scale down the primary,
# write through Sentinel discovery, and verify authenticated replication on recovery.
# Usage: FAILURE_MODE=scale|delete scripts/k8s-sentinel-failover-test.sh
# Only creates/deletes its own namespace on minikube/kind/k3d.
set -euo pipefail

NS="recsys-sentinel-drill-$$"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PW="smoketest-redis-pw"
FAILURE_MODE=${FAILURE_MODE:-scale}
SWITCH_BUDGET_S=${SWITCH_BUDGET_S:-120}
WORK="$(mktemp -d)"
CREATED=false
cleanup() {
  if [ "$CREATED" = true ]; then kubectl delete namespace "$NS" --wait=false >/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT
say() { printf '\n== %s\n' "$*"; }
pass() { printf 'ok: %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
case "$FAILURE_MODE" in scale|delete) ;; *) fail "FAILURE_MODE must be scale or delete" ;; esac
ctx="$(kubectl config current-context)"
case "$ctx" in minikube|kind-*|k3d-*) ;; *) fail "refusing non-disposable context '$ctx'" ;; esac

sent() {
  kubectl -n "$NS" exec redis-sentinel-0 -c sentinel -- \
    env -u REDISCLI_AUTH redis-cli --raw -p 26379 "$@" 2>/dev/null | tr -d '\r'
}
master_addr() { sent sentinel get-master-addr-by-name mymaster | paste -sd: -; }
redis_do() {
  local host="$1" port="$2"; shift 2
  kubectl -n "$NS" exec probe -- redis-cli --raw -h "$host" -p "$port" "$@" 2>/dev/null | tr -d '\r'
}
role_of() {
  kubectl -n "$NS" exec "$1" -c redis -- redis-cli --raw info replication 2>/dev/null \
    | tr -d '\r' | sed -n 's/^role://p'
}
write_discovered() {
  local address
  address=$(master_addr) || return 1
  [ -n "$address" ] && [ "$(redis_do "${address%:*}" "${address##*:}" set failover-canary "$1")" = OK ]
}
wait_for() {
  local description="$1"; shift
  local start
  start=$(date +%s)
  until "$@"; do
    [ $(( $(date +%s) - start )) -lt "$SWITCH_BUDGET_S" ] || fail "timed out: $description"
    sleep 2
  done
}
replicas_synced() {
  local pod
  for pod in redis-replica-0 redis-replica-1; do
    kubectl -n "$NS" exec "$pod" -c redis -- redis-cli --raw info replication 2>/dev/null \
      | tr -d '\r' | grep -q '^master_link_status:up$' || return 1
  done
}
was_promoted() { [ "$(master_addr)" != "$BEFORE" ] && write_discovered promoted; }
recovered_replication() {
  [ "$(role_of redis-primary-0)" = slave ] || return 1
  [ "$(kubectl -n "$NS" exec redis-primary-0 -c redis -- redis-cli --raw get failover-canary 2>/dev/null | tr -d '\r')" = recovered ]
}

all_replicas_received_canary() {
  local pod
  for pod in redis-primary-0 redis-replica-0 redis-replica-1; do
    if [ "$(role_of "$pod")" = slave ]; then
      [ "$(kubectl -n "$NS" exec "$pod" -c redis -- redis-cli --raw get failover-canary 2>/dev/null | tr -d '\r')" = recovered ] || return 1
      kubectl -n "$NS" exec "$pod" -c redis -- redis-cli --raw info replication 2>/dev/null \
        | tr -d '\r' | grep -q '^master_link_status:up$' || return 1
    fi
  done
}

say "Fresh namespace $NS ($FAILURE_MODE)"
kubectl create namespace "$NS" >/dev/null
CREATED=true
sed -e "s|__REDIS_PASSWORD__|$REDIS_PW|" -e 's|__CATALOG_PASSWORD__|catalog-pw|' \
  -e 's|__MODEL_PASSWORD__|model-pw|' -e 's|__ONLINE_PASSWORD__|online-pw|' \
  -e 's|__GATEWAY_PASSWORD__|gateway-pw|' -e 's|__RECONCILIATION_PASSWORD__|reconciliation-pw|' \
  "$REPO_ROOT/k8s/base/redis-users.acl.template" > "$WORK/users.acl"
kubectl -n "$NS" create secret generic recsys-secrets --from-literal=redis-password="$REDIS_PW" \
  --from-file=redis-users.acl="$WORK/users.acl" >/dev/null
for file in redis-cluster.yaml configmap.yaml; do
  sed "s/namespace: recsys/namespace: $NS/g; s/recsys.svc.cluster.local/$NS.svc.cluster.local/g" \
    "$REPO_ROOT/k8s/base/$file" | kubectl apply -f - >/dev/null
done
for sts in redis-primary redis-sentinel redis-replica; do
  kubectl -n "$NS" rollout status "statefulset/$sts" --timeout=300s >/dev/null
done
kubectl -n "$NS" run probe --image=redis:7-alpine --restart=Never \
  --env="REDISCLI_AUTH=$REDIS_PW" --command -- sleep 600 >/dev/null
kubectl -n "$NS" wait pod/probe --for=condition=Ready --timeout=120s >/dev/null
wait_for 'replicas authenticated and synchronized' replicas_synced
[ "$(kubectl -n "$NS" get configmap recsys-config -o jsonpath='{.data.REDIS_MODE}')" = sentinel ] || fail 'base must use Sentinel'
[ -z "$(kubectl -n "$NS" get configmap recsys-config -o jsonpath='{.data.REDIS_HOST}')" ] || fail 'base advertises a static write host'
if kubectl -n "$NS" get service redis >/dev/null 2>&1; then fail 'static redis write alias still exists'; fi
[ "$(kubectl -n "$NS" get service redis-primary -o go-template='{{index .metadata.annotations "recsys.io/endpoint-purpose"}}')" = 'bootstrap-only; not an application write endpoint' ] \
  || fail 'redis-primary must be explicitly bootstrap-only'
BEFORE=$(master_addr)
write_discovered before || fail 'initial discovered primary is not writable'
pass "contract: no static application write endpoint; discovery returns $BEFORE"

say "Fail primary ($FAILURE_MODE)"
START=$(date +%s)
if [ "$FAILURE_MODE" = scale ]; then
  kubectl -n "$NS" scale statefulset redis-primary --replicas=0 >/dev/null
  wait_for 'Sentinel promotion and writable discovery' was_promoted
else
  kubectl -n "$NS" delete pod redis-primary-0 --wait=true >/dev/null
  kubectl -n "$NS" rollout status statefulset/redis-primary --timeout=180s >/dev/null
  wait_for 'writable discovery after pod replacement' write_discovered promoted
fi
AFTER=$(master_addr)
pass "writable discovery after ~$(( $(date +%s) - START ))s: $BEFORE -> $AFTER"
# Record detection/election evidence; deletion may recover before the down-after threshold.
for ordinal in 0 1 2; do
  kubectl -n "$NS" logs "redis-sentinel-$ordinal" -c sentinel \
    | grep -E '\+sdown|\+odown|\+vote-for-leader|\+switch-master' || true
done

say 'Recovery and continued writes'
if [ "$FAILURE_MODE" = scale ]; then
  kubectl -n "$NS" scale statefulset redis-primary --replicas=1 >/dev/null
  kubectl -n "$NS" rollout status statefulset/redis-primary --timeout=180s >/dev/null
fi
wait_for 'write after primary recovery' write_discovered recovered
if [ "$AFTER" != "$BEFORE" ]; then
  wait_for 'returning primary demoted and data replicated' recovered_replication
  [ "$(master_addr)" = "$AFTER" ] || fail 'promotion reverted on recovery'
  pass 'returning primary is a replica and receives post-recovery writes'
else
  [ "$(role_of redis-primary-0)" = master ] || fail 'restarted primary is not the discovered master'
  wait_for 'replicas synchronized after short outage' replicas_synced
  pass 'short pod outage recovered before election; replicas synchronized'
fi
wait_for 'every replica received the recovered write' all_replicas_received_canary
masters=0
for pod in redis-primary-0 redis-replica-0 redis-replica-1; do
  role=$(role_of "$pod")
  printf '%s role=%s\n' "$pod" "$role"
  if [ "$role" = master ]; then masters=$((masters + 1)); fi
done
[ "$masters" = 1 ] || fail "split topology: $masters masters"
pass 'exactly one primary; Sentinel-discovered SET succeeds without selector edits'
