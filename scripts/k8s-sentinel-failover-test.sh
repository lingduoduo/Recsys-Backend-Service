#!/usr/bin/env bash
# Kills redis-primary for real and checks what Sentinel does about it.
#
# Answers issue #338. Three assertions plus one warning:
#
#   A. Sentinel promotes a replica when the primary dies (+switch-master).
#   B. A Sentinel-discovering client can WRITE to the promotion. A replica answers
#      PING and refuses SET with -READONLY, so only a write proves a real promotion.
#   C. When the old primary comes back it rejoins as a replica, and Sentinel keeps
#      pointing at the promoted one rather than flipping back.
#
#   WARNING: after C, the `redis-primary` and `redis` Services still select
#   `role: primary` — a static pod label that failover does not move — so they resolve
#   to a DEMOTED REPLICA. A client on REDIS_HOST gets PONG on ping, correct data on
#   reads, and -READONLY on every write, indefinitely. Sentinel clients are unaffected;
#   this is why k8s/base sets REDIS_MODE=sentinel. The script reports it rather than
#   failing, because it is current documented behaviour (04_Replication.md sharp edge 7),
#   not a regression.
#
# Usage: scripts/k8s-sentinel-failover-test.sh
# Requires: kubectl on a throwaway cluster. Deletes the `recsys` namespace.

set -uo pipefail

NS=recsys
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PW="smoketest-redis-pw"
SWITCH_BUDGET_S=${SWITCH_BUDGET_S:-120}
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
pass() { printf '\033[32mok:\033[0m %s\n' "$*"; }
warn() { printf '\033[33mWARNING:\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

ctx="$(kubectl config current-context 2>/dev/null)"
case "$ctx" in
  minikube|kind-*|k3d-*) ;;
  *) fail "refusing to run against context '$ctx' — this deletes the '$NS' namespace" ;;
esac

# Sentinel sets no requirepass, but its pods carry REDISCLI_AUTH to reach the master, so
# redis-cli would send AUTH to Sentinel and print an error while still answering.
sent() { kubectl -n "$NS" exec redis-sentinel-0 -c sentinel -- \
  env -u REDISCLI_AUTH redis-cli -p 26379 "$@" 2>/dev/null | tr -d '\r'; }
master_addr() { sent sentinel get-master-addr-by-name mymaster | paste -sd: -; }
role_of()  { kubectl -n "$NS" exec "$1" -c redis -- redis-cli info replication 2>/dev/null | tr -d '\r' | sed -n 's/^role://p'; }
endpoints_of() { kubectl -n "$NS" get endpoints "$1" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null; }

redis_do() { # $1 host  $2 port  $3.. command
  local h="$1" p="$2"; shift 2
  kubectl -n "$NS" run "probe-$RANDOM" --rm -i --restart=Never --image=redis:7-alpine \
    --quiet --env="PW=$REDIS_PW" -- \
    sh -c "REDISCLI_AUTH=\$PW redis-cli -h $h -p $p -t 5 $* 2>&1 | head -1" 2>/dev/null | tr -d '\r' | head -1
}

say "Setup — a clean cluster, deliberately"
# Sentinel adds the demoted master to its replica list. Because that "master" is a
# ClusterIP, a stack that has already failed over carries a permanently-dead slave entry
# which slows the next election from under a second to minutes. Starting dirty measures
# that artefact instead of the behaviour under test.
kubectl get namespace "$NS" >/dev/null 2>&1 && kubectl delete namespace "$NS" --timeout=180s >/dev/null
kubectl apply -f "$REPO_ROOT/k8s/base/namespace.yaml" >/dev/null
sed -e "s|__REDIS_PASSWORD__|$REDIS_PW|" -e "s|__CATALOG_PASSWORD__|catalog-pw|" \
    -e "s|__MODEL_PASSWORD__|model-pw|" -e "s|__ONLINE_PASSWORD__|online-pw|" \
    -e "s|__GATEWAY_PASSWORD__|gateway-pw|" -e "s|__RECONCILIATION_PASSWORD__|reconciliation-pw|" \
    "$REPO_ROOT/k8s/base/redis-users.acl.template" > "$WORK/users.acl"
kubectl -n "$NS" create secret generic recsys-secrets \
  --from-literal=redis-password="$REDIS_PW" --from-file=redis-users.acl="$WORK/users.acl" >/dev/null
kubectl apply -f "$REPO_ROOT/k8s/base/redis-cluster.yaml" >/dev/null
for sts in redis-primary redis-sentinel redis-replica; do
  kubectl -n "$NS" rollout status "statefulset/$sts" --timeout=300s >/dev/null \
    || fail "$sts never became ready"
done
BEFORE=$(master_addr)
note "sentinel master before : $BEFORE   (a ClusterIP — see sharp edge 7)"
note "redis-primary-0 role   : $(role_of redis-primary-0)"

say "A. Does Sentinel promote when the primary dies?"
kubectl -n "$NS" scale statefulset redis-primary --replicas=0 >/dev/null
note "redis-primary scaled to 0; waiting up to ${SWITCH_BUDGET_S}s for +switch-master"
T0=$(date +%s); AFTER=""
while [ $(( $(date +%s) - T0 )) -lt "$SWITCH_BUDGET_S" ]; do
  cur=$(master_addr)
  if [ -n "$cur" ] && [ "$cur" != "$BEFORE" ]; then AFTER="$cur"; break; fi
  sleep 5
done
ELAPSED=$(( $(date +%s) - T0 ))
[ -n "$AFTER" ] || fail "no +switch-master within ${SWITCH_BUDGET_S}s; sentinel still reports $BEFORE"
pass "promoted in ~${ELAPSED}s: $BEFORE -> $AFTER"
sent sentinel master mymaster | paste - - | awk '$1=="flags"{print "   master flags           : "$2}'

say "B. Can a Sentinel-discovering client write to the promotion?"
H="${AFTER%:*}"; P="${AFTER##*:}"
W=$(redis_do "$H" "$P" set failover-canary promoted)
[ "$W" = "OK" ] || fail "write to the promoted master returned '$W' (a -READONLY here means nothing was really promoted)"
pass "SET against $AFTER returned OK"

say "C. Does the old primary rejoin as a replica?"
kubectl -n "$NS" scale statefulset redis-primary --replicas=1 >/dev/null
kubectl -n "$NS" rollout status statefulset/redis-primary --timeout=180s >/dev/null
note "waiting 40s for Sentinel to reconfigure the returning pod"
sleep 40
OLD_ROLE=$(role_of redis-primary-0)
NOW=$(master_addr)
note "redis-primary-0 role   : $OLD_ROLE"
note "sentinel master        : $NOW"
[ "$OLD_ROLE" = "slave" ] || fail "redis-primary-0 came back as '$OLD_ROLE'; expected it to be demoted to slave"
[ "$NOW" = "$AFTER" ] || fail "sentinel master moved to $NOW after recovery; expected it to stay at $AFTER"
pass "old primary rejoined as a replica; the promotion held"

say "The Service trap — reported, not asserted"
SVC_EP=$(endpoints_of redis-primary)
POD_IP=$(kubectl -n "$NS" get pod redis-primary-0 -o jsonpath='{.status.podIP}' 2>/dev/null)
note "redis-primary Service -> $SVC_EP"
note "redis-primary-0 podIP -> $POD_IP   (role: $OLD_ROLE)"
PING=$(redis_do redis-primary 6379 ping)
SET=$(redis_do redis-primary 6379 set after-recovery x)
note "via the Service: PING -> $PING"
note "via the Service: SET  -> $SET"
if [ "$SET" = "OK" ]; then
  pass "the Service resolves to a writable master"
else
  warn "the 'redis-primary' and 'redis' Services select role=primary, a static pod label"
  warn "that failover does not move, so they now resolve to a demoted replica:"
  warn "  PING succeeds, reads succeed, every write fails with -READONLY, indefinitely."
  warn "Sentinel clients are unaffected. This is why k8s/base sets REDIS_MODE=sentinel."
  warn "See docs/system_design/04_Replication.md sharp edge 7 and issue #338."
fi

say "RESULT: failover assertions passed"
kubectl delete namespace "$NS" --wait=false >/dev/null
echo "namespace '$NS' deletion requested"
