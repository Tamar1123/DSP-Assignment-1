#!/bin/bash
# Fleet lab run: several clients, several managers, several workers, at most 9 EC2 instances.
#
# Usage: scripts/lab-fleet-run.sh <clients> <managers> <workers> [input-file]
#
#   clients   number of local applications; each one gets its own slice of the input URLs
#   managers  number of manager nodes to start
#   workers   target number of worker nodes (shared queue, so the fleet-wide total)
#   input     URL list, one per line (default: codeExamples/inputExample.txt)
#
# The script computes everything else:
#
#   manager-clients-n = ceil(clients / managers)
#   worker-files-n    = ceil(task lines / workers)
#   URLs per client   = the input list split into <clients> balanced contiguous slices
#
# Why those numbers: managers are seeded one at a time.  Before each seed client starts,
# every running manager's "free" tag is set to 0, so the new client cannot reuse one and
# has to start its own manager.  Managers publish their own real "free" tag when they admit
# or finish a client; the script does not fake it afterwards.  Later clients see a free
# manager and reuse it.  Workers are shared through one queue and every manager sizes its
# own workers from its own clients' task count, so the fleet-wide worker total is the sum of
# ceil(manager task lines / worker-files-n), capped at <workers>.  The input is cycled when it is
# too short to give <managers> x <workers> task lines.
#
# The cap is enforced twice: by the arithmetic above (managers + expected workers), and by
# a monitor that terminates the newest excess node if the fleet ever goes over the cap.
#
# Flags:
#   --dry-run       print the plan and the URL slices, touch nothing
#   --terminate     after all clients finish, send <managers> TERMINATE messages and
#                   report the acks (cleanup normally stops the nodes anyway)
#   --no-build      use the existing target/App.jar
#   --no-cleanup    leave instances, queues and S3 objects behind
#   --no-preclean   do not terminate stale manager/worker instances first
#   --no-stream     do not print client/EC2 logs live
#
# Environment:
#   DSP_MAX_INSTANCES  instance cap (default 9)
#   DSP_CLIENT_TIMEOUT per-client wall clock in seconds (default 3600)
#   DSP_MANAGER_WAIT   seconds to wait for a seed client's manager (default 300)
set -u
cd "$(dirname "$0")/.."
export AWS_DEFAULT_REGION=us-east-1

die() { echo "error: $*" >&2; exit 2; }

BUCKET=${S3_BUCKET:-${AWS_BUCKET_NAME:-}}
if [ -z "$BUCKET" ]; then
  BUCKET=$(aws s3api list-buckets \
    --query 'Buckets[?starts_with(Name, `dsp1-only-together2-`)].Name | [0]' \
    --output text 2>/dev/null || true)
fi
[ -n "$BUCKET" ] && [ "$BUCKET" != "None" ] || die "could not resolve the S3 bucket; set S3_BUCKET"
LOGS=s3://$BUCKET/assignment1/logs

CAP=${DSP_MAX_INSTANCES:-9}
CLIENT_TIMEOUT=${DSP_CLIENT_TIMEOUT:-3600}
MANAGER_WAIT=${DSP_MANAGER_WAIT:-300}

CLIENT_PIDS=()
TAIL_PIDS=()
CLEANED=0
OUT=

BUILD=1 CLEAN=1 STREAM=1 PRECLEAN=1 TERMINATE=0 DRYRUN=0
POS=()

usage() {
  awk 'NR == 1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
}

for a in "$@"; do
  case "$a" in
    --dry-run)     DRYRUN=1 ;;
    --terminate)   TERMINATE=1 ;;
    --no-build)    BUILD=0 ;;
    --no-cleanup)  CLEAN=0 ;;
    --no-preclean) PRECLEAN=0 ;;
    --no-stream)   STREAM=0 ;;
    -h|--help)     usage; exit 0 ;;
    -*)            usage >&2; die "unknown option: $a" ;;
    *)             POS+=("$a") ;;
  esac
done

case "$CAP" in
  ''|*[!0-9]*) die "DSP_MAX_INSTANCES must be a positive integer, got '$CAP'" ;;
esac
CAP=$((10#$CAP))
case "$CLIENT_TIMEOUT" in
  ''|*[!0-9]*) die "DSP_CLIENT_TIMEOUT must be a positive integer, got '$CLIENT_TIMEOUT'" ;;
esac
case "$MANAGER_WAIT" in
  ''|*[!0-9]*) die "DSP_MANAGER_WAIT must be a positive integer, got '$MANAGER_WAIT'" ;;
esac

[ ${#POS[@]} -ge 3 ] || { usage >&2; exit 2; }
[ ${#POS[@]} -le 4 ] || die "too many arguments"

C=${POS[0]} M=${POS[1]} W=${POS[2]} INPUT=${POS[3]:-codeExamples/inputExample.txt}

for v in C M W; do
  case "${!v}" in
    ''|*[!0-9]*) die "$v must be a non-negative integer, got '${!v}'" ;;
  esac
  printf -v "$v" '%d' "$((10#${!v}))"
done

[ "$C" -ge 1 ] || die "clients must be >= 1"
[ "$M" -ge 1 ] || die "managers must be >= 1"
[ "$W" -ge 1 ] || die "workers must be >= 1"
[ "$M" -le "$C" ] || die "managers ($M) cannot exceed clients ($C): each client starts at most one manager"
[ $((M + W)) -le "$CAP" ] || die "managers + workers = $((M + W)) exceeds the $CAP-instance cap"
[ -r "$INPUT" ] || die "cannot read input file: $INPUT"

# ---- URLs: read, cycle to a useful size, split into balanced slices -----------------

BASE=()
while IFS= read -r line || [ -n "$line" ]; do
  line=${line%$'\r'}
  line=${line#"${line%%[![:space:]]*}"}
  line=${line%"${line##*[![:space:]]}"}
  [ -z "$line" ] && continue
  BASE+=("$line")
done < "$INPUT"
B=${#BASE[@]}
[ "$B" -gt 0 ] || die "input file has no URLs: $INPUT"

# Every manager's nominal share should hold at least <workers> URLs, so the requested
# worker count is reachable; every client needs at least one line.
TARGET=$((M * W))
[ "$TARGET" -lt "$C" ] && TARGET=$C
[ "$TARGET" -lt "$B" ] && TARGET=$B

URLS=()
for ((i = 0; i < TARGET; i++)); do
  URLS+=("${BASE[$((i % B))]}")
done

CHUNK_START=()
CHUNK_SIZE=()
UMAX=0
for ((i = 0; i < C; i++)); do
  s=$((i * TARGET / C))
  e=$(((i + 1) * TARGET / C))
  sz=$((e - s))
  CHUNK_START+=("$s")
  CHUNK_SIZE+=("$sz")
  [ "$sz" -gt "$UMAX" ] && UMAX=$sz
done

# ---- parameters ---------------------------------------------------------------------

N=$(((C + M - 1) / M))            # manager-clients-n
F=$(((TARGET + W - 1) / W))       # worker-files-n = ceil(task lines / workers)
[ "$F" -lt 1 ] && F=1
EXPECTED=$(((TARGET + F - 1) / F)) # workers the fleet asks for, across all managers
[ "$EXPECTED" -gt "$W" ] && EXPECTED=$W

CYCLED="no cycling"
if [ "$TARGET" -gt "$B" ]; then
  CYCLES=$(((TARGET + B - 1) / B))
  CYCLED="$B base line(s), cycled ${CYCLES}x"
fi

printf '=== fleet plan ===\n'
printf 'input          : %s (%d non-empty line(s))\n' "$INPUT" "$B"
printf 'task lines     : %d (%s)\n' "$TARGET" "$CYCLED"
printf 'clients        : %d local app(s), %d URL(s) each (max)\n' "$C" "$UMAX"
printf 'managers       : %d EC2 node(s), manager-clients-n=%d (%d client(s) each)\n' "$M" "$N" "$N"
printf 'workers        : target %d, worker-files-n=%d, expected <=%d across all managers\n' "$W" "$F" "$EXPECTED"
printf 'instances      : %d manager(s) + <=%d worker(s) = <=%d (cap %d)\n' "$M" "$EXPECTED" "$((M + EXPECTED))" "$CAP"
printf 'worker math    : %d task line(s), %d per worker -> %d worker(s) fleet-wide\n' "$TARGET" "$F" "$EXPECTED"

if [ "$DRYRUN" = 1 ]; then
  printf '\n=== URL slices ===\n'
  for ((i = 0; i < C; i++)); do
    s=${CHUNK_START[$i]}
    sz=${CHUNK_SIZE[$i]}
    first=${URLS[$s]}
    last=${URLS[$((s + sz - 1))]}
    printf 'client %d: %2d line(s)  %s ... %s\n' "$i" "$sz" "$first" "$last"
  done
  exit 0
fi

# ---- preflight ----------------------------------------------------------------------

aws sts get-caller-identity --query Account --output text >/dev/null 2>&1 \
  || die "no valid AWS credentials (aws sts get-caller-identity failed)"

live_list() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=manager,worker" \
              "Name=instance-state-name,Values=pending,running" \
    --query 'Reservations[].Instances[].[InstanceId,LaunchTime,Tags[?Key==`Name`]|[0].Value]' \
    --output text 2>/dev/null
}

live_count() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=manager,worker" \
              "Name=instance-state-name,Values=pending,running" \
    --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo 0
}

managers_live() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=manager" \
              "Name=instance-state-name,Values=pending,running" \
    --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo 0
}

# Running managers only: a pending manager is not serving yet, so it is not tagged.
running_manager_ids() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=manager" \
              "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null
}

zero_free_tags() {
  local id ids
  ids=$(running_manager_ids)
  [ -z "$ids" ] && return
  for id in $ids; do
    aws ec2 create-tags --resources "$id" --tags Key=free,Value=0 >/dev/null 2>&1 || true
  done
}

cleanup() {
  [ "$CLEANED" = 1 ] && return
  CLEANED=1
  echo
  echo "=== cleanup ==="
  if [ ${#TAIL_PIDS[@]} -gt 0 ]; then
    for p in "${TAIL_PIDS[@]}"; do
      command -v pkill >/dev/null 2>&1 && pkill -P "$p" 2>/dev/null
      kill "$p" 2>/dev/null
    done
  fi
  for i in "${!CLIENT_PIDS[@]}"; do
    kill "${CLIENT_PIDS[$i]}" 2>/dev/null
  done
  if [ "$CLEAN" = 0 ]; then
    echo "skipped (--no-cleanup); workspace ${OUT:-none}"
    return
  fi

  IDS=$(live_list | awk '{print $1}')
  if [ -n "$IDS" ]; then
    aws ec2 terminate-instances --instance-ids $IDS \
      --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text
  fi
  for _ in $(seq 1 60); do
    [ -z "$(live_list | awk '{print $1}')" ] && break
    sleep 3
  done

  for q in $(aws sqs list-queues --query 'QueueUrls[]' --output text 2>/dev/null); do
    aws sqs delete-queue --queue-url "$q" 2>/dev/null && echo "deleted $q"
  done
  aws s3 rm "s3://$BUCKET/assignment1/summaries/" --recursive --quiet
  aws s3 rm "s3://$BUCKET/assignment1/input-files/" --recursive --quiet
  aws s3 rm "$LOGS/" --recursive --quiet

  echo "node instances:"
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=manager,worker" \
    --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text \
    | grep -v terminated || echo "all terminated"
  echo "workspace: ${OUT:-none}"
  echo "note: deleted queue names need ~60s before a queue with the same name can be recreated"
}
trap 'cleanup; exit 130' INT TERM

if [ "$PRECLEAN" = 1 ]; then
  IDS=$(live_list | awk '{print $1}')
  if [ -n "$IDS" ]; then
    echo "[pre] terminating stale manager/worker instances: $(echo "$IDS" | tr '\n' ' ')"
    aws ec2 terminate-instances --instance-ids $IDS >/dev/null
    for _ in $(seq 1 60); do
      [ -z "$(live_list | awk '{print $1}')" ] && break
      sleep 3
    done
    [ -n "$(live_list | awk '{print $1}')" ] && echo "[pre] WARN instances still live after 180s"
  fi
fi

# Stale messages would be consumed by the new nodes; purge rather than delete, because a
# deleted queue name is unavailable for about a minute.
for q in worker_queue $(aws sqs list-queues --queue-name-prefix manager_ --query 'QueueUrls[]' --output text 2>/dev/null | tr '\t' '\n' | sed 's|.*/||'); do
  url=$(aws sqs get-queue-url --queue-name "$q" --query QueueUrl --output text 2>/dev/null || true)
  case "$url" in
    ''|None) ;;
    *) if aws sqs purge-queue --queue-url "$url" >/dev/null 2>&1; then
         echo "[pre] purged $q"
       else
         echo "[pre] purge throttled for $q; draining instead"
         for _ in 1 2 3 4 5; do
           handles=$(aws sqs receive-message --queue-url "$url" \
             --max-number-of-messages 10 --wait-time-seconds 1 \
             --query 'Messages[].ReceiptHandle' --output text 2>/dev/null || true)
           [ -z "$handles" ] && break
           while read -r h; do
             [ -n "$h" ] && aws sqs delete-message --queue-url "$url" --receipt-handle "$h" >/dev/null 2>&1
           done <<< "$handles"
         done
       fi ;;
  esac
done

if [ "$BUILD" = 1 ]; then
  mvn -o -q package -DskipTests || die "build failed"
fi

OUT=/tmp/lab-fleet-$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
for ((i = 0; i < C; i++)); do
  printf '%s\n' "${URLS[@]:${CHUNK_START[$i]}:${CHUNK_SIZE[$i]}}" > "$OUT/in-$i.txt"
done
echo "[plan] workspace: $OUT"

aws s3 rm "$LOGS/" --recursive --quiet >/dev/null 2>&1 || true

start_client() {
  local i=$1
  timeout "$CLIENT_TIMEOUT" java -jar target/App.jar \
    "$OUT/in-$i.txt" "$OUT/out-$i.html" "$N" "$F" -D > "$OUT/client-$i.log" 2>&1 &
  CLIENT_PIDS[$i]=$!
  echo "[launch] client $i pid ${CLIENT_PIDS[$i]} ($(wc -l < "$OUT/in-$i.txt") URL(s))"
}

# ---- start managers: seed clients one by one, zeroing the free tag between them ------

LAUNCHED=0
while [ "$LAUNCHED" -lt "$C" ] && [ "$(managers_live)" -lt "$M" ]; do
  BEFORE=$(managers_live)
  zero_free_tags
  start_client "$LAUNCHED"
  LAUNCHED=$((LAUNCHED + 1))
  DEADLINE=$((SECONDS + MANAGER_WAIT))
  while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ "$(managers_live)" -gt "$BEFORE" ] && break
    zero_free_tags
    sleep 5
  done
  if [ "$(managers_live)" -le "$BEFORE" ]; then
    echo "[seed] WARN no new manager within ${MANAGER_WAIT}s; starting another seed"
  fi
done
K=$(managers_live)
if [ "$K" -lt "$M" ]; then
  echo "[seed] WARN only $K/$M manager(s) started after $LAUNCHED client(s)"
elif [ "$K" -gt "$M" ]; then
  echo "[seed] WARN $K managers started (asked for $M); cap guard will keep the fleet at $CAP"
fi
for ((i = LAUNCHED; i < C; i++)); do
  start_client "$i"
done

if [ "$STREAM" = 1 ]; then
  for ((i = 0; i < C; i++)); do
    ( tail -n +1 -F "$OUT/client-$i.log" 2>/dev/null | sed "s/^/[c$i] /" ) &
    TAIL_PIDS+=($!)
  done
fi

# ---- live view, cap guard, wait for clients -----------------------------------------

enforce_cap() {
  local list count over ids have
  list=$(live_list)
  count=$(printf '%s\n' "$list" | grep -c . || true)
  [ "$count" -le "$CAP" ] && return
  over=$((count - CAP))
  echo "[cap] WARN $count instances live > $CAP; terminating $over newest worker(s)"
  ids=$(printf '%s\n' "$list" | awk '$3 == "worker"' | sort -k2,2r | awk -v n="$over" 'NR <= n {print $1}')
  have=$(printf '%s\n' "$ids" | grep -c . || true)
  if [ "$have" -lt "$over" ]; then
    ids="$ids $(printf '%s\n' "$list" | awk '$3 != "worker"' | sort -k2,2r | awk -v n="$((over - have))" 'NR <= n {print $1}')"
  fi
  [ -n "$(echo "$ids" | tr -d ' ')" ] && aws ec2 terminate-instances --instance-ids $ids >/dev/null
}

declare -A SEEN
declare -A SEEN_SIZE
poll_logs() {
  local sz f n t
  while read -r sz f; do
    [ -z "$f" ] && continue
    [ "${SEEN_SIZE[$f]:-}" = "$sz" ] && continue
    aws s3 cp "$LOGS/$f" "/tmp/lab-fleet-$f" --quiet 2>/dev/null || continue
    n=${SEEN[$f]:-0}
    t=$(wc -l < "/tmp/lab-fleet-$f")
    if [ "$t" -gt "$n" ]; then
      tail -n +$((n + 1)) "/tmp/lab-fleet-$f" | sed "s/^/[${f%.log}] /"
      SEEN[$f]=$t
    fi
    SEEN_SIZE[$f]=$sz
  done < <(aws s3 ls "$LOGS/" 2>/dev/null | awk '{print $3, $4}')
}

while :; do
  ALIVE=0
  for i in "${!CLIENT_PIDS[@]}"; do
    kill -0 "${CLIENT_PIDS[$i]}" 2>/dev/null && ALIVE=1
  done
  [ "$ALIVE" -eq 0 ] && break
  enforce_cap
  poll_logs
  sleep 5
done
poll_logs

echo
echo "=== results ==="
FAILED=0
for i in "${!CLIENT_PIDS[@]}"; do
  wait "${CLIENT_PIDS[$i]}"
  rc=$?
  [ "$rc" -ne 0 ] && FAILED=$((FAILED + 1))
  if [ -f "$OUT/out-$i.html" ]; then
    echo "client $i: rc=$rc, $OUT/out-$i.html ($(wc -l < "$OUT/out-$i.html") lines)"
  else
    echo "client $i: rc=$rc, NO OUTPUT (see $OUT/client-$i.log)"
  fi
done
MLIVE=$(managers_live)
LLIVE=$(live_count)
echo "nodes: $MLIVE manager(s), $((LLIVE - MLIVE)) worker(s), $LLIVE/$CAP instance(s)"

# ---- optional TERMINATE after every client is done -----------------------------------

if [ "$TERMINATE" = 1 ]; then
  ACKQ=client_lab-fleet
  MGRQS=$(aws sqs list-queues --queue-name-prefix manager_ --query 'QueueUrls[]' --output text 2>/dev/null | tr '\t' ' ' || true)
  if [ -z "$MGRQS" ] || [ "$MGRQS" = None ]; then
    echo "[terminate] no manager_* queue found; skipped"
  else
    ACKURL=$(aws sqs create-queue --queue-name "$ACKQ" --query QueueUrl --output text)
    BODY='{"type":"TERMINATE","localAppID":"lab-fleet"}'
    N=0
    for MGRQ in $MGRQS; do
      aws sqs send-message --queue-url "$MGRQ" --message-body "$BODY" >/dev/null
      N=$((N + 1))
    done
    echo "[terminate] sent TERMINATE to $N manager queue(s), waiting for acks"
    sleep 20
    ACKS=$(aws sqs get-queue-attributes --queue-url "$ACKURL" \
             --attribute-names ApproximateNumberOfMessages \
             --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo 0)
    echo "[terminate] acks in $ACKQ: $ACKS/$N"
  fi
fi

# ---- cleanup ------------------------------------------------------------------------

cleanup
[ "$FAILED" -eq 0 ] || exit 1
