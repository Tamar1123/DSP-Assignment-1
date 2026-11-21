#!/bin/bash
# Small lab run: 1 client, 1 manager, 1 worker, 3 URLs. Streams client, manager and worker
# logs live (EC2 logs are synced to S3 every 5s because -D is on). Ctrl-C cleans up.
# Usage: scripts/lab-run.sh [--no-build] [--no-cleanup]
set -u
cd "$(dirname "$0")/.."
export AWS_DEFAULT_REGION=us-east-1
BUCKET=$(grep -o 'bucketName = "[^"]*"' src/main/java/utils/AWS.java | cut -d'"' -f2)
LOGS=s3://$BUCKET/assignment1/logs
BUILD=1; CLEAN=1
for a in "$@"; do [ "$a" = --no-build ] && BUILD=0; [ "$a" = --no-cleanup ] && CLEAN=0; done

aws sts get-caller-identity --query Account --output text >/dev/null || { echo "No valid AWS creds"; exit 1; }
if [ $BUILD = 1 ]; then mvn -o -q package -DskipTests || exit 1; fi

head -3 codeExamples/inputExample.txt > /tmp/lab-in.txt
rm -f /tmp/lab-out.html /tmp/lab-client.log
CLIENT_PID=""; TAIL_PID=""

cleanup() {
  echo; echo "=== cleanup ==="
  [ -n "$TAIL_PID" ] && kill $TAIL_PID 2>/dev/null
  [ -n "$CLIENT_PID" ] && kill $CLIENT_PID 2>/dev/null
  [ $CLEAN = 0 ] && { echo "skipped (--no-cleanup)"; return; }
  IDS=$(aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running \
        --query 'Reservations[].Instances[].InstanceId' --output text)
  [ -n "$IDS" ] && aws ec2 terminate-instances --instance-ids $IDS --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text
  for q in $(aws sqs list-queues --query 'QueueUrls[]' --output text); do aws sqs delete-queue --queue-url $q; echo "deleted $q"; done
  aws s3 rm s3://$BUCKET/assignment1/summaries/ --recursive --quiet
  aws s3 rm s3://$BUCKET/assignment1/input-files/ --recursive --quiet
  aws s3 rm $LOGS/ --recursive --quiet
  sleep 30
  echo "instance states:"; aws ec2 describe-instances --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text | grep -v terminated || echo "all terminated"
}
trap 'cleanup; exit 130' INT TERM

aws s3 rm $LOGS/ --recursive --quiet
# n=1 client per manager, 100 files per worker -> exactly 1 worker for 3 URLs.
java -jar target/App.jar /tmp/lab-in.txt /tmp/lab-out.html 1 100 terminate -D > /tmp/lab-client.log 2>&1 &
CLIENT_PID=$!

# Live view: client log directly, EC2 logs polled from S3 and printed as they grow.
( tail -n +1 -F /tmp/lab-client.log 2>/dev/null | sed 's/^/[local-client] /' ) &
TAIL_PID=$!
declare -A SEEN
while kill -0 $CLIENT_PID 2>/dev/null; do
  for f in $(aws s3 ls $LOGS/ 2>/dev/null | awk '{print $4}'); do
    aws s3 cp $LOGS/$f /tmp/lab-$f --quiet 2>/dev/null || continue
    n=${SEEN[$f]:-0}; t=$(wc -l < /tmp/lab-$f)
    if [ "$t" -gt "$n" ]; then tail -n +$((n+1)) /tmp/lab-$f | sed "s/^/[${f%.log}] /"; SEEN[$f]=$t; fi
  done
  sleep 5
done
wait $CLIENT_PID; RC=$?
echo "=== client exited rc=$RC ==="
[ -f /tmp/lab-out.html ] && { echo "output: /tmp/lab-out.html"; head -c 600 /tmp/lab-out.html; echo; }
cleanup
