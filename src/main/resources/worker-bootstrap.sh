#!/bin/bash
set -e
exec > >(tee -a /var/log/dsp-bootstrap.log) 2>&1
mkdir -p /home/ec2-user
export DEBIAN_FRONTEND=noninteractive
if ! command -v aws >/dev/null 2>&1 || ! command -v java >/dev/null 2>&1; then
  apt-get update -y && apt-get install -y awscli openjdk-11-jre-headless
fi
aws s3 cp s3://@BUCKET@/@JAR_KEY@ @JAR_PATH@
JAVA_BIN=$(command -v java)
@DEBUG_LOG_UPLOAD@set +e
"$JAVA_BIN" -jar @JAR_PATH@ > /home/ec2-user/worker.log 2>&1
STATUS=$?
cat /home/ec2-user/worker.log > /dev/console || true
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --region @REGION@ --instance-ids "$INSTANCE_ID" || true
exit $STATUS
