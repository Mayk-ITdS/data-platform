#!/usr/bin/env bash
# Run as root on the dedicated Terraform VM after cloud-init has completed.
set -euo pipefail
test "$(id -u)" = 0
device=$(readlink -e /dev/disk/by-id/google-ceph-osd)
host=$(hostname -s)
ip=$(hostname -I | awk '{print $1}')
if ! test -s /etc/ceph/ceph.conf; then
  config=$(mktemp)
  trap 'rm -f "$config"' EXIT
  cat >"$config" <<EOF
[global]
osd_pool_default_size = 1
osd_pool_default_min_size = 1
mon_warn_on_pool_no_redundancy = false
EOF
  cephadm bootstrap --mon-ip "$ip" --single-host-defaults --skip-dashboard --config "$config"
fi
ceph() { cephadm shell -- ceph "$@"; }
if ! ceph orch device ls --format json | python3 -c 'import json,sys; d=sys.argv[1]; sys.exit(not any(x.get("path")==d and x.get("available") for h in json.load(sys.stdin) for x in h.get("devices",[])))' "$device"; then
  ceph osd tree -f json | python3 -c 'import json,sys; assert any(n["type"]=="osd" for n in json.load(sys.stdin)["nodes"]), "Dedicated disk is unavailable; no OSD present"'
else
  ceph orch daemon add osd "$host:$device"
fi
ceph orch apply rgw fema --placement="1 $host" --port=7480
for attempt in {1..90}; do
  if curl -fsS "http://$ip:7480" >/dev/null; then break; fi
  sleep 2
done
curl -fsS "http://$ip:7480" >/dev/null
umask 077
secrets=/etc/fema-lakehouse.env
if [[ ! -s $secrets ]]; then
  user=$(cephadm shell -- radosgw-admin user info --uid=fema 2>/dev/null ||
    cephadm shell -- radosgw-admin user create --uid=fema --display-name=FEMA)
  temp=$(mktemp /etc/fema-env.XXXXXX)
  python3 -c 'import json,secrets,sys,shlex; key=json.load(sys.stdin)["keys"][0]
for k,v in {"AWS_ACCESS_KEY_ID":key["access_key"],"AWS_SECRET_ACCESS_KEY":key["secret_key"],"CATALOG_PASSWORD":secrets.token_hex(24),"AWS_DEFAULT_REGION":"us-east-1","FEMA_REST_URI":"http://iceberg-rest:8181","FEMA_S3_ENDPOINT":"http://ceph-rgw:7480"}.items(): print(k+"="+shlex.quote(v))' <<<"$user" >"$temp"
  mv "$temp" "$secrets"
fi
set -a
source "$secrets"
set +a
aws --endpoint-url "http://$ip:7480" s3api head-bucket --bucket fema-lakehouse 2>/dev/null ||
  aws --endpoint-url "http://$ip:7480" s3api create-bucket --bucket fema-lakehouse
docker network inspect fema-cdc >/dev/null 2>&1 || docker network create fema-cdc
docker compose -f infrastructure/docker/compose.lakehouse.yml up -d
