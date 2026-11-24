#!/usr/bin/env bash
# Temporary Oracle schema on the GitHub-hosted scanner runner.
set -euo pipefail
password="Fema$(openssl rand -hex 18)"
echo "::add-mask::$password"
echo "DICTIONARY_PASSWORD=$password" >> "$GITHUB_ENV"
export APP_USER_PASSWORD="$password"
docker run -d --name fema-sonar-oracle -p 127.0.0.1:1521:1521 \
  -e ORACLE_RANDOM_PASSWORD=yes -e APP_USER=ASSUREPLUS -e APP_USER_PASSWORD \
  gvenzl/oracle-free:23-slim-faststart
for i in {1..120}; do
  if docker exec fema-sonar-oracle healthcheck.sh >/dev/null 2>&1; then break; fi
  sleep 3
done
docker exec fema-sonar-oracle healthcheck.sh
docker exec -i fema-sonar-oracle bash -c 'exec sqlplus -L -s "ASSUREPLUS/$APP_USER_PASSWORD@//localhost:1521/FREEPDB1"' \
  < data/source_system/dbs/oracle/migrations/oracle_init.sql
curl -fsSL --retry 3 -o "$RUNNER_TEMP/fema-ojdbc.jar" \
  https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc11/23.6.0.24.10/ojdbc11-23.6.0.24.10.jar
