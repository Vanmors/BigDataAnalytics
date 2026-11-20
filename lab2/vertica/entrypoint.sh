#!/bin/bash
# Single-node Vertica without the Kubernetes operator: node management agent (NMA) + vcluster.
# First start creates the database, later starts reuse the catalog in /data.
set -euo pipefail

DB_NAME=${VERTICA_DB_NAME:?}
DB_PASSWORD=${VERTICA_PASSWORD:?}
STATE=/data/state                      # certificates and vcluster config survive restarts
CONFIG=$STATE/vertica_cluster.yaml
CERTS=/opt/vertica/config/https_certs
HOST=$(hostname -i | awk '{print $1}')
READY=/tmp/vertica-ready               # healthcheck waits for it: the node is UP before create_db finishes
CATALOG=/data/$DB_NAME/v_${DB_NAME}_node0001_catalog

rm -f "$READY"

mkdir -p "$STATE" "$HOME" "$VTMPDIR"

if [ ! -f "$STATE/rootca.pem" ]; then
    echo "Generating TLS certificates for the node management agent"
    (cd "$STATE" && gen_httpstls_json.sh > "$STATE/gen_certs.log" 2>&1)
    cp "$STATE/rootca_cert.pem" "$STATE/rootca.pem"
fi
# NMA, vcluster and the Vertica HTTPS service (httpstls.json) look for these exact file names.
cp "$STATE/httpstls.json" "$CERTS/httpstls.json"
cp "$STATE/rootca.pem" "$CERTS/rootca.pem"
cp "$STATE/nma_cert.pem" "$CERTS/vertica_https.pem"
cp "$STATE/nma_key.pem" "$CERTS/vertica_https.key"
cp "$STATE/dbadmin_cert.pem" "$CERTS/nma_client.pem"
cp "$STATE/dbadmin_key.pem" "$CERTS/nma_client.key"
cp "$STATE/dbadmin_cert.pem" "$CERTS/dbadmin.pem"
cp "$STATE/dbadmin_key.pem" "$CERTS/dbadmin.key"

rm -f /opt/vertica/config/node_management_agent.pid
manage_node_agent.sh start
for _ in $(seq 30); do
    curl -sk --cert "$CERTS/dbadmin.pem" --key "$CERTS/dbadmin.key" "https://localhost:5554/v1/health" > /dev/null && break
    sleep 1
done

if [ -d "$CATALOG" ] && [ ! -f "$CONFIG" ]; then
    echo "Catalog $CATALOG exists but $CONFIG does not: the first create_db was interrupted." >&2
    echo "Remove the volume (make reset) and start again." >&2
    exit 1
fi

if [ -f "$CONFIG" ]; then
    echo "Starting database $DB_NAME"
    printf '%s' "$DB_PASSWORD" | vcluster start_db --config "$CONFIG" --password-file -
else
    echo "Creating database $DB_NAME"
    printf '%s' "$DB_PASSWORD" | vcluster create_db --db-name "$DB_NAME" --hosts "$HOST" \
        --catalog-path /data --data-path /data --config "$CONFIG" --password-file -
fi

stop_db() {
    echo "Stopping database $DB_NAME"
    printf '%s' "$DB_PASSWORD" | vcluster stop_db --config "$CONFIG" --password-file - || true
    exit 0
}
trap stop_db TERM INT

touch "$READY"
# vertica.log is very verbose, so it stays in the volume instead of the container output.
echo "Database $DB_NAME is up, log: $CATALOG/vertica.log"
sleep infinity &
wait $!
