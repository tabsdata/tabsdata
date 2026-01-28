#
# Copyright 2026 Tabs Data Inc.
#

rpk redpanda start \
  --smp 1 \
  --memory 1G \
  --overprovisioned \
  --kafka-addr internal://${KAFKA_BIND_ADDRESS}:${KAFKA_INTERNAL_LISTENER_PORT} \
  --kafka-addr external://${KAFKA_BIND_ADDRESS}:${KAFKA_EXTERNAL_LISTENER_PORT} \
  --advertise-kafka-addr internal://${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_INTERNAL_LISTENER_PORT} \
  --advertise-kafka-addr external://${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_EXTERNAL_HOST_PORT} \
  --schema-registry-addr ${KAFKA_BIND_ADDRESS}:${KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT} \
  --advertise-pandaproxy-addr ${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_SCHEMA_REGISTRY_HOST_PORT} \
  --rpc-addr ${KAFKA_BIND_ADDRESS}:${KAFKA_RPC_INTERNAL_PORT} \
  --advertise-rpc-addr ${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_RPC_INTERNAL_PORT} \
  --set redpanda.kafka_api='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_INTERNAL_LISTENER_PORT},"authentication_method":"none"},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_EXTERNAL_LISTENER_PORT},"authentication_method":"sasl"}]' \
  --set schema_registry.schema_registry_api='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT}},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_SCHEMA_REGISTRY_HOST_PORT},"authentication_method":"http_basic"}]' \
  --set redpanda.admin='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_ADMIN_INTERNAL_PORT}},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_ADMIN_EXTERNAL_PORT}}]' \
  --set redpanda.enable_sasl=true \
  &

echo '[TABSDATA-FIXTURE] Waiting for Redpanda...'
until rpk cluster health --api-urls localhost:${KAFKA_ADMIN_INTERNAL_PORT} >/dev/null 2>&1; do
  sleep 2
done

echo '[TABSDATA-FIXTURE] Creating Admin User...'
rpk acl user create ${KAFKA_ADMIN_USER} -p ${KAFKA_ADMIN_PASSWORD} --mechanism SCRAM-SHA-256 --api-urls localhost:${KAFKA_ADMIN_INTERNAL_PORT}

echo '[TABSDATA-FIXTURE] Redpanda is Ready!'

wait
