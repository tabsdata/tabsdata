#
# Copyright 2026 Tabs Data Inc.
#

rpk redpanda start \
  --smp 1 \
  --memory 1G \
  --overprovisioned \
  --advertise-kafka-addr internal://${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_INTERNAL_LISTENER_PORT},external://${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_EXTERNAL_HOST_PORT} \
  --set redpanda.kafka_api='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_INTERNAL_LISTENER_PORT},"authentication_method":"none"},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_EXTERNAL_LISTENER_PORT},"authentication_method":"sasl"}]' \
  --set schema_registry.schema_registry_api='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT}},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT},"authentication_method":"http_basic"}]' \
  --set redpanda.admin='[{"name":"internal","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_ADMIN_INTERNAL_PORT}},{"name":"external","address":"${KAFKA_BIND_ADDRESS}","port":${KAFKA_ADMIN_EXTERNAL_PORT}}]' \
  &

echo '[td fixture] Waiting for Redpanda...'
until rpk cluster health --api-urls ${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_ADMIN_INTERNAL_PORT} >/dev/null 2>&1; do
  sleep 2
done

echo '[td fixture] Creating Admin User...'
rpk acl user create ${KAFKA_ADMIN_USER} -p ${KAFKA_ADMIN_PASSWORD} --mechanism SCRAM-SHA-256 --api-urls ${KAFKA_ADVERTISE_ADDRESS}:${KAFKA_ADMIN_INTERNAL_PORT}
rpk security acl create --allow-principal ${KAFKA_ADMIN_USER} --operation all --topic '*' --group '*'

echo '[td fixture] Redpanda is Ready!'

wait
