#!/bin/sh
exec /usr/local/bin/realized \
  --address "${ADDRESS}" \
  --metrics-addr "${METRICS_ADDRESS}" \
  --privkey "${PRIVKEY}" \
  --config "${CONFIG}"
