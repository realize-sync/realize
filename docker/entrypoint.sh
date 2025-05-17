#!/bin/sh
exec /usr/local/bin/realized \
  --address "${ADDRESS}" \
  --metrics-addr "${ADDRESS}" \
  --privkey "${PRIVKEY}" \
  --config "${CONFIG}"
