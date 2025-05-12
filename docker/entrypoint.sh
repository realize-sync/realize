#!/bin/sh
exec /usr/local/bin/realized \
  --address "${ADDRESS}" \
  --privkey "${PRIVKEY}" \
  --config "${CONFIG}"
