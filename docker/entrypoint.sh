#!/bin/sh
args="--address ${ADDRESS}\
 --metrics-addr ${METRICS_ADDRESS}\
 --privkey ${PRIVKEY}\
 --config ${CONFIG}"

if [ "z${NFS_ADDRESS}" != "z" ]; then
    args="${args} --nfs ${NFS_ADDRESS}"
fi
exec /usr/local/bin/realized $args
