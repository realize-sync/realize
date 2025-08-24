#!/bin/sh
args="--address ${ADDRESS}\
 --privkey ${PRIVKEY}\
 --socket ${SOCKFILE}\
 --config ${CONFIG}"

if [ "z${NFS_ADDRESS}" != "z" ]; then
    args="${args} --nfs ${NFS_ADDRESS}"
fi
exec /usr/local/bin/realized $args
