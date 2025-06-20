# Bash integration test management functions

this="$(realpath "$(dirname "$0")")"
root="$(realpath "${this}/..")"
bindir="${root}/target/release"
realize_bin="${bindir}/realize-cmd"
realized_bin="${bindir}/realize-daemon"
rust_log="debug"

function rebuild {
    cd "${root}" && cargo build --release -p $1
}

function maybe_rebuild {
    [[ -f "${bindir}/$1" ]] || rebuild $1
}

function rebuild_all {
    rebuild realize-cmd && rebuild realize-daemon
}

function up_all {
    maybe_rebuild realize-daemon && up a 7001 7002 7003 && up b 8001 8002 8003
}

function down_all {
    down a
    down b
}

function up {
    inst=$1
    port=$2
    metrics_port=$3
    nfs_port=$4
    configfile="${this}/${inst}.toml"
    keyfile="${root}/resources/test/${inst}.key"
    outfile="${this}/${inst}.out"
    pidfile="${this}/${inst}.pid"
    down $inst

    echo "=== ${inst} Starting on ${port} [metrics ${metrics_port}]"
    {
        cd "${this}"
        RUST_LOG=${RUST_LOG:-${rust_log}} "${realized_bin}" \
            --config "${configfile}" \
            --privkey "${keyfile}" \
            --address "127.0.0.1:${port}" \
            --nfs "127.0.0.1:${nfs_port}" \
            --metrics-addr "127.0.0.1:${metrics_port}" \
            </dev/null >"${outfile}" 2>&1 &
        pid=$!
        echo $pid >"${pidfile}"
    }
    sleep 0.25
    if pgrep -F "${pidfile}" realize-daemon 2>/dev/null; then
        echo "== ${inst} STARTED: PID $pid out ${outfile}"
        head "${outfile}"
        return 0
    else
        echo "== ${inst} FAILED: out ${outfile}"
        head "${outfile}"
        return 1
    fi
}

function down {
    inst=$1
    pidfile="${this}/${inst}.pid"

    if [ -f "${pidfile}" ] && pgrep -F "${pidfile}" realize-daemon 2>/dev/null; then
        pkill -F "${pidfile}"
        echo "=== ${inst} killed"
    else
        echo "=== ${inst} down"
    fi
    rm -f "${pidfile}"
}

function status_all {
    status a
    status b
}

function status {
    inst=$1
    outfile="${this}/${inst}.out"
    pidfile="${this}/${inst}.pid"

    if [ -f "${pidfile}" ] && pgrep -F "${pidfile}" realize-daemon 2>/dev/null; then
        echo "== ${inst} UP $(cat "${pidfile}")"
    else
        echo "== ${inst} DOWN"
    fi
    tail "${outfile}"
}

function metrics_all {
    metrics a && metrics b
}
function metrics {
    inst=$1
    case $inst in
        a) port=7002;;
        b) port=8002;;
        *) echo >&2 "error: unknown instance"
    esac

    echo "=== metrics $inst at ${port}"
    curl http://127.0.0.1:${port}/metrics
    echo
}

function moveall {
    keyfile="${root}/resources/test/client.key"
    peersfile="${this}/client.toml"

    maybe_rebuild realize-cmd && \
        exec "${realize_bin}" \
             --privkey "${keyfile}"\
             --config "${peersfile}" \
             --src a \
             --dst b \
             "$@"
}

function mount {
    inst="$1"

    case "${inst}" in
        a) port=7003 ;;
        b) port=8003 ;;
        *) echo >&2 "error: unknown instance ${inst}"
    esac
         
    mkdir -p "${inst}-nfs"
    sudo mount -t nfs \
         -o user,noauto,noatime,nodiratime,noacl,nolock,vers=3,tcp,wsize=1048576,rsize=131072,actimeo=120,port=${port},mountport=${port} \
         127.0.0.1:/ "${inst}-nfs"
}

function mount_all {
    mount a
    mount b
}

function unmount {
    sudo umount "$1-nfs"
}

function unmount_all {
    unmount a
    unmount b
}
