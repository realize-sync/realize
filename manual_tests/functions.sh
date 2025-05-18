# Bash integration test management functions

this="$(realpath "$(dirname "$0")")"
root="$(realpath "${this}/..")"
bindir="${root}/target/release"
realize_bin="${bindir}/realize"
realized_bin="${bindir}/realized"
rust_log="debug"

function rebuild {
    cd "${root}" && cargo build --release --bin $1
}

function maybe_rebuild {
    [[ -f "${bindir}/$1" ]] || rebuild $1
}

function rebuild_all {
    cd "${root}" && cargo build --release --bin realize --bin realized
}

function up_all {
    maybe_rebuild realized && up a 7001 7002 && up b 8001 8002
}

function down_all {
    down a
    down b
}

function up {
    inst=$1
    port=$2
    metrics_port=$3
    configfile="${this}/${inst}.yaml"
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
            --metrics-addr "127.0.0.1:${metrics_port}" \
            </dev/null >"${outfile}" 2>&1 &
        pid=$!
        echo $pid >"${pidfile}"
    }
    sleep 0.25
    if pgrep -q -F "${pidfile}" realized 2>/dev/null; then
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

    if [ -f "${pidfile}" ] && pgrep -q -F "${pidfile}" realized 2>/dev/null; then
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

    if [ -f "${pidfile}" ] && pgrep -q -F "${pidfile}" realized 2>/dev/null; then
        echo "== a UP $(cat "${pidfile}")"
    else
        echo "== a DOWN"
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
    curl http://localhost:${port}/metrics
    echo
}

function moveall {
    keyfile="${root}/resources/test/client.key"
    peersfile="${root}/resources/test/peers.pem"

    maybe_rebuild realize && \
        exec "${realize_bin}" \
             --privkey "${keyfile}"\
             --peers "${peersfile}" \
             --src-addr "localhost:7001" \
             --dst-addr "localhost:8001" \
             "$@"
}

