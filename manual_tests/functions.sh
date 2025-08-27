this="$(realpath "$(dirname "$0")")"
root="$(realpath "${this}/..")"
releasedir="${root}/target/release"
realize_bin="${root}/target/debug/realize-control"
realized_bin="${root}/target/release/realize-daemon"
rust_log="realize_=debug"

function rebuild {
    local args=""
    if [[ "$2" == "release" ]]; then
        args="--release"
    fi
    cd "${root}" && cargo build $args -p $1
}

function maybe_rebuild {
    [[ -f "${root}/target/${2}/$1" ]] || rebuild $1 $2
}

function rebuild_all {
    rebuild realize-control debug && rebuild realize-daemon release
}

function up_all {
    maybe_rebuild realize-daemon release && up a 7001 7003 && up b 8001 8003
}

function down_all {
    down a
    down b
}

function up {
    inst=$1
    port=$2
    nfs_port=$3
    configfile="${this}/${inst}.toml"
    keyfile="${root}/resources/test/${inst}.key"
    outfile="${this}/${inst}.out"
    pidfile="${this}/${inst}.pid"
    sockfile="${this}/${inst}.socket"
    rm -f "${sockfile}"
    down $inst

    echo "=== ${inst} Starting on ${port} [nfs ${nfs_port}] [sock ${sockfile}]"
    {
        cd "${this}"
        RUST_LOG=${RUST_LOG:-${rust_log}} "${realized_bin}" \
            --config "${configfile}" \
            --privkey "${keyfile}" \
            --address "127.0.0.1:${port}" \
            --socket "${sockfile}" \
            --nfs "127.0.0.1:${nfs_port}" \
            --fuse "${this}/${inst}-fuse" \
            </dev/null >"${outfile}" 2>&1 &
        pid=$!
        echo $pid >"${pidfile}"
    }
    sleep 0.25
    if pgrep -F "${pidfile}" realize-daemon 2>/dev/null; then
        echo "== ${inst} STARTED: PID $pid out ${outfile}"
        tail "${outfile}"
        return 0
    else
        echo "== ${inst} FAILED: out ${outfile}"
        tail "${outfile}"
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

function control {
    inst="${1:-a}"
    shift
    maybe_rebuild realize-control debug && \
        exec "${realize_bin}" \
             --socket "${this}/${inst}.socket"\
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

function clean {
    rm -fr *.db *.out *.blob*
}
