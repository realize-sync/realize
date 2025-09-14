this="$(realpath "$(dirname "$0")")"
root="$(realpath "${this}/..")"
releasedir="${root}/target/release"
realize_bin="${root}/target/debug/realize"
realized_bin="${root}/target/release/realized"
rust_log="realize_=debug,fuser=debug"

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
    maybe_rebuild realized release && up a 7001 7003 && up b 8001 8003
}

function down_all {
    down a
    down b
}

function up {
    inst=$1
    port=$2
    configfile="${this}/${inst}.toml"
    keyfile="${root}/resources/test/${inst}.key"
    outfile="${this}/${inst}.out"
    pidfile="${this}/${inst}.pid"
    sockfile="${this}/${inst}.socket"
    rm -f "${sockfile}"
    down $inst

    echo "=== ${inst} Starting on ${port} [sock ${sockfile}]"
    {
        cd "${this}"
        RUST_LOG=${RUST_LOG:-${rust_log}} "${realized_bin}" \
            --config "${configfile}" \
            --privkey "${keyfile}" \
            --address "127.0.0.1:${port}" \
            --socket "${sockfile}" \
            --fuse "${this}/${inst}-fuse" \
            </dev/null >"${outfile}" 2>&1 &
        pid=$!
        echo $pid >"${pidfile}"
    }
    sleep 0.25
    if pgrep -F "${pidfile}" realized 2>/dev/null; then
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

    if [ -f "${pidfile}" ] && pgrep -F "${pidfile}" realized 2>/dev/null; then
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

    if [ -f "${pidfile}" ] && pgrep -F "${pidfile}" realized 2>/dev/null; then
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

function clean {
    rm -fr *.db *.out *.blob*
}

function merge {
    opts=(
        cache.files=off
        category.create=ff
        category.action=ff
        category.search=ff
        dropcacheonclose=false
        link_cow=true
    )
    sudo mergerfs \
         -o $(IFS=, ; echo "${opts[*]}")\
         ${this}/one-a/data:${this}/a-fuse/one \
         ${this}/one-a/merged
    sudo mergerfs \
         -o $(IFS=, ; echo "${opts[*]}")\
         ${this}/two-a/data:${this}/a-fuse/two \
         ${this}/two-a/merged
}

function unmerge {
    sudo umount ${this}/one-a/merged
    sudo umount ${this}/two-a/merged
}
