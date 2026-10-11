#!/bin/bash
# Runs every privileged macOS TAP/utun test in its own process under a shell
# watchdog, so one wedged test cannot stall the others or hide the log.
#
# usage: run_macos_tap_tests.sh <async_tokio|async_io>
#
# A test that outlives PER_TEST_SECS gets a process listing and stack samples
# dumped, is killed with SIGKILL and is reported as failed; the remaining
# tests still run until TOTAL_SECS is spent. Exits non-zero if any test failed.
set -u

feature="${1:?feature required}"
per_test="${PER_TEST_SECS:-120}"
total="${TOTAL_SECS:-270}"
started=$(date +%s)

build_out=$(cargo test --test macos_tap_detach --features "$feature" --no-run 2>&1)
status=$?
echo "$build_out"
if [ "$status" -ne 0 ]; then
    echo "build failed"
    exit 1
fi
bin=$(echo "$build_out" | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
    echo "cannot locate the test binary"
    exit 1
fi
tests=$("$bin" --ignored --list 2>/dev/null | sed -n 's/: test$//p')
if [ -z "$tests" ]; then
    echo "no tests listed"
    exit 1
fi

# Runs a command for at most $1 seconds; never waits on a stuck process.
bounded() {
    local limit="$1"
    shift
    "$@" &
    local pid=$!
    local waited=0
    while [ "$waited" -lt "$limit" ]; do
        if ! is_running "$pid"; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    kill -9 "$pid" 2>/dev/null
    return 1
}

# True while the process exists and is not a zombie.
is_running() {
    local state
    state=$(ps -o state= -p "$1" 2>/dev/null | tr -d ' ')
    [ -n "$state" ] && [ "${state#Z}" = "$state" ]
}

dump_state() {
    echo "---- process table ----"
    ps -axo pid,ppid,state,wchan,command | grep -E "PID|macos_tap_detach|ifconfig|sudo" | grep -v grep
    for p in $(pgrep -f "$bin"); do
        echo "---- sample $p ----"
        bounded 20 sudo sample "$p" 3
    done
    if command -v spindump >/dev/null 2>&1; then
        echo "---- spindump ----"
        bounded 30 sudo spindump -notarget 2 -onlyRunnable 2>&1 | head -200
    fi
}

failed=0
for t in $tests; do
    now=$(date +%s)
    if [ $((now - started)) -ge "$total" ]; then
        echo "=== $t: SKIPPED, time budget of ${total}s spent"
        failed=$((failed + 1))
        continue
    fi
    echo "=== $t"
    log=$(mktemp)
    sudo "$bin" --ignored --exact "$t" --nocapture --test-threads=1 >"$log" 2>&1 &
    pid=$!
    waited=0
    while is_running "$pid"; do
        if [ "$waited" -ge "$per_test" ]; then
            echo "=== $t: HUNG after ${per_test}s"
            cat "$log"
            dump_state
            sudo pkill -9 -f "$bin"
            sudo kill -9 "$pid" 2>/dev/null
            # Interfaces the test left behind, best effort and bounded.
            for n in $(ifconfig -l | tr ' ' '\n' | grep '^feth'); do
                bounded 10 sudo ifconfig "$n" destroy
            done
            failed=$((failed + 1))
            continue 2
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait "$pid"
    rc=$?
    cat "$log"
    if [ "$rc" -ne 0 ]; then
        echo "=== $t: FAILED (exit $rc)"
        failed=$((failed + 1))
    else
        echo "=== $t: ok"
    fi
    rm -f "$log"
done

if [ "$failed" -ne 0 ]; then
    echo "$failed test(s) failed, hung or were skipped"
    exit 1
fi
