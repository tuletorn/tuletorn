#!/usr/bin/env bash
# Compare io_uring scheduler topologies against the epoll/Tokio baseline.
#
# Core budget on a 4-core box. The proxy is the thing under test, so it gets a
# fixed pair of cores and the load generator and origin get one each. Letting
# all three float over all four cores is what made the earlier runs in results/
# meaningless: the generator and the proxy stole from each other, so the numbers
# measured contention rather than the proxy.
set -euo pipefail

BIN="${BIN:-/root/lb/target/release}"
OUT="${OUT:-/root/lb/bench-out/$(date -u +%Y%m%dT%H%M%S)}"
PROXY_CORES="${PROXY_CORES:-0,1}"
PROXY_WORKERS="${PROXY_WORKERS:-2}"
ORIGIN_CORE="${ORIGIN_CORE:-2}"
LOAD_CORE="${LOAD_CORE:-3}"
DURATION="${DURATION:-20s}"
WARMUP="${WARMUP:-5s}"
CONNS="${CONNS:-8 64 256 1000}"
PORT=8080
# Request target. The mock origin sizes its body from ?size=N, so this is how
# the sweep moves between header-dominated and byte-dominated workloads.
TARGET="${TARGET:-/}"

mkdir -p "$OUT"
ulimit -n 1048576 || true

# Bracketed patterns so the pattern cannot match the shell that is running it.
pkill -f '[l]b-proxy-' 2>/dev/null || true
pkill -f '[l]b-mock-upstream' 2>/dev/null || true
sleep 1

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }

wait_for_port() {
    for _ in $(seq 1 100); do
        if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
        sleep 0.1
    done
    echo "port $1 never opened" >&2; return 1
}

# utime+stime in clock ticks. Parsed after the ")" so a comm with spaces cannot
# shift the field offsets.
cpu_ticks() { awk -F') ' '{split($2,a," "); print a[12]+a[13]}' "/proc/$1/stat" 2>/dev/null || echo 0; }
peak_rss_kb() { awk '/VmHWM/{print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }

log "starting origin on core $ORIGIN_CORE"
taskset -c "$ORIGIN_CORE" "$BIN/lb-mock-upstream" --listen 127.0.0.1:9090 --workers "${ORIGIN_WORKERS:-1}" &
ORIGIN_PID=$!
wait_for_port 9090
trap 'kill $ORIGIN_PID 2>/dev/null || true' EXIT

# name -> command, kept as parallel arrays so the same binary can appear under
# several configurations.
NAMES=()
CMDS=()
add_case() { NAMES+=("$1"); shift; CMDS+=("$*"); }

add_case hyper-epoll-ws \
    "$BIN/lb-proxy-hyper --listen 0.0.0.0:$PORT --workers $PROXY_WORKERS --default-upstream 127.0.0.1:9090"

# Completion-native: no async runtime, registered buffers, multishot accept,
# one io_uring_enter per loop turn.
add_case native-uring \
    "$BIN/lb-proxy-native --listen 0.0.0.0:$PORT --workers $PROXY_WORKERS --default-upstream 127.0.0.1:9090"

# Hyper over an io_uring reactor, batched. The middle of the three designs.
add_case uring-tpc-batched \
    "$BIN/lb-proxy-uring --listen 0.0.0.0:$PORT --workers $PROXY_WORKERS --scheduler tpc --dispatch balanced --defer-submit true"
add_case uring-ws-batched \
    "$BIN/lb-proxy-uring --listen 0.0.0.0:$PORT --workers $PROXY_WORKERS --scheduler ws --rings per-worker --defer-submit true"

if [[ "${SQPOLL:-0}" == "1" ]]; then
    add_case uring-tpc-batched-sqpoll \
        "$BIN/lb-proxy-uring --listen 0.0.0.0:$PORT --workers $PROXY_WORKERS --scheduler tpc --dispatch balanced --sqpoll --sqpoll-pin true"
fi

TICKS_PER_SEC=$(getconf CLK_TCK)
echo "candidate,conns,rps,p50_ms,p99_ms,p999_ms,success,errors,proxy_cpu_pct,peak_rss_mb,origin_cpu_pct,load_cpu_pct,bottleneck" > "$OUT/summary.csv"

for i in "${!NAMES[@]}"; do
    NAME="${NAMES[$i]}"
    CMD="${CMDS[$i]}"

    for C in $CONNS; do
        log "$NAME @ c=$C"
        # shellcheck disable=SC2086
        taskset -c "$PROXY_CORES" $CMD > "$OUT/$NAME.log" 2>&1 &
        PID=$!
        if ! wait_for_port "$PORT"; then
            echo "$NAME,$C,START_FAILED,,,,,,," >> "$OUT/summary.csv"
            kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true
            continue
        fi

        # Correctness gate: a proxy that 404s every request looks fast.
        BODY=$(taskset -c "$LOAD_CORE" curl -fsS "http://127.0.0.1:$PORT/" || echo FAILED)
        if [[ "$BODY" != *mock-backend* ]]; then
            log "  !! $NAME did not proxy correctly: $BODY"
            echo "$NAME,$C,PROXY_BROKEN,,,,,,," >> "$OUT/summary.csv"
            kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true
            continue
        fi

        taskset -c "$LOAD_CORE" oha -c "$C" -z "$WARMUP" --no-tui --output-format json "http://127.0.0.1:$PORT$TARGET" > /dev/null 2>&1 || true

        # Sample the proxy, the origin and the generator together. Without all
        # three there is no way to tell a fast proxy from a starved generator.
        T0=$(cpu_ticks $PID); O0=$(cpu_ticks $ORIGIN_PID)
        /usr/bin/time -f "%P" -o "$OUT/$NAME-c$C.load_cpu" \
            taskset -c "$LOAD_CORE" oha -c "$C" -z "$DURATION" --no-tui --output-format json \
            "http://127.0.0.1:$PORT$TARGET" > "$OUT/$NAME-c$C.json" 2>"$OUT/$NAME-c$C.err" || true
        T1=$(cpu_ticks $PID); O1=$(cpu_ticks $ORIGIN_PID)
        RSS=$(peak_rss_kb $PID)

        SECS=${DURATION%s}
        pct() { awk -v a="$1" -v b="$2" -v t="$TICKS_PER_SEC" -v s="$SECS" \
                'BEGIN{ if (s>0) printf "%.1f", (b-a)/t/s*100; else print 0 }'; }
        CPU=$(pct "$T0" "$T1")
        OCPU=$(pct "$O0" "$O1")
        LCPU=$(tr -d '%' < "$OUT/$NAME-c$C.load_cpu" 2>/dev/null | tail -1)
        LCPU=${LCPU:-0}

        NLOAD=$(awk -F, '{print NF}' <<< "$LOAD_CORE")
        NORIGIN=$(awk -F, '{print NF}' <<< "$ORIGIN_CORE")
        python3 - "$OUT/$NAME-c$C.json" "$NAME" "$C" "$CPU" "$RSS" "$OCPU" "$LCPU" "$PROXY_WORKERS" "$NLOAD" "$NORIGIN" >> "$OUT/summary.csv" <<'PY'
import json, sys
path, name, conns, cpu, rss, ocpu, lcpu, workers, nload, norigin = sys.argv[1:11]
try:
    d = json.load(open(path))
except Exception:
    print(f"{name},{conns},PARSE_FAILED,,,,,,,,,,"); raise SystemExit
s = d["summary"]
pct = d.get("latencyPercentiles", {})
codes = d.get("statusCodeDistribution", {})
ok = sum(v for k, v in codes.items() if k.startswith("2"))
bad = sum(v for k, v in codes.items() if not k.startswith("2"))
bad += sum(d.get("errorDistribution", {}).values())
ms = lambda x: round(x * 1000, 3) if isinstance(x, (int, float)) else ""

# Which of the three processes ran out of CPU first. A run where the generator
# or the origin is pegged says nothing about the proxy, so label it rather than
# quietly reporting it as a proxy result.
f = lambda v: float(v) if v not in ("", None) else 0.0
ceiling = float(workers) * 100
load_ceiling = float(nload) * 100
origin_ceiling = float(norigin) * 100
bottleneck = "proxy"
if f(lcpu) >= load_ceiling * 0.95 and f(cpu) < ceiling * 0.9:
    bottleneck = "LOAD-GEN"
elif f(ocpu) >= origin_ceiling * 0.95 and f(cpu) < ceiling * 0.9:
    bottleneck = "ORIGIN"
elif f(cpu) < ceiling * 0.75:
    bottleneck = "unsaturated"

print(",".join(str(x) for x in [
    name, conns, round(s["requestsPerSec"], 1),
    ms(pct.get("p50")), ms(pct.get("p99")), ms(pct.get("p99.9")),
    ok, bad, cpu, round(int(rss) / 1024, 1), ocpu, lcpu, bottleneck,
]))
PY

        kill $PID 2>/dev/null || true
        wait $PID 2>/dev/null || true
        sleep 1
    done
done

log "results in $OUT"
column -s, -t "$OUT/summary.csv"
