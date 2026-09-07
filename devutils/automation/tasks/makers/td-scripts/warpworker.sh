#
#  Copyright 2026 Tabsdata Inc.
#

# Runs as a per-group symlink under 'target/td-test-rs': the link name is the
# group label, and every parameter comes from '.pool/<label>.conf' next to it.

set -u

job_poll=0.3
gate_poll=0.5
label="$(basename "$0")"
pool="$(cd "$(dirname "$0")" && pwd)/.pool"
job="$pool/$label.job"
busy="$pool/$label.busy"
. "$pool/$label.conf"
echo $$ > "$pool/$label.pid"
trap 'rm -f "$pool/$label.pid" "$busy"' EXIT

while [ -d "$pool" ] && [ ! -f "$pool/retire" ]; do
    if [ -f "$job" ]; then
        run="$(cat "$job")"
        rm -f "$job"
        touch "$busy"
        (
            set -a
            . "$run/environment"
            set +a
            if [ -n "$waits" ]; then
                echo "⏳ waiting for: $pending"
                driver="$(cat "$run/driver")"
                for marker in $waits; do
                    while [ ! -f "$run/$marker" ]; do
                        kill -0 "$driver" 2>/dev/null || exit 1
                        sleep "$gate_poll"
                    done
                done
            fi
            started=$SECONDS
            ( "${make_command[@]}" "$task"; printf %s $? > "$run/$task.code" ) 2>&1 \
                | tee "$run/$task.log"
            printf %s $((SECONDS - started)) > "$run/$task.secs"
        )
        rm -f "$busy"
    fi
    sleep "$job_poll"
done
echo "🚪 retired: this pane can be closed"
