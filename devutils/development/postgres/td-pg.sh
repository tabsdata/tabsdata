#!/usr/bin/env bash
#
# Copyright 2026 Tabsdata Inc.
#

#
# Dev helper: spin up a local PostgreSQL container for tabsdata.
#
# Usage:
#   eval "$(devutils/development/postgres/td-pg.sh start)"   # boots PG, exports TD_DATABASE_URL
#   devutils/development/postgres/td-pg.sh stop              # stops + removes the container
#   devutils/development/postgres/td-pg.sh url               # prints just the URL
#   devutils/development/postgres/td-pg.sh logs              # tails container logs
#   devutils/development/postgres/td-pg.sh reset             # drops + recreates the database
#
# Defaults (override via env):
#   TD_PG_CONTAINER=td-pg
#   TD_PG_PORT=5432
#   TD_PG_USER=td
#   TD_PG_PASSWORD=td
#   TD_PG_DATABASE=tabsdata
#   TD_PG_IMAGE=postgres:16
#
# `start` writes the resolved URL to stdout as an `export TD_DATABASE_URL=…`
# line. Wrap the call in `eval "$(... start)"` (or `source` the file) so the
# variable lands in your shell — then `tdserver start` picks it up via the
# apiserver's clap `env = "TD_DATABASE_URL"` hook.

set -euo pipefail

: "${TD_PG_CONTAINER:=td-pg}"
: "${TD_PG_PORT:=5432}"
: "${TD_PG_USER:=td}"
: "${TD_PG_PASSWORD:=td}"
: "${TD_PG_DATABASE:=tabsdata}"
: "${TD_PG_IMAGE:=postgres:16}"

pg_url() {
    printf 'postgres://%s:%s@localhost:%s/%s' \
        "$TD_PG_USER" "$TD_PG_PASSWORD" "$TD_PG_PORT" "$TD_PG_DATABASE"
}

container_running() {
    docker ps --format '{{.Names}}' | grep -qx "$TD_PG_CONTAINER"
}

container_exists() {
    docker ps -a --format '{{.Names}}' | grep -qx "$TD_PG_CONTAINER"
}

wait_ready() {
    # pg_isready inside the container — fastest, no host-side psql dependency.
    local tries=0
    until docker exec "$TD_PG_CONTAINER" pg_isready -U "$TD_PG_USER" -d "$TD_PG_DATABASE" >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -ge 30 ]; then
            echo "td-pg: container never became ready" >&2
            docker logs "$TD_PG_CONTAINER" >&2 || true
            exit 1
        fi
        sleep 0.5
    done
}

cmd_start() {
    if container_running; then
        echo "td-pg: '$TD_PG_CONTAINER' already running on :$TD_PG_PORT" >&2
    elif container_exists; then
        echo "td-pg: starting existing '$TD_PG_CONTAINER'" >&2
        docker start "$TD_PG_CONTAINER" >/dev/null
        wait_ready
    else
        echo "td-pg: creating '$TD_PG_CONTAINER' ($TD_PG_IMAGE) on :$TD_PG_PORT" >&2
        docker run -d \
            --name "$TD_PG_CONTAINER" \
            -p "${TD_PG_PORT}:5432" \
            -e POSTGRES_USER="$TD_PG_USER" \
            -e POSTGRES_PASSWORD="$TD_PG_PASSWORD" \
            -e POSTGRES_DB="$TD_PG_DATABASE" \
            "$TD_PG_IMAGE" >/dev/null
        wait_ready
    fi

    # Stdout is intended for `eval`. Anything diagnostic goes to stderr.
    printf 'export TD_DATABASE_URL=%q\n' "$(pg_url)"
    echo "# td-pg ready at $(pg_url)" >&2
    echo "# eval the line above, then run: tdserver start" >&2
}

cmd_stop() {
    if container_exists; then
        echo "td-pg: stopping & removing '$TD_PG_CONTAINER'" >&2
        docker rm -f "$TD_PG_CONTAINER" >/dev/null
    else
        echo "td-pg: '$TD_PG_CONTAINER' not found" >&2
    fi
}

cmd_url() {
    pg_url
    echo
}

cmd_logs() {
    docker logs -f "$TD_PG_CONTAINER"
}

cmd_reset() {
    if ! container_running; then
        echo "td-pg: '$TD_PG_CONTAINER' is not running" >&2
        exit 1
    fi
    echo "td-pg: dropping & recreating database '$TD_PG_DATABASE'" >&2
    # Issue DROP and CREATE as separate sessions; `psql -c "<a>; <b>"` wraps
    # the pair in an implicit transaction and `DROP DATABASE` refuses that.
    docker exec -e PGPASSWORD="$TD_PG_PASSWORD" "$TD_PG_CONTAINER" \
        psql -U "$TD_PG_USER" -d postgres -c \
        "DROP DATABASE IF EXISTS \"$TD_PG_DATABASE\" WITH (FORCE)" >/dev/null
    docker exec -e PGPASSWORD="$TD_PG_PASSWORD" "$TD_PG_CONTAINER" \
        psql -U "$TD_PG_USER" -d postgres -c \
        "CREATE DATABASE \"$TD_PG_DATABASE\"" >/dev/null
    echo "td-pg: database reset" >&2
}

case "${1:-start}" in
    start) cmd_start ;;
    stop)  cmd_stop  ;;
    url)   cmd_url   ;;
    logs)  cmd_logs  ;;
    reset) cmd_reset ;;
    *)
        echo "usage: $0 {start|stop|url|logs|reset}" >&2
        exit 2
        ;;
esac
