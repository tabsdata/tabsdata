#!/usr/bin/env bash
#
# Dev helper: spin up a local MySQL container for tabsdata.
#
# Usage:
#   eval "$(devutils/development/mysql/td-my.sh start)"   # boots MySQL, exports TD_DATABASE_URL
#   devutils/development/mysql/td-my.sh stop              # stops + removes the container
#   devutils/development/mysql/td-my.sh url               # prints just the URL
#   devutils/development/mysql/td-my.sh logs              # tails container logs
#   devutils/development/mysql/td-my.sh reset             # drops + recreates the database
#
# Defaults (override via env):
#   TD_MY_CONTAINER=td-my
#   TD_MY_PORT=3306
#   TD_MY_USER=td
#   TD_MY_PASSWORD=td
#   TD_MY_ROOT_PASSWORD=root
#   TD_MY_DATABASE=tabsdata
#   TD_MY_IMAGE=mysql:8
#
# `start` writes the resolved URL to stdout as an `export TD_DATABASE_URL=…`
# line. Wrap the call in `eval "$(... start)"` (or `source` the file) so the
# variable lands in your shell — then `tdserver start` picks it up via the
# apiserver's clap `env = "TD_DATABASE_URL"` hook.

set -euo pipefail

: "${TD_MY_CONTAINER:=td-my}"
: "${TD_MY_PORT:=3306}"
: "${TD_MY_USER:=td}"
: "${TD_MY_PASSWORD:=td}"
: "${TD_MY_ROOT_PASSWORD:=root}"
: "${TD_MY_DATABASE:=tabsdata}"
: "${TD_MY_IMAGE:=mysql:8}"

my_url() {
    printf 'mysql://%s:%s@localhost:%s/%s' \
        "$TD_MY_USER" "$TD_MY_PASSWORD" "$TD_MY_PORT" "$TD_MY_DATABASE"
}

container_running() {
    docker ps --format '{{.Names}}' | grep -qx "$TD_MY_CONTAINER"
}

container_exists() {
    docker ps -a --format '{{.Names}}' | grep -qx "$TD_MY_CONTAINER"
}

wait_ready() {
    # mysqladmin ping inside the container — fastest, no host-side mysql dependency.
    # MySQL boots in two phases: the init process restarts the server after
    # creating the database, so we also confirm the target DB is reachable
    # as the app user before declaring readiness.
    local tries=0
    until docker exec "$TD_MY_CONTAINER" \
            mysqladmin ping -h 127.0.0.1 -u"$TD_MY_USER" -p"$TD_MY_PASSWORD" --silent >/dev/null 2>&1 \
        && docker exec "$TD_MY_CONTAINER" \
            mysql -h 127.0.0.1 -u"$TD_MY_USER" -p"$TD_MY_PASSWORD" -D "$TD_MY_DATABASE" -e 'SELECT 1' >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -ge 60 ]; then
            echo "td-my: container never became ready" >&2
            docker logs "$TD_MY_CONTAINER" >&2 || true
            exit 1
        fi
        sleep 0.5
    done
}

cmd_start() {
    if container_running; then
        echo "td-my: '$TD_MY_CONTAINER' already running on :$TD_MY_PORT" >&2
    elif container_exists; then
        echo "td-my: starting existing '$TD_MY_CONTAINER'" >&2
        docker start "$TD_MY_CONTAINER" >/dev/null
        wait_ready
    else
        echo "td-my: creating '$TD_MY_CONTAINER' ($TD_MY_IMAGE) on :$TD_MY_PORT" >&2
        docker run -d \
            --name "$TD_MY_CONTAINER" \
            -p "${TD_MY_PORT}:3306" \
            -e MYSQL_ROOT_PASSWORD="$TD_MY_ROOT_PASSWORD" \
            -e MYSQL_USER="$TD_MY_USER" \
            -e MYSQL_PASSWORD="$TD_MY_PASSWORD" \
            -e MYSQL_DATABASE="$TD_MY_DATABASE" \
            "$TD_MY_IMAGE" >/dev/null
        wait_ready
    fi

    # Stdout is intended for `eval`. Anything diagnostic goes to stderr.
    printf 'export TD_DATABASE_URL=%q\n' "$(my_url)"
    echo "# td-my ready at $(my_url)" >&2
    echo "# eval the line above, then run: tdserver start" >&2
}

cmd_stop() {
    if container_exists; then
        echo "td-my: stopping & removing '$TD_MY_CONTAINER'" >&2
        docker rm -f "$TD_MY_CONTAINER" >/dev/null
    else
        echo "td-my: '$TD_MY_CONTAINER' not found" >&2
    fi
}

cmd_url() {
    my_url
    echo
}

cmd_logs() {
    docker logs -f "$TD_MY_CONTAINER"
}

cmd_reset() {
    if ! container_running; then
        echo "td-my: '$TD_MY_CONTAINER' is not running" >&2
        exit 1
    fi
    echo "td-my: dropping & recreating database '$TD_MY_DATABASE'" >&2
    # Use root to drop/create; then re-grant the app user — DROP DATABASE
    # revokes any database-scoped grants tied to it.
    docker exec "$TD_MY_CONTAINER" \
        mysql -h 127.0.0.1 -uroot -p"$TD_MY_ROOT_PASSWORD" -e \
        "DROP DATABASE IF EXISTS \`$TD_MY_DATABASE\`;
         CREATE DATABASE \`$TD_MY_DATABASE\`;
         GRANT ALL PRIVILEGES ON \`$TD_MY_DATABASE\`.* TO '$TD_MY_USER'@'%';
         FLUSH PRIVILEGES;" >/dev/null
    echo "td-my: database reset" >&2
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
