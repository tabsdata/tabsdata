#!/bin/sh

#
# Copyright 2026 Tabsdata Inc.
#
# Shell emulator of td_ac (Dummy Application Worker): runs forever,
# ticks every 30 s. Used by kprofile development manifests in the
# kubernetes provider where launching the rust binary inside a tiny image
# is more setup than just running this script under busybox.

set -eu

NAME="Dummy Application Worker"
PERIOD=30

echo "[$NAME] starting"
tick=0
while true; do
    sleep "$PERIOD"
    tick=$((tick + 1))
    echo "[$NAME] tick $tick"
done
