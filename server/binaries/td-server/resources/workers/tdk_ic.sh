#!/bin/sh

#
# Copyright 2026 Tabs Data Inc.
#
# Shell emulator of td_ic (Dummy Initializer Worker): 5 ticks at 1 s
# intervals, then exits. Used by kprofile development manifests in the
# kubernetes provider where launching the rust binary inside a tiny image
# is more setup than just running this script under busybox.

set -eu

NAME="Dummy Initializer Worker"
TICKS=5

echo "[$NAME] starting"
tick=1
while [ "$tick" -le "$TICKS" ]; do
    sleep 1
    echo "[$NAME] tick $tick/$TICKS"
    tick=$((tick + 1))
done
echo "[$NAME] exiting"
