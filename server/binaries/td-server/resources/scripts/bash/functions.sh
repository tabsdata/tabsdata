#
# Copyright 2024 Tabs Data Inc.
#

# function to check execution error
check_error() {
    local status=$1
    if [ $status -ne 0 ]; then
        echo "Command failed with status $status"
        exit $status
    fi
}

# function to sleep some time
td_sleep() {
    local duration=$1
    sleep "$duration"
}