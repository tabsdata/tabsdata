#
# Copyright 2025 Tabs Data Inc.
#

root_folder=$(pwd)
mkdir -p ./target/audit
rm -f ./target/audit/licenses_rs.txt

cargo license --manifest-path ./Cargo.toml --json | jq -r '.[] | [.name, .version, .license] | @csv' | python3 "${root_folder}/devutils/automation/tasks/makers/td-scripts/report_licenses_rs.py" >> ./target/audit/licenses_rs.txt
cat ./target/audit/licenses_rs.txt