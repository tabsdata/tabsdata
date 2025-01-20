#
# Copyright 2025 Tabs Data Inc.
#

root_folder=$(pwd)
mkdir -p ./target/audit
rm -f ./target/audit/licenses_py.txt

licensecheck -u requirements:requirements.txt --format json | python3 "${root_folder}/devutils/automation/tasks/makers/td-scripts/report_licenses_py.py" >> ./target/audit/licenses_py.txt
cat ./target/audit/licenses_py.txt