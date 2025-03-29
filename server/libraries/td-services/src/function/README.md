<!--
Copyright 2025 Tabs Data Inc.
-->

# Functions Register, Update, Delete and Table Delete Logic

## Register Function

* Check function name does not exist in collection.
* Extract output tables, table dependencies and triggers.
* Insert into function_versions(sql) status=Active.
* Insert into functions(sql) function info.
* Insert into table_versions(sql) current function tables status=Active.
  Reuse table_id for tables that existed (had status=Frozen)
* Insert into tables(sql) function tables info and update already existing tables (frozen tables).
* Insert into dependency_versions(sql) current function table dependencies status=Active.
* Insert into trigger_versions(sql) current function trigger status=Active.
* Insert into dependencies(sql) function dependencies info.
* Insert into triggers(sql) function trigger info.

## Update Function

* Check function exists in collection.
* If function has a new name, check new name does not exist in collection.
    * Check function output tables do not exist in collection. Or if they do,
      they have status=Frozen, or they already belonged to the function.
* Insert into table_versions(sql) dropped function tables status=Frozen.
* Insert into dependency_versions(sql) dropped function table dependencies status=Deleted.
* Insert into trigger_versions(sql) dropped function trigger status=Deleted.

* Insert into function_versions(sql) status=Active.
* Insert into table_versions(sql) current function tables status=Active.
  Reuse table_id for tables that existed (had status=Frozen)
* Insert into dependency_versions(sql) current function table dependencies status=Active.
* Insert into trigger_versions(sql) current function trigger status=Active.

* Update functions table.
* Insert into tables(sql) function tables info, except for already existing from previous
  version of the function or already existing (status=Frozen).
* Update tables(sql) with new table_version_id, function_version_id and status=Active for
  tables that were status=Frozen.
* Delete previous/Insert dependencies(sql) function dependencies info.
* Delete previous/Insert triggers(sql) function trigger info.

## Delete Function

* Check function exists in collection.
* Insert into function_versions(sql) status=Deleted.
* Insert into table_versions(sql) all last tables setting status=Frozen.
* Insert into dependency_versions(sql) all last dependencies setting status=Deleted.
* Insert into trigger_versions(sql) all last trigger setting status=Deleted.
* Delete from functions(sql).
* Update tables(sql) with all function tables as frozen.
* Delete dependencies(sql) all function dependencies.
* Delete trigger(sql) all function dependencies.

NOTE: Delete Function does not delete data.

## Delete Table

* Check table exists in collection and it has status=frozen.
* Insert into table_versions(sql) an entry with status=Deleted.
* Insert into function_versions(sql) entries with status=Frozen, for all functions with
  status=Active that have the table as dependency.
* Update functions(sql) with status=Frozen, for all functions with status=Active that have
  the table as output.
* Delete table from tables(sql).

NOTE: Delete table does not delete data.

## Delete Collection

* Do 'Delete Function' logic for all functions in collection.
* Do 'Delete Table' logic for all tables in collection.

NOTE: Delete Collection does not delete data.
