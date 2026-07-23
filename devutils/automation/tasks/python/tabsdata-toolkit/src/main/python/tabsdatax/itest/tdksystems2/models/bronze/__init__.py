#
# Copyright 2026 Tabsdata Inc.
#

# bronze model — a medallion-bronze arrival-pattern simulator: five tables, one
# per delivery pattern (full snapshot, insert/update soft-delete, insert/update
# no-delete, @td.cdc.* CDC stream, append-only events). Reference-only: no delta
# recipe, so create-data works but modify-data has no entities to change.
