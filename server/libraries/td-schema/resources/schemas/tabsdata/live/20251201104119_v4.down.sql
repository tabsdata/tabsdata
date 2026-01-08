--
--  Copyright 2025 Tabs Data Inc.
--

UPDATE tabsdata_system
SET value = '3'
WHERE name = 'db_version';

DROP VIEW function_runs__with_names;
CREATE VIEW function_runs__with_names AS
SELECT f.*,
       fv.name                                         AS name,
       c.name                                          AS collection,
       e.name                                          AS execution,
       IFNULL(u.name, '[' || f.triggered_by_id || ']') as triggered_by
FROM function_runs f
         LEFT JOIN collections c ON f.collection_id = c.id
         LEFT JOIN functions fv ON f.function_version_id = fv.id
         LEFT JOIN executions e ON f.execution_id = e.id
         LEFT JOIN users u ON f.triggered_by_id = u.id;
