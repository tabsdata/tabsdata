--
-- Copyright 2025 Tabs Data Inc.
--

create table test_table
(
    id           TEXT primary key,
    partition_id TEXT      not null,
    status       TEXT      not null,
    defined_on   TIMESTAMP not null
);


INSERT INTO test_table
SELECT '00000000000000000000000004',
       '0',
       'A',
       '2025-04-02T08:19:53.543+00:00'
;

INSERT INTO test_table
SELECT '00000000000000000000000008',
       '1',
       'A',
       '2025-04-02T08:19:53.543+00:00'
;

INSERT INTO test_table
SELECT '0000000000000000000000000C',
       '1',
       'D',
       '2025-04-02T08:19:54.543+00:00'
;
