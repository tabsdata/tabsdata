--
-- Copyright 2025 Tabs Data Inc.
--

create table test_table
(
    id         TEXT primary key,
    status     TEXT      not null,
    current    TEXT      not null,
    downstream TEXT      not null,
    defined_on TIMESTAMP not null
);


INSERT INTO test_table
SELECT 'AAA',
       'A',
       'ref_0',
       'ref_1',
       '2025-04-02T08:19:50.543+00:00'
;

INSERT INTO test_table
SELECT 'BBB',
       'A',
       'ref_1',
       'ref_2',
       '2025-04-02T08:19:50.543+00:00'
;

INSERT INTO test_table
SELECT 'CCC',
       'A',
       'ref_1',
       'ref_3',
       '2025-04-02T08:19:51.543+00:00'
;

INSERT INTO test_table
SELECT 'DDD',
       'A',
       'ref_1',
       'ref_4',
       '2025-04-02T08:19:52.543+00:00'
;

INSERT INTO test_table
SELECT 'EEE',
       'A',
       'ref_4',
       'ref_5',
       '2025-04-02T08:19:53.543+00:00'
;

create table test_table_reference
(
    id           TEXT primary key,
    reference_id TEXT      not null,
    status       TEXT      not null,
    defined_on   TIMESTAMP not null
);

INSERT INTO test_table_reference
SELECT 'MMM',
       'ref_0',
       'A',
       '2025-04-02T08:19:50.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'NNN',
       'ref_1',
       'A',
       '2025-04-02T08:19:50.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'OOO',
       'ref_2',
       'A',
       '2025-04-02T08:19:50.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'QQQ',
       'ref_3',
       'A',
       '2025-04-02T08:19:51.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'PPP',
       'ref_2',
       'D',
       '2025-04-02T08:19:52.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'RRR',
       'ref_4',
       'A',
       '2025-04-02T08:19:53.543+00:00'
;

INSERT INTO test_table_reference
SELECT 'SSS',
       'ref_5',
       'A',
       '2025-04-02T08:19:53.543+00:00'
;