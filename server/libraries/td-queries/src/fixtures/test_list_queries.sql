--
-- Copyright 2025 Tabs Data Inc.
--

create table test_table
(
    id          TEXT primary key,
    name        TEXT    not null,
    modified_on INTEGER not null
);


INSERT INTO test_table
SELECT '00000000000000000000000004',
       'B',
       '1'
;

INSERT INTO test_table
SELECT '00000000000000000000000008',
       'A',
       '2'
;

INSERT INTO test_table
SELECT '0000000000000000000000000C',
       'A',
       '3'
;

INSERT INTO test_table
SELECT '0000000000000000000000000G',
       'C',
       '4'
;
