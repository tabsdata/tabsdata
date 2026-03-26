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
       'mario',
       '1234'
;

INSERT INTO test_table
SELECT '00000000000000000000000008',
       'luigi',
       '6789'
;
