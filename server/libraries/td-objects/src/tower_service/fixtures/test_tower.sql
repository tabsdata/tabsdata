--
-- Copyright 2025 Tabs Data Inc.
--

create table foo
(
    id   TEXT primary key,
    name TEXT not null
);


INSERT INTO foo
SELECT 'its a me',
       'mario'
;

INSERT INTO foo
SELECT 'its a me but in green',
       'luigi'
;