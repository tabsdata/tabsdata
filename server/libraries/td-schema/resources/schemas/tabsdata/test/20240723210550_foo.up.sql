--
-- Copyright 2024 Tabs Data Inc.
--

-- Add up migration script here

create table foo
(
    id   TEXT primary key,
    name TEXT not null
);

create table foo_scoped
(
    id    TEXT not null,
    scope TEXT not null,
    name  TEXT not null
);