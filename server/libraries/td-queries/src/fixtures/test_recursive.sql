--
-- Copyright 2026 Tabs Data Inc.
--

CREATE TABLE test_basic_reference
(
    id           TEXT PRIMARY KEY,
    reference_id TEXT NOT NULL
);

CREATE TABLE test_basic_table
(
    id         TEXT PRIMARY KEY,
    current    TEXT NOT NULL,
    downstream TEXT NOT NULL
);

-- Chain: ref_0 -> ref_1 -> ref_2
INSERT INTO test_basic_reference SELECT 'R0', 'ref_0';
INSERT INTO test_basic_reference SELECT 'R1', 'ref_1';
INSERT INTO test_basic_reference SELECT 'R2', 'ref_2';

INSERT INTO test_basic_table SELECT 'N0', 'ref_0', 'ref_1';
INSERT INTO test_basic_table SELECT 'N1', 'ref_1', 'ref_2';