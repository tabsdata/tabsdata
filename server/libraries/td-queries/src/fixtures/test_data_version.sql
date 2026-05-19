--
-- Copyright 2026 Tabs Data Inc.
--

CREATE TABLE test_dv_table
(
    id           TEXT PRIMARY KEY,
    partition_id TEXT      NOT NULL,
    status       TEXT      NOT NULL,
    defined_on   TIMESTAMP NOT NULL
);

-- Partition p0: 3 active versions
-- After ranking (ORDER BY defined_on DESC):
--   DV_3 (t3) -> rn=1 (HEAD)
--   DV_2 (t2) -> rn=2
--   DV_1 (t1) -> rn=3 (INITIAL), total_count=3
INSERT INTO test_dv_table
SELECT 'LDV20260101.0000000001', 'p0', 'A', '2026-01-01T00:00:01.000+00:00';
INSERT INTO test_dv_table
SELECT 'LDV20260101.0000000002', 'p0', 'A', '2026-01-01T00:00:02.000+00:00';
INSERT INTO test_dv_table
SELECT 'LDV20260101.0000000003', 'p0', 'A', '2026-01-01T00:00:03.000+00:00';

-- Partition p1: 1 active + 1 deleted
-- After ranking:
--   DV_5 (t2, D) -> rn=1 (HEAD, deleted)
--   DV_4 (t1, A) -> rn=2 (INITIAL), total_count=2
INSERT INTO test_dv_table
SELECT 'LDV20260101.0000000004', 'p1', 'A', '2026-01-01T00:00:01.000+00:00';
INSERT INTO test_dv_table
SELECT 'LDV20260101.0000000005', 'p1', 'D', '2026-01-01T00:00:02.000+00:00';