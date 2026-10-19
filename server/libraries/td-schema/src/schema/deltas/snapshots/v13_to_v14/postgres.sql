-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE FUNCTION td_mint_id(instant timestamptz) RETURNS text
LANGUAGE sql VOLATILE PARALLEL SAFE AS $$
  SELECT string_agg(substr('0123456789abcdefghijklmnopqrstuv', substring(bits FROM i * 5 + 1 FOR 5)::int + 1, 1), '' ORDER BY i)
  FROM (
    SELECT ('x' || encode(set_bit(set_bit(
      overlay(uuid_send(gen_random_uuid())
        PLACING substring(int8send(floor(extract(epoch FROM instant) * 1000)::bigint) FROM 3) FROM 1 FOR 6),
      52, 1), 53, 1), 'hex'))::bit(128) || B'00' AS bits
  ) AS uuid, generate_series(0, 25) AS i
$$

ALTER TABLE "metrics_facts" ADD COLUMN "id" text NULL COLLATE "C"

UPDATE "metrics_facts" SET "id" = td_mint_id(ts)

ALTER TABLE "metrics_facts" ADD PRIMARY KEY ("id")

DROP FUNCTION td_mint_id(timestamptz)

UPDATE tabsdata_system SET value = '14' WHERE name = 'db_version'
