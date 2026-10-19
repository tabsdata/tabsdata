//
// Copyright 2026 Tabsdata Inc.
//

//! `td_mint_id(instant)`: the application's `Id` (a UUIDv7 as lowercase
//! unpadded base32hex) minted in SQL at `instant`'s millisecond, for a delta
//! creating rows. Ids minted within one millisecond are unordered. A delta
//! creates it and drops it before it ends: the schema keeps no functions.

pub const MINT_ID: &str = "CREATE FUNCTION td_mint_id(instant timestamptz) RETURNS text\n\
     LANGUAGE sql VOLATILE PARALLEL SAFE AS $$\n  \
       SELECT string_agg(substr('0123456789abcdefghijklmnopqrstuv', substring(bits FROM i * 5 + 1 FOR 5)::int + 1, 1), '' ORDER BY i)\n  \
       FROM (\n    \
         SELECT ('x' || encode(set_bit(set_bit(\n      \
           overlay(uuid_send(gen_random_uuid())\n        \
             PLACING substring(int8send(floor(extract(epoch FROM instant) * 1000)::bigint) FROM 3) FROM 1 FOR 6),\n      \
           52, 1), 53, 1), 'hex'))::bit(128) || B'00' AS bits\n  \
       ) AS uuid, generate_series(0, 25) AS i\n\
     $$";

pub const DROP_MINT_ID: &str = "DROP FUNCTION td_mint_id(timestamptz)";
