# Connections come from the collection

There is no by-name `connectionRef` resolution any more. A flow
function never declares or names a connection: the connection is
registered on the function's *collection*, and the runtime resolves
it and hands the concrete `<Name>Conn` instance to the connector's
plugin (`read_in(ctx, conn, src)` / `write_out(ctx, conn, dest,
tables)` / `run(ctx, conn, stream_src)`) at run time.

Consequences:

- `Src` / `StreamSrc` / `Dest` descriptors carry no `conn` field, so
  there is nothing on a serialized flow to resolve to a connection.
- The `NamedConn` wrapper and the bare-string→`NamedConn` promotion
  are gone; there is no JSON traversal that swaps a `connectionRef`
  entry for a resolved connection.
- A source/destination is bound to its connection *type* in its
  `SrcDef(conn=<Name>Conn, ...)` / `DestDef(conn=<Name>Conn, ...)`
  binding; the runtime uses that binding to know which collection
  connection to inject.

## Secret hydration

`Secret` values still resolve at run time: a `Secret` serializes to
the dehydrated marker `":secret:"` and is hydrated with its actual
value by the runtime before the connector uses it. This is value
hydration, not connection resolution — there is no separate
`connectionRef` pass to run first.
