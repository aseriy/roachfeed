# RoachFeed

[![hex.pm](https://img.shields.io/hexpm/v/roachfeed.svg)](https://hex.pm/packages/roachfeed)
[![hex.pm](https://img.shields.io/hexpm/dt/roachfeed.svg)](https://hex.pm/packages/roachfeed)
[![hex.pm](https://img.shields.io/hexpm/l/roachfeed.svg)](https://hex.pm/packages/roachfeed)
[![github.com](https://img.shields.io/github/last-commit/karlseguin/roachfeed.svg)](https://github.com/karlseguin/roachfeed)

Consumes a CockroachDB Core ChangeFeed. Doesn't use Postgrex (except for
testing). Doesn't open a pool. It open a single (per instance) TCP connection
which is optimized for dealing with feed data.

## Usage

In your `mix.exs` file, add the project dependency:

```
{:roachfeed, "~> 0.0.7"}
```

Next create a module:

```elixir
defmodule MyModule do
  use RoachFeed

  defp setup(_opts) do
    state = nil
    connection_config = Application.fetch_env!(:app, :config)
    {state, connection_config}
  end

  # Called once the connection is estasblished.
  # `for` must be specified (it can be a list of table, or a single table)
  # `with` is an optional keyword list that matches the options that
  #        'experimental changefeed for ...' supports
  #        (Note: it's OK to pass `nil` to the `cursor` key)
  defp query(state) do
    changefeed_config = [
      for: ["table_1", "table_2"],
      with: [
        resolved: "10s",
        cursor: elem(state, 2)[:resolved]
      ]
    ]
    {state, changefeed_config}
  end

  # `msg` is not parsed. You probably want to Jason.decode!/1 it.
  defp handle_resolved(msg, state) do
    IO.inspect(msg)
    state
  end

  # `key` and `data` are not parsed. You may want to Jason.decode/1 them.
  # If `envelope: "key_only` is passed to the `with:` keyword list of
  # `query/1`, then `data` will be nil.
  defp handle_change(table, key, data, state) do
    IO.inspect({table, key, data})
    state
  end
end
```

Start `MyModule` (as a child of a supervisor most likely), passing it the
typically connection string value:

```elixir
{MyModule, [any_opts_you_want_passed_to_setup/1]}
```

## Single-table changefeed (CDC query)

An alternative config shape for `query/1`. It attaches the changefeed to
exactly one table and supports column projection and row filtering:

```elixir
change_feed = [
  table: "messages",         # required - exactly one table
  columns: ["id", "body"],   # optional - omitted/[] selects all columns
  where: "author = 'alex'",  # optional - omitted/"" means no WHERE clause
  resolved: "10s",           # optional - defaults to "10s"
  after: timestamp           # optional - omitted/nil does a full catch-up (initial scan)
]
```

Semantics:

- Mutually exclusive with `for`: a config contains exactly one of `table` or `for`.
- No `with` key in this shape; nothing from the caller maps to the SQL `WITH` clause.
- The envelope is fixed at `wrapped`; messages always arrive as `{"after": {...}}`.
- `resolved` defaults to `"10s"`, so every feed emits watermarks.
- `after` is the restart mechanism: pass the timestamp of the last received
  message and the feed resumes strictly past it (CockroachDB's `cursor` option).
  When omitted, the feed does an initial scan of the table, then streams live changes.
- `table`, `columns` and `where` are spliced into the SQL verbatim; the server
  validates them.

The generated statement:

```sql
CREATE CHANGEFEED WITH envelope = 'wrapped', resolved = '10s'[, cursor = <after>]
AS SELECT <columns|*> FROM <table>[ WHERE <predicate>]
```

## License

[ISC](LICENSE) Copyright (c) 2020, Karl Seguin
