defmodule SelectoDBPostgreSQL.UnusedJoinIntegrationTest do
  # A domain may declare joins a read never uses, to tables that do not exist
  # in every environment (a session's temporary tables). Only the relations the
  # compiled read names are resolved: an unused join answers, a used one
  # resolves, and refuses when its table is missing.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    {:ok, c} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(c)

    for sql <- [
          "CREATE TEMP TABLE orders (id integer PRIMARY KEY, tenant_id integer NOT NULL)",
          "CREATE TEMP TABLE order_lines (id integer PRIMARY KEY, tenant_id integer NOT NULL, order_id integer NOT NULL)",
          "INSERT INTO orders VALUES (1, 7), (2, 7)",
          "INSERT INTO order_lines VALUES (11, 7, 1)"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)
    %{conn: c}
  end

  defp columns(fields), do: Map.new(fields, &{&1, %{type: :integer}})

  defp schema(table, fields, associations \\ %{}),
    do: %{
      source_table: table,
      primary_key: "id",
      fields: fields,
      redact_fields: [],
      columns: columns(fields),
      associations: associations
    }

  # orders -> lines (order_lines) -> allocations (line_allocations, which no
  # test creates), and a declared non-association join to `line_notes`, which
  # does not exist either.
  defp domain do
    %{
      name: "orders",
      source:
        schema("orders", ["id", "tenant_id"], %{
          "lines" => %{
            field: "lines",
            queryable: "order_lines",
            owner_key: "id",
            related_key: "order_id",
            cardinality: :many
          }
        })
        |> Map.put(:source_schema, "pg_temp"),
      schemas: %{
        "order_lines" =>
          schema("order_lines", ["id", "tenant_id", "order_id"], %{
            "allocations" => %{
              field: "allocations",
              queryable: "line_allocations",
              owner_key: "id",
              related_key: "line_id",
              cardinality: :many
            }
          }),
        "line_allocations" => schema("line_allocations", ["id", "line_id"])
      },
      joins: %{
        "lines" => %{type: :left, joins: %{"allocations" => %{type: :left}}},
        notes: %{
          non_assoc: true,
          source: "line_notes",
          type: :left,
          owner_key: "id",
          related_key: "order_id",
          fields: %{
            "notes.order_id" => %{
              colid: "notes.order_id",
              field: "order_id",
              requires_join: :notes,
              type: :integer
            }
          }
        }
      }
    }
  end

  defp q(ctx), do: Selecto.configure(domain(), ctx.conn, adapter: Adapter, validate: false)

  defp outcome(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _, _}} -> Enum.sort(rows)
      {:error, %Selecto.Error{details: details}} -> {details[:reason], details[:relations]}
    end
  end

  test "unused declared joins to missing temporary tables answer", ctx do
    assert outcome(Selecto.select(q(ctx), ["id"])) == [[1], [2]]
    assert outcome(Selecto.select(q(ctx), ["id", "lines.id"])) == [[1, 11], [2, nil]]

    filtered = q(ctx) |> Selecto.select(["id"]) |> Selecto.filter({"lines.id", 11})
    assert outcome(filtered) == [[1]]
  end

  test "a used join still reads its table: a missing one is the server's error", ctx do
    for query <- [
          Selecto.select(q(ctx), ["id", "allocations.id"]),
          q(ctx) |> Selecto.select(["id"]) |> Selecto.filter({"allocations.line_id", 1})
        ] do
      assert {:error, %Selecto.Error{details: %{sqlstate: "42P01"}}} = Selecto.execute(query)
    end

    {:ok, _} =
      Adapter.execute(
        ctx.conn,
        "CREATE TEMP TABLE line_allocations (id integer PRIMARY KEY, line_id integer NOT NULL)",
        [],
        []
      )

    {:ok, _} = Adapter.execute(ctx.conn, "INSERT INTO line_allocations VALUES (5, 11)", [], [])
    assert outcome(Selecto.select(q(ctx), ["id", "allocations.id"])) == [[1, 5], [2, nil]]
  end
end
