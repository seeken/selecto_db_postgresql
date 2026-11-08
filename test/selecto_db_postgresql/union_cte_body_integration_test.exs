defmodule SelectoDBPostgreSQL.UnionCteBodyIntegrationTest do
  # A CTE whose body is a set operation, one whose member defines its own CTE,
  # and a top-level union member carrying a WITH.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    p = "un_p_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TABLE public.#{p} (id int PRIMARY KEY, name text)",
          "INSERT INTO public.#{p} VALUES (1, 'a'), (2, 'b')"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(opts)
      Adapter.execute(d, "DROP TABLE IF EXISTS public.#{p}", [], [])
      GenServer.stop(d)
    end)

    %{connection: c, p: p}
  end

  defp q(ctx) do
    %{
      name: ctx.p,
      schemas: %{},
      joins: %{},
      source: %{
        source_table: ctx.p,
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      }
    }
    |> Selecto.configure(ctx.connection, adapter: Adapter, validate: false)
  end

  defp union_body(ctx, member_cte?) do
    fn ->
      left = q(ctx) |> Selecto.filter({"id", 1}) |> Selecto.select(["id", "name"])

      right =
        if member_cte? do
          own =
            Selecto.Advanced.CTE.create_cte(
              "pc",
              fn -> q(ctx) |> Selecto.filter({"id", 2}) |> Selecto.select(["id", "name"]) end,
              columns: ["pid", "pname"]
            )

          q(ctx)
          |> Selecto.with_ctes([own])
          |> Selecto.join(:pcj,
            source: "pc",
            source_kind: :cte,
            type: :inner,
            owner_key: :id,
            related_key: :pid,
            fields: %{"pname" => %{type: :string}}
          )
          |> Selecto.select(["id", "pcj.pname"])
        else
          q(ctx) |> Selecto.filter({"id", 2}) |> Selecto.select(["id", "name"])
        end

      Selecto.union(left, right, all: true)
    end
  end

  defp union_cte(ctx, member_cte?) do
    q(ctx)
    |> Selecto.with_cte("u", union_body(ctx, member_cte?),
      columns: ["uid", "uname"],
      join: [owner_key: :id, related_key: :uid, fields: %{"uname" => %{type: :string}}]
    )
    |> Selecto.select(["name", "u.uname"])
  end

  defp rows(query) do
    {:ok, {rows, _, _}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  test "a CTE whose body is a union answers", ctx do
    assert rows(union_cte(ctx, false)) == [["a", "a"], ["b", "b"]]
  end

  test "a union member defining its own CTE answers, in a CTE body or at the top", ctx do
    assert rows(union_cte(ctx, true)) == [["a", "a"], ["b", "b"]]
    assert rows(union_body(ctx, true).()) == [[1, "a"], [2, "b"]]
  end
end
