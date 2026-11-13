defmodule SelectoDBPostgreSQL.UnnestIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    {:ok, connection} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(connection)
    on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)

    Postgrex.query!(
      connection,
      "CREATE TEMP TABLE unnest_products (id integer, name text, tags text[], active boolean)",
      []
    )

    Postgrex.query!(
      connection,
      "INSERT INTO unnest_products VALUES (1, 'Atlas', ARRAY['sql', 'typed'], true), (2, 'Hidden', ARRAY['hidden'], false)",
      []
    )

    domain = %{
      name: "Unnest products",
      source: %{
        source_table: "unnest_products",
        source_schema: "pg_temp",
        primary_key: :id,
        fields: [:id, :name, :tags, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          tags: %{type: {:array, :string}},
          active: %{type: :boolean}
        },
        associations: %{},
        redact_fields: []
      },
      schemas: %{},
      joins: %{}
    }

    %{connection: connection, query: Selecto.configure(domain, connection, adapter: Adapter)}
  end

  test "unnest value prepares and executes without ordinality", %{
    connection: connection,
    query: base
  } do
    query =
      base
      |> Selecto.unnest("tags", as: "product_tag")
      |> Selecto.select(["name", "product_tag"])
      |> Selecto.filter({"active", true})

    {sql, params} = Selecto.to_sql(query)
    assert sql =~ "AS product_tag(value)"
    assert sql =~ "product_tag.value"
    assert params == [true]
    statement = Postgrex.prepare!(connection, "unnest_without_ordinality", sql)

    assert %{rows: [["Atlas", "sql"], ["Atlas", "typed"]]} =
             Postgrex.execute!(connection, statement, params)
  end

  test "unnest value and ordinality both execute with their declared columns", %{
    connection: connection,
    query: base
  } do
    query =
      base
      |> Selecto.unnest("tags", as: "product_tag", ordinality: "position")
      |> Selecto.select(["name", "product_tag", "product_tag_ordinality"])
      |> Selecto.filter({"active", true})
      |> Selecto.order_by("product_tag_ordinality")

    {sql, params} = Selecto.to_sql(query)
    assert sql =~ "WITH ORDINALITY AS product_tag(value, position)"
    statement = Postgrex.prepare!(connection, "unnest_with_ordinality", sql)

    assert %{rows: [["Atlas", "sql", 1], ["Atlas", "typed", 2]]} =
             Postgrex.execute!(connection, statement, params)
  end
end
