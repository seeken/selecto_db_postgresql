defmodule SelectoDBPostgreSQL.ComputedValueIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)
    on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)

    Postgrex.query!(
      connection,
      "CREATE TEMP TABLE selecto_computed_boundary (id integer, amount text, payload jsonb)",
      []
    )

    Postgrex.query!(
      connection,
      "INSERT INTO selecto_computed_boundary VALUES (1, '12', '{\"items\":[{\"sku\":\"a-1\"}]}'), (2, NULL, '{\"items\":[{\"sku\":null}]}'), (3, '4', NULL)",
      []
    )

    %{connection: connection}
  end

  defp query(connection) do
    columns = %{
      id: %{type: :integer},
      amount: %{type: :string},
      payload: %{type: :json},
      number: %{
        type: :integer,
        computed: %{kind: :expression, expression: ["cast", ["field", "amount"], "integer"]}
      },
      sku: %{
        type: :string,
        computed: %{kind: :expression, expression: ["json_text", "payload", ["items", 0, "sku"]]}
      },
      labeled: %{
        type: :string,
        computed: %{
          kind: :expression,
          expression: ["coalesce", ["field", "sku"], ["literal", "missing"]]
        }
      }
    }

    domain = %{
      name: "Computed boundary",
      source: %{
        source_table: "selecto_computed_boundary",
        primary_key: :id,
        fields: Map.keys(columns),
        columns: columns,
        associations: %{},
        redact_fields: []
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, connection, adapter: Adapter)
  end

  test "actual adapter casts and extracts JSON with ordered binds and SQL nulls", %{
    connection: connection
  } do
    query =
      query(connection)
      |> Selecto.select(["id", "number", "sku", "labeled"])
      |> Selecto.order_by("id")

    {sql, _aliases, params} = Selecto.gen_sql(query, [])
    assert params == ["items", "0", "sku", "items", "0", "sku", "missing"]
    assert sql =~ "CAST(selecto_root.amount AS BIGINT)"
    assert sql =~ "$7"
    assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    assert rows == [[1, 12, "a-1", "a-1"], [2, nil, nil, "missing"], [3, 4, nil, "missing"]]
  end

  test "computed filtering, grouping and ordering retain parameter numbering and results", %{
    connection: connection
  } do
    query =
      query(connection)
      |> Selecto.select(["labeled", {:count}])
      |> Selecto.filter({"labeled", "missing"})
      |> Selecto.group_by("labeled")
      |> Selecto.order_by("labeled")

    {sql, _aliases, params} = Selecto.gen_sql(query, [])
    assert params == ["items", "0", "sku", "missing", "items", "0", "sku", "missing", "missing"]
    assert sql =~ "group by 1"
    assert sql =~ "$9"
    assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    assert rows == [["missing", 2]]
  end
end
