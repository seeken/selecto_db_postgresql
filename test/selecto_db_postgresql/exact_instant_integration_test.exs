defmodule SelectoDBPostgreSQL.ExactInstantIntegrationTest do
  use ExUnit.Case, async: false
  alias SelectoDBPostgreSQL.Adapter
  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)
    on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)
    Postgrex.query!(connection, "SET TIME ZONE 'America/New_York'", [])

    Postgrex.query!(
      connection,
      "CREATE TEMP TABLE selecto_exact_instant_live (id integer PRIMARY KEY, instant timestamptz, epoch numeric, stored_utc timestamp)",
      []
    )

    Postgrex.query!(
      connection,
      "INSERT INTO selecto_exact_instant_live VALUES (1,'2024-01-01T10:00:00.123456Z',-0.000000001,'2024-01-01 10:00:00.123456'), (2,NULL,NULL,NULL), (3,'1970-01-01T00:00:00Z',0,'1970-01-01 00:00:00')",
      []
    )

    %{connection: connection}
  end

  defp ids(connection, field, operation, decimal) do
    columns = %{
      id: %{type: :integer},
      instant: %{type: :utc_datetime},
      epoch: %{type: :epoch_datetime},
      stored_utc: %{type: :utc_datetime, storage: :naive_utc}
    }

    domain = %{
      name: "Owned exact instant integration",
      source: %{
        source_table: "selecto_exact_instant_live",
        source_schema: "pg_temp",
        primary_key: :id,
        fields: Map.keys(columns),
        columns: columns,
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    query =
      Selecto.configure(domain, connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id"])
      |> Selecto.filter(Selecto.Expr.instant_compare(field, operation, decimal))
      |> Selecto.order_by(["id"])

    {:ok, {rows, _, _}} = Selecto.execute(query)
    rows
  end

  test "timestamp comparisons preserve submicrosecond predicates under a non-UTC session", %{
    connection: connection
  } do
    assert ids(connection, "instant", :eq, "1704103200.123456001") == []
    assert ids(connection, "instant", :lt, "1704103200.123456001") == [[1], [3]]
    assert ids(connection, "instant", :eq, "1704103200.123456") == [[1]]
  end

  test "negative numeric epochs retain nanoseconds and zero remains a real value", %{
    connection: connection
  } do
    assert ids(connection, "epoch", :eq, "-0.000000001") == [[1]]
    assert ids(connection, "epoch", :gt, "-0.000000002") == [[1], [3]]
    assert ids(connection, "epoch", :eq, "0") == [[3]]
  end

  test "declared naive-UTC storage compares instants independently of session timezone", %{
    connection: connection
  } do
    assert ids(connection, "stored_utc", :eq, "1704103200.123456") == [[1]]
    assert ids(connection, "stored_utc", :eq, "1704103200.123456001") == []
  end
end
