defmodule SelectoDBPostgreSQL.LongTokenIntegrationTest do
  # The session guard's shadow count matches the statement's tokens against
  # temp type names. Bound as name[], a token of 64+ bytes (a hash or JSON
  # literal, a long value or identifier) could not even be encoded by
  # Postgrex, so every such read and write refused as the session state
  # being unreadable. The tokens bind as text[] and are cast server-side.
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  @long_col "a_very_long_column_name_that_goes_past_the_sixty_three_byte_limit_of_postgres"

  setup do
    schema = "selecto_long_#{System.unique_integer([:positive])}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

    {:ok, connection} =
      Adapter.connect(
        options
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    Process.unlink(connection)

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE TABLE #{schema}.items (id int PRIMARY KEY, #{@long_col} text, n int)",
          "INSERT INTO #{schema}.items VALUES (1, 'x', 0), (2, 'y', 0)"
        ],
        do: {:ok, _} = Adapter.execute(connection, sql, [], [])

    on_exit(fn ->
      if Process.alive?(connection) do
        Adapter.execute(connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [], [])
        GenServer.stop(connection)
      end
    end)

    long = String.to_atom(@long_col)

    domain = %{
      name: "items",
      schemas: %{},
      joins: %{},
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: [:id, long, :n],
        redact_fields: [],
        columns: %{long => %{type: :string}, id: %{type: :integer}, n: %{type: :integer}},
        associations: %{}
      }
    }

    %{connection: connection, domain: domain, long: long}
  end

  defp rows(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _, _}} -> Enum.sort(rows)
      {:error, %Selecto.Error{} = error} -> {:error, error.details[:reason] || error.type}
    end
  end

  test "reads and writes whose SQL holds a token of 64+ bytes answer", %{
    connection: c,
    domain: domain,
    long: long
  } do
    q = fn -> Selecto.configure(domain, c, adapter: Adapter, validate: false) end
    sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    json =
      ~s({"note": "a free-text description that runs well past the sixty-three byte name limit"})

    assert rows(
             q.()
             |> Selecto.select(["id"])
             |> Selecto.filter({:raw_sql_filter, "selecto_root.id <> 0 OR '#{sha}' = ''"})
           ) == [[1], [2]]

    assert rows(
             q.()
             |> Selecto.select(["id"])
             |> Selecto.filter({:raw_sql_filter, "selecto_root.id > 0 OR '#{json}' = ''"})
           ) == [[1], [2]]

    assert rows(
             q.()
             |> Selecto.select(["id"])
             |> Selecto.filter({@long_col, String.duplicate("e", 70)})
           ) ==
             []

    assert rows(q.() |> Selecto.select(["id", @long_col]) |> Selecto.filter({@long_col, "x"})) ==
             [[1, "x"]]

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :items,
        assignments: [%{field: :n, value: {:literal, 1}}],
        predicate: {:eq, {:field, long}, {:literal, "x"}},
        expected_cardinality: :many,
        metadata: %{field_types: %{long => :string, n: :integer}}
      })

    authorization = apply(SelectoUpdato.GovernedWrite, :authorize!, [command])

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write(c, command, authorization: authorization)

    assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT id FROM items WHERE n = 1", [], [])
  end
end
