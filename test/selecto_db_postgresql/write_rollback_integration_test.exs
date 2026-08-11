defmodule SelectoDBPostgreSQL.WriteRollbackIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    options =
      case System.get_env("SELECTO_POSTGRES_TEST_URL") do
        nil ->
          [
            hostname: "localhost",
            username: "postgres",
            password: "postgres",
            database: "selecto_example_dev"
          ]

        url ->
          [url: url]
      end

    {:ok, connection} = Adapter.connect(options)
    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    {:ok, _} =
      Adapter.execute(
        connection,
        "CREATE TEMP TABLE selecto_write_rollback (id integer PRIMARY KEY, tenant_id integer NOT NULL, state text NOT NULL)",
        [],
        []
      )

    {:ok, _} =
      Adapter.execute(
        connection,
        "INSERT INTO selecto_write_rollback (id, tenant_id, state) VALUES (1, 7, 'done'), (2, 7, 'open')",
        [],
        []
      )

    %{connection: connection}
  end

  test "cardinality mismatch rolls back every tentative mutation", %{connection: connection} do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :selecto_write_rollback,
        assignments: [%{field: :state, value: {:literal, "archived"}}],
        predicate:
          {:and,
           [
             {:in, {:field, :id}, [{:literal, 1}, {:literal, 2}]},
             {:eq, {:field, :tenant_id}, {:literal, 7}},
             {:eq, {:field, :state}, {:literal, "done"}}
           ]},
        expected_cardinality: {:exactly, 2}
      })

    assert {:error,
            %Error{
              type: :cardinality_mismatch,
              details: %{actual: 1, expected: {:exactly, 2}}
            }} = Adapter.execute_write(connection, command)

    assert {:ok, %{rows: [[1, "done"], [2, "open"]]}} =
             Adapter.execute(
               connection,
               "SELECT id, state FROM selecto_write_rollback ORDER BY id",
               [],
               []
             )
  end
end
